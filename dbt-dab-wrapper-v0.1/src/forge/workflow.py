# =============================================
# src/forge/workflow.py
# =============================================
# THE CODE IS THE DOCUMENTATION
#
# Generates Databricks Workflow DAGs from dbt lineage.
#
# The transformation pipeline is split into stages:
#   1. INGEST   — seeds, sources, raw loads
#   2. STAGE    — staging models (stg_*)
#   3. CLEAN    — cleaning + quarantine models
#   4. ENRICH   — joins, business logic
#   5. SERVE    — gold / summary / publish
#
# Each stage becomes a Databricks job task with
# explicit dependencies, so the workflow DAG
# mirrors the logical data flow.
#
# Usage:
#   from forge.workflow import build_workflow
#   wf = build_workflow(forge_config, graph)
#   wf.to_databricks_yml()   # → databricks.yml jobs section
#   wf.to_mermaid()          # → visual DAG
#   wf.to_dict()             # → serialisable dict
#
# The forge CLI command:
#   forge workflow            # prints the DAG
#   forge workflow --deploy   # deploys via DAB
#   forge workflow --mermaid  # emits Mermaid diagram
#
# Stage assignment rules (in priority order):
#   1. Explicit: model meta `stage: "clean"` in schema.yml
#   2. Convention: name prefix → stg_=STAGE, raw_=INGEST
#   3. Features: quarantine post_hook → CLEAN
#   4. Lineage depth: 0 hops=INGEST, 1=STAGE, 2+=ENRICH/SERVE

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


from forge.compute_resolver import read_databrickscfg, get_schema_variables, resolve_profile


# =============================================
# STAGE DEFINITIONS
# =============================================

STAGES = ["ingest", "stage", "clean", "enrich", "serve"]

SQL_SETUP_DIR = Path("sql") / "setup"
SQL_PROCESS_DIR = Path("sql") / "process"
SQL_TEARDOWN_DIR = Path("sql") / "teardown"


def _sql_task_path(file_path: Path) -> str:
    return file_path.as_posix()

# Name-prefix conventions for auto-assignment
_STAGE_PREFIXES: dict[str, str] = {
    "raw_": "ingest",
    "src_": "ingest",
    "seed_": "ingest",
    "stg_": "stage",
    "staging_": "stage",
    "clean_": "clean",
    "int_": "enrich",
    "fct_": "enrich",
    "dim_": "enrich",
    "agg_": "serve",
    "rpt_": "serve",
    "pub_": "serve",
}

# Keywords in description / tags
_STAGE_KEYWORDS: dict[str, str] = {
    "staging": "stage",
    "clean": "clean",
    "quarantine": "clean",
    "gold": "serve",
    "silver": "stage",
    "bronze": "ingest",
    "summary": "serve",
    "aggregate": "serve",
    "join": "enrich",
    "enrich": "enrich",
}


def _assign_stage(
    model_name: str,
    contract: dict,
    depth: int,
    has_quarantine: bool,
) -> str:
    """Determine which pipeline stage a model belongs to."""
    # 1. Explicit meta override
    meta = contract.get("_meta", {})
    if "stage" in meta:
        return meta["stage"]

    # 2. Name prefix convention
    for prefix, stage in _STAGE_PREFIXES.items():
        if model_name.startswith(prefix):
            return stage

    # 3. Quarantine → clean
    if has_quarantine:
        return "clean"

    # 4. Tags / description keywords
    tags = set(contract.get("tags", []))
    desc = contract.get("dataset", {}).get("description", "").lower()
    for keyword, stage in _STAGE_KEYWORDS.items():
        if keyword in tags or keyword in desc:
            return stage

    # 5. Fallback: lineage depth
    if depth == 0:
        return "ingest"
    elif depth == 1:
        return "stage"
    elif depth == 2:
        return "enrich"
    else:
        return "serve"


# =============================================
# WORKFLOW DAG
# =============================================

@dataclass
class WorkflowTask:
    """One task in the Databricks workflow DAG."""
    name: str
    stage: str
    task_type: str = "dbt"             # "dbt", "python", "sql"
    models: list[str] = field(default_factory=list)
    python_file: str | None = None      # path for spark_python_task
    sql_file: str | None = None         # path for sql_task
    additional_config: dict | None = None  # extra DAB keys (warehouse_id, base_parameters, etc.)
    depends_on: list[str] = field(default_factory=list)
    compute_type: str = "serverless"
    timeout_minutes: int = 60
    dbt_commands: list[str] | None = None  # override dbt commands (for setup/teardown tasks)


@dataclass
class Workflow:
    """
    A complete Databricks Workflow definition,
    broken into stage-based tasks with dependencies.
    """
    name: str
    tasks: list[WorkflowTask] = field(default_factory=list)
    schedule: str | None = None
    environment: str = "dev"
    catalog: str = "main"
    schema: str = "default"
    compute_type: str = "serverless"
    warehouse_id: str | None = None
    dbt_vars: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Serialise to a plain dict."""
        return {
            "name": self.name,
            "environment": self.environment,
            "schedule": self.schedule,
            "tasks": [
                {
                    "task_key": t.name,
                    "stage": t.stage,
                    "task_type": t.task_type,
                    "models": t.models,
                    **({"python_file": t.python_file} if t.python_file else {}),
                    **({"sql_file": t.sql_file} if t.sql_file else {}),
                    **({"dbt_commands": t.dbt_commands} if t.dbt_commands else {}),
                    "depends_on": [{"task_key": d} for d in t.depends_on],
                    "compute_type": t.compute_type,
                    "timeout_minutes": t.timeout_minutes,
                }
                for t in self.tasks
            ],
        }

    def to_databricks_yml(self) -> str:
        """
        Generate the `resources.jobs` section of databricks.yml.

        Each stage becomes a task running `dbt run --select`:
          - ingest:  dbt run --select tag:ingest
          - stage:   dbt run --select tag:stage
          - clean:   dbt run --select tag:clean
          - enrich:  dbt run --select tag:enrich
          - serve:   dbt run --select tag:serve
        """
        job: dict[str, Any] = {
            "resources": {
                "jobs": {
                    self.name: {
                        "name": f"{self.name} ({self.environment})",
                        "tasks": [],
                    }
                }
            }
        }

        if self.schedule:
            job["resources"]["jobs"][self.name]["schedule"] = {
                "quartz_cron_expression": self.schedule,
                "timezone_id": "UTC",
            }

        # Serverless environment — client "2" for newer serverless runtime
        env_deps: list[str | dict] = [
            "dbt-databricks",
            "pydantic>=2.0",
            "pyyaml",
        ]
        # Include forge wheel if any python tasks exist
        has_python_tasks = any(t.task_type == "python" for t in self.tasks)
        if has_python_tasks:
            env_deps.append({"whl": "../../dist/*.whl"})

        job["resources"]["jobs"][self.name]["environments"] = [
            {
                "environment_key": "default",
                "spec": {
                    "client": "2",
                    "dependencies": env_deps,
                }
            }
        ]

        for task in self.tasks:
            task_def: dict[str, Any] = {
                "task_key": task.name,
                "timeout_seconds": task.timeout_minutes * 60,
            }

            # environment_key only for dbt and python tasks (not sql)
            if task.task_type in ("dbt", "python"):
                task_def["environment_key"] = "default"

            if task.task_type == "python" and task.python_file:
                # spark_python_task — runs a .py file on the cluster
                python_file_path = task.python_file
                if not python_file_path.startswith("/"):
                    python_file_path = "../../" + python_file_path
                task_def["spark_python_task"] = {
                    "python_file": python_file_path,
                    **(task.additional_config or {}),
                }
            elif task.task_type == "sql" and task.sql_file:
                # sql_task — runs a .sql file on a SQL warehouse
                sql_file_path = task.sql_file
                if not sql_file_path.startswith("/"):
                    sql_file_path = "../../" + sql_file_path
                sql_config: dict[str, Any] = {
                    "file": {"path": sql_file_path},
                    **(task.additional_config or {}),
                }
                if self.warehouse_id:
                    sql_config["warehouse_id"] = self.warehouse_id
                # Pass dynamic value refs as named parameters for :param SQL syntax
                sql_config["parameters"] = {
                    **(sql_config.get("parameters") or {}),
                    "run_id": "{{job.run_id}}",
                }
                task_def["sql_task"] = sql_config
            else:
                # dbt_task — runs dbt on serverless, SQL goes to warehouse
                commands = task.dbt_commands or [
                    "dbt deps",
                    f"dbt run --select {' '.join(task.models)}"
                ]
                # Inject --vars for catalog/schema resolution
                if self.dbt_vars:
                    vars_yaml = json.dumps(self.dbt_vars)
                    commands = [
                        f"{cmd} --vars '{vars_yaml}'" if cmd.startswith("dbt ") and not cmd.startswith("dbt deps") else cmd
                        for cmd in commands
                    ]
                dbt_config = {
                    "project_directory": "${workspace.file_path}",
                    "commands": commands,
                    "catalog": self.catalog,
                }
                if self.warehouse_id:
                    dbt_config["warehouse_id"] = self.warehouse_id
                task_def["dbt_task"] = dbt_config

            if task.depends_on:
                task_def["depends_on"] = [
                    {"task_key": d} for d in task.depends_on
                ]

            job["resources"]["jobs"][self.name]["tasks"].append(task_def)

        return yaml.dump(job, sort_keys=False, default_flow_style=False)

    def to_mermaid(self) -> str:
        """Render the workflow DAG as a Mermaid diagram."""
        lines = ["flowchart LR"]

        # Stage subgraphs
        stage_tasks: dict[str, list[WorkflowTask]] = {s: [] for s in STAGES}
        for task in self.tasks:
            stage_tasks.get(task.stage, stage_tasks["enrich"]).append(task)

        for stage in STAGES:
            tasks = stage_tasks[stage]
            if not tasks:
                continue
            lines.append(f"    subgraph {stage.upper()}")
            for task in tasks:
                safe = task.name.replace("-", "_")
                model_list = ", ".join(task.models[:3])
                if len(task.models) > 3:
                    model_list += f" +{len(task.models)-3}"
                if task.task_type == "python":
                    lines.append(f'        {safe}["{task.name}<br/>🐍 {task.python_file or model_list}"]')
                elif task.task_type == "sql":
                    lines.append(f'        {safe}["{task.name}<br/>📄 {task.sql_file}"]')
                else:
                    lines.append(f'        {safe}["{task.name}<br/>{model_list}"]')
            lines.append("    end")

        # Edges
        for task in self.tasks:
            for dep in task.depends_on:
                src = dep.replace("-", "_")
                tgt = task.name.replace("-", "_")
                lines.append(f"    {src} --> {tgt}")

        return "\n".join(lines)


def render_workflows_mermaid(workflows: list[Workflow], direction: str = "LR") -> str:
    """Render multiple workflows as one Mermaid flowchart."""
    direction_value = direction.upper()
    lines = [f"flowchart {direction_value}"]

    stage_tasks: dict[str, list[WorkflowTask]] = {s: [] for s in STAGES}
    for workflow in workflows:
        for task in workflow.tasks:
            stage_tasks.get(task.stage, stage_tasks["enrich"]).append(task)

    for stage in STAGES:
        tasks = stage_tasks[stage]
        if not tasks:
            continue
        lines.append(f"    subgraph {stage.upper()}")
        for task in tasks:
            safe = task.name.replace("-", "_")
            model_list = ", ".join(task.models[:3])
            if len(task.models) > 3:
                model_list += f" +{len(task.models)-3}"
            if task.task_type == "python":
                lines.append(f'        {safe}["{task.name}<br/>🐍 {task.python_file or model_list}"]')
            elif task.task_type == "sql":
                lines.append(f'        {safe}["{task.name}<br/>📄 {task.sql_file}"]')
            else:
                lines.append(f'        {safe}["{task.name}<br/>{model_list}"]')
        lines.append("    end")

    lines.append("")
    lines.append("    %% === Data flow (all connections at the bottom) ===")

    for workflow in workflows:
        for task in workflow.tasks:
            for dep in task.depends_on:
                src = dep.replace("-", "_")
                tgt = task.name.replace("-", "_")
                lines.append(f"    {src} --> {tgt}")

    for index, workflow in enumerate(workflows[:-1]):
        next_workflow = workflows[index + 1]
        if not workflow.name.startswith("PROCESS_") or not next_workflow.name.startswith("TEARDOWN_"):
            continue

        leaf_tasks = [
            task for task in workflow.tasks
            if not any(task.name in other.depends_on for other in workflow.tasks)
        ]
        root_tasks = [task for task in next_workflow.tasks if not task.depends_on]

        for leaf in leaf_tasks:
            src = leaf.name.replace("-", "_")
            for root in root_tasks:
                tgt = root.name.replace("-", "_")
                lines.append(f"    {src} --> {tgt}")

    return "\n".join(lines)


# =============================================
# DATABRICKS BUNDLE CONFIG (databricks.yml)
# =============================================

def generate_bundle_config(
    forge_config: dict,
    job_yaml_paths: list[str] | None = None,
    sql_mode: bool = False,
    prebuilt_wheel: bool = False,
) -> str:
    """
    Generate a root databricks.yml that ties the DAB bundle together.

    This is the entry point file that `databricks bundle deploy` reads.
    It references the generated job YAMLs via include paths.
    """
    project_name = forge_config.get("name", "unnamed")
    project_id = forge_config.get("id", project_name)
    resolved_profile = resolve_profile(forge_config)
    profile = resolved_profile.get("_name", next(iter(forge_config.get("profiles", {})), "dev"))

    # Resolve the workspace host from the active profile
    profiles = forge_config.get("profiles", {})
    active_prof = profiles.get(profile, {})
    databricks_profile = active_prof.get("databricks_profile", "DEFAULT")

    sync_include: list[str] = []
    if sql_mode:
        sync_include.append("sql/")
    if Path("dist").is_dir():
        sync_include.append("dist/")
    # Python tasks need the script files; the forge runtime comes from the wheel.
    if Path("python").is_dir():
        sync_include.append("python/")
    # dbt metadata (schema.yml) needed by build_models() at runtime
    if Path("dbt").is_dir():
        sync_include.append("dbt/")
    if Path("macros").is_dir():
        sync_include.append("macros/")
    if Path("dbt_project.yml").exists():
        sync_include.append("dbt_project.yml")
    if Path("profiles.yml").exists():
        sync_include.append("profiles.yml")
    # forge.yml needed by ForgeTask at runtime
    if Path("forge.yml").exists():
        sync_include.append("forge.yml")

    bundle: dict[str, Any] = {
        "bundle": {
            "name": f"{project_name}_{project_id}",
        },
        "include": job_yaml_paths or ["resources/jobs/*.yml"],
        "sync": {
            "include": sync_include,
        },
    }

    if not prebuilt_wheel:
        bundle["artifacts"] = {
            "forge_wheel": {
                "type": "whl",
                "build": "poetry build",
                "path": ".",
            },
        }

    # Add targets from forge.yml profiles
    targets = {}
    for prof_name, prof_config in profiles.items():
        if prof_config.get("platform") == "databricks":
            databricks_prof = prof_config.get("databricks_profile", prof_name)
            
            # Add target
            target_config = {
                "workspace": {
                    "profile": databricks_prof,
                }
            }
            if prof_name == profile:  # active profile
                target_config["default"] = True
            targets[prof_name] = target_config
    
    if targets:
        bundle["targets"] = targets

    return yaml.dump(bundle, sort_keys=False, default_flow_style=False)


def run_bundle_command(command: str, target: str | None = None) -> dict:
    """
    Run `databricks bundle <command>` (deploy, destroy, validate).

    Automatically detects and uses the correct Databricks CLI version.
    Returns {"success": bool, "output": str, "error": str | None}.
    """
    import subprocess
    import os

    # Detect which CLI to use
    cli_candidates = [
        os.path.expanduser("~/.local/bin/databricks"),  # New CLI installed by setup script
        os.path.expanduser("~/.local/bin/databricks-new"),  # Alternative location
        "/usr/local/bin/databricks",  # System installation
        "databricks",  # In PATH
    ]

    cli_path = None
    for candidate in cli_candidates:
        if os.path.exists(candidate) or (candidate == "databricks" and _check_command_exists("databricks")):
            # Test if this CLI supports bundle commands
            try:
                result = subprocess.run(
                    [candidate, "bundle", "--help"],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                if result.returncode == 0:
                    cli_path = candidate
                    break
            except (subprocess.TimeoutExpired, FileNotFoundError, subprocess.CalledProcessError):
                continue

    if not cli_path:
        return {
            "success": False,
            "output": "",
            "error": (
                "Databricks CLI with bundle support not found. Install with:\n"
                "./setup.sh\n"
                "or manually:\n"
                "curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh"
            ),
        }

    cmd = [cli_path, "bundle", command]
    if target:
        cmd += ["--target", target]

    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            cwd=os.getcwd(),  # Run from project root
        )
        return {"success": True, "output": result.stdout, "error": None}
    except subprocess.CalledProcessError as e:
        return {
            "success": False,
            "output": e.stdout or "",
            "error": (e.stderr or "").strip(),
        }


def _check_command_exists(command: str) -> bool:
    """Check if a command exists in PATH."""
    import shutil
    return shutil.which(command) is not None


# =============================================
# CUSTOM TASKS – user-defined sql/python tasks
# =============================================

def load_custom_tasks(forge_config: dict) -> list[dict]:
    """
    Read custom_tasks from forge.yml (or wrapper.yml).

        forge.yml:
                custom_tasks:
                    - task_key: run_reconciliation
                        sql_task: sql/reconcile.sql
                        stage: enrich
                        depends_on: [customer_clean]
                    - task_key: my_api_pull
                        python_task: python/my_api_pull.py
                        stage: ingest
                        config:
                            timeout_seconds: 7200

    Returns list of task dicts with keys:
            task_key, task_type, file, stage, depends_on, config.
    """
    custom = forge_config.get("custom_tasks", [])
    result = []
    for task in custom:
        task_key = task["task_key"]
        # Determine task_type from which key is present
        if "python_task" in task:
            task_type = "python"
            file_path = task["python_task"]
        elif "sql_task" in task:
            task_type = "sql"
            file_path = task["sql_task"]
        elif "notebook_task" in task:
            raise ValueError(
                f"custom_task '{task_key}' uses notebook_task, but notebook task support has been removed. "
                "Replace it with a python_task or sql_task."
            )
        else:
            task_type = "dbt"
            file_path = None

        result.append({
            "task_key": task_key,
            "task_type": task_type,
            "file": file_path,
            "stage": task.get("stage", "enrich"),
            "depends_on": task.get("depends_on", []),
            "config": task.get("config", {}),
        })
    return result


def _resolve_task_dependency(
    dep_name: str,
    all_task_names: set[str],
    model_to_task: dict[str, str],
) -> str | None:
    """
    Resolve a depends_on reference to an actual task key.

    Tries (in order):
      1. Exact match against existing task names
      2. Model name → its per-model task key
    """
    # 1. Exact match
    if dep_name in all_task_names:
        return dep_name

    # 2. Model name → task key
    if dep_name in model_to_task:
        return model_to_task[dep_name]

    return None


# =============================================
# BUILD WORKFLOW FROM GRAPH
# =============================================

def _compute_depths(graph: dict) -> dict[str, int]:
    """BFS from roots (no incoming edges) to compute hop depth per contract."""
    contracts = graph.get("contracts", {})
    edges = graph.get("edges", [])

    # Build adjacency
    children: dict[str, list[str]] = {cid: [] for cid in contracts}
    parents: dict[str, list[str]] = {cid: [] for cid in contracts}
    for edge in edges:
        src, tgt = edge["from"], edge["to"]
        if src in children:
            children[src].append(tgt)
        if tgt in parents:
            parents[tgt].append(src)

    # BFS from roots
    roots = [cid for cid, p in parents.items() if not p]
    depths: dict[str, int] = {}
    queue = [(r, 0) for r in roots]
    while queue:
        node, depth = queue.pop(0)
        if node in depths:
            depths[node] = max(depths[node], depth)
            continue
        depths[node] = depth
        for child in children.get(node, []):
            queue.append((child, depth + 1))

    # Assign depth 0 to anything not reached
    for cid in contracts:
        if cid not in depths:
            depths[cid] = 0

    return depths


def _resolve_dbt_vars(forge_config: dict) -> dict[str, str]:
    """Compute catalog/schema dbt variables from the resolved profile."""
    profile_config = resolve_profile(forge_config)
    if not profile_config:
        return {}
    schema_vars = get_schema_variables(profile_config, forge_config)
    # Also include lineage provenance vars from forge_config
    extra = {
        "git_commit": forge_config.get("git_commit", "local"),
        "compute_type": forge_config.get("compute", {}).get("type", "serverless"),
    }
    return {**schema_vars, **extra}


def build_workflow(
    forge_config: dict,
    graph: dict,
    sql_mode: bool = False,
) -> Workflow:
    """
    Build a Databricks Workflow from the lineage graph.

    Default: one task per model (per_model_tasks=true).
    Each task depends on its actual upstream models from the lineage graph.
    Task naming: {stage}_{model_name} (e.g. ingest_raw_customers).

    Set per_model_tasks: false in forge.yml to group models by stage instead.
    Set sql_mode: true to use sql_task (pure SQL files) instead of dbt_task.

    Python tasks and custom tasks are merged in by stage.
    """
    project_name = forge_config.get("name", "unnamed")
    project_id = forge_config.get("id", project_name)
    wf_prefix = f"PROCESS_{project_id}"
    environment = forge_config.get("environment", "dev")
    schema = forge_config.get("schema", "default")
    compute_type = forge_config.get("compute", {}).get("type", "serverless")

    # Resolve the actual catalog name via the profile's naming pattern
    profile_config = resolve_profile(forge_config)
    logical_catalog = profile_config.get("catalog", forge_config.get("catalog", "main"))
    dbt_vars = _resolve_dbt_vars(forge_config)
    resolved_catalog = dbt_vars.get(f"catalog_{logical_catalog}", logical_catalog)
    
    # Get warehouse_id for serverless compute
    warehouse_id = None
    if compute_type == "serverless":
        try:
            databricks_profile = profile_config.get("databricks_profile", "DEFAULT")
            config = read_databrickscfg(databricks_profile)
            http_path = config.get("http_path")
            if http_path and "/warehouses/" in http_path:
                warehouse_id = http_path.split("/warehouses/")[-1]
        except Exception:
            # If we can't read config, continue without warehouse_id
            pass
    schedule = forge_config.get("schedule", None)
    per_model = forge_config.get("per_model_tasks", True)

    depths = _compute_depths(graph)

    # Classify every model into a stage + collect contract IDs
    stage_models: dict[str, list[str]] = {s: [] for s in STAGES}
    model_stages: dict[str, str] = {}            # model_name → stage
    model_to_cid: dict[str, str] = {}            # model_name → contract_id
    contracts = graph.get("contracts", {})

    for cid, contract in contracts.items():
        tags = set(contract.get("tags", []))
        if tags & {"quarantine", "prior_version", "auto-generated"}:
            continue
        dataset_type = contract["dataset"]["type"]
        # Only include models — skip volumes, sources, seeds, tests, checks, functions, workflows
        if dataset_type not in ("model", "table", "view", "incremental"):
            continue
        if cid.startswith(("workflow.", "custom_task.", "check.", "volume.", "source.", "seed.")):
            continue
        # Skip managed_by models — schema created in SETUP, data populated by python task
        if contract.get("_meta", {}).get("managed_by"):
            continue

        model_name = contract["dataset"]["name"]
        depth = depths.get(cid, 0)
        has_quarantine = contract.get("provenance", {}).get("uses_quarantine", False)

        stage = _assign_stage(model_name, contract, depth, has_quarantine)
        stage_models[stage].append(model_name)
        model_stages[model_name] = stage
        model_to_cid[model_name] = cid

    # Build lineage parent map: model_name → [upstream model names]
    edges = graph.get("edges", [])
    cid_to_model: dict[str, str] = {v: k for k, v in model_to_cid.items()}
    model_parents: dict[str, list[str]] = {m: [] for m in model_stages}
    for edge in edges:
        src_model = cid_to_model.get(edge["from"])
        tgt_model = cid_to_model.get(edge["to"])
        if src_model and tgt_model and src_model in model_stages and tgt_model in model_stages:
            if src_model not in model_parents[tgt_model]:
                model_parents[tgt_model].append(src_model)

    # ── Load python_tasks (auto-discovered from python/) ──
    from forge.python_task import load_python_tasks
    python_tasks = load_python_tasks()

    # ── Load DDL model defs for managed_by column schemas ──
    from forge.simple_ddl import load_ddl
    dbt_dir = Path(forge_config.get("dbt_project_dir", "dbt"))
    ddl_path = dbt_dir / "ddl"
    ddl_models = load_ddl(ddl_path, forge_config=forge_config) if ddl_path.exists() else {}

    # ── Load custom_tasks from forge.yml ──────────────
    custom_tasks = load_custom_tasks(forge_config)
    custom_by_stage: dict[str, list[dict]] = {s: [] for s in STAGES}
    for ct in custom_tasks:
        custom_by_stage.get(ct["stage"], custom_by_stage["enrich"]).append(ct)

    # ── Validate all referenced asset files exist ─────
    missing: list[str] = []
    for ct in custom_tasks:
        if ct.get("file") and not Path(ct["file"]).exists():
            missing.append(f"custom_task '{ct['task_key']}': {ct['file']}")
    if missing:
        raise FileNotFoundError(
            "Referenced task assets not found:\n"
            + "\n".join(f"  - {m}" for m in missing)
            + "\nCreate these files or remove the tasks from forge.yml."
        )

    # ── Build sql file index (sql_mode) ─────────────
    sql_file_map: dict[str, str] = {}  # model_name → sql/NNN_model.sql
    if sql_mode:
        sql_dir = SQL_PROCESS_DIR
        if sql_dir.is_dir():
            for f in sorted(sql_dir.iterdir()):
                if f.suffix == ".sql" and "_" in f.stem:
                    # Extract model name: "002_customer_clean" → "customer_clean"
                    parts = f.stem.split("_", 1)
                    if parts[0].isdigit() and len(parts) > 1:
                        sql_file_map[parts[1]] = _sql_task_path(f)

    # ── Build tasks ───────────────────────────────────
    tasks: list[WorkflowTask] = []
    model_to_task: dict[str, str] = {}  # model_name → task_key

    if per_model:
        # ── Per-model mode (default) ──────────────────
        # Pass 1: create all tasks (deps filled in pass 2)
        for stage in STAGES:
            models = sorted(stage_models[stage])
            for model_name in models:
                task_name = f"{stage}_{model_name}"

                if sql_mode and model_name in sql_file_map:
                    task_type = "sql"
                    sql_file = sql_file_map[model_name]
                    # Also add quarantine sibling if it exists
                    q_key = f"{model_name}_quarantine"
                    q_file = sql_file_map.get(q_key)
                else:
                    task_type = "dbt"
                    sql_file = None
                    q_file = None

                tasks.append(WorkflowTask(
                    name=task_name,
                    stage=stage,
                    task_type=task_type,
                    sql_file=sql_file,
                    models=[model_name],
                    depends_on=[],
                    compute_type=compute_type,
                ))
                model_to_task[model_name] = task_name

                # Quarantine task (sql_mode only)
                if q_file:
                    q_task_name = f"{stage}_{model_name}_quarantine"
                    tasks.append(WorkflowTask(
                        name=q_task_name,
                        stage=stage,
                        task_type="sql",
                        sql_file=q_file,
                        models=[],
                        depends_on=[task_name],
                        compute_type=compute_type,
                    ))

        # Pass 2: wire real lineage deps
        for task in tasks:
            if task.task_type not in ("dbt", "sql") or not task.models:
                continue
            model_name = task.models[0]
            for parent in model_parents.get(model_name, []):
                parent_task = model_to_task.get(parent)
                if parent_task and parent_task not in task.depends_on:
                    task.depends_on.append(parent_task)
    else:
        # ── Stage-level mode (opt-in) ─────────────────
        prev_task_name: str | None = None
        for stage in STAGES:
            models = sorted(stage_models[stage])
            if not models:
                continue
            task_name = f"{stage}"
            depends = [prev_task_name] if prev_task_name else []

            tasks.append(WorkflowTask(
                name=task_name,
                stage=stage,
                task_type="dbt",
                models=models,
                depends_on=depends,
                compute_type=compute_type,
            ))
            for m in models:
                model_to_task[m] = task_name
            prev_task_name = task_name

    # ── Python tasks ──────────────────────────────────
    # DDL-driven: python tasks that match managed_by values run first in
    # PROCESS (they ingest fresh data each cycle) and also in SETUP (schema
    # creation).  Root model tasks are rewired to depend on them so the
    # DAG flows: ingest → staging → … → serve.
    managed_by_values: set[str] = set()
    for cid, contract in contracts.items():
        mb = contract.get("_meta", {}).get("managed_by")
        if mb:
            managed_by_values.add(mb)

    domain_name = forge_config.get("_domain")  # set by build_domain_workflows
    ingest_task_names: list[str] = []

    # Build runtime params: pass resolved catalog/schema to python tasks
    # so they don't need to re-resolve naming patterns on Databricks
    # (where getpass.getuser() returns a Spark session ID, not the deployer).
    runtime_params: list[str] = []
    for var_key, var_val in dbt_vars.items():
        runtime_params.append(f"--{var_key}={var_val}")

    for pt in python_tasks:
        if "python" in managed_by_values or pt["name"] in managed_by_values:
            # DDL-driven ingest task → first task in PROCESS
            pt_name = f"ingest_py_{pt['name']}"

            params = list(runtime_params)
            if domain_name:
                params.append(f"--domain={domain_name}")

            # Bake DDL column schemas for managed_by models so the python
            # task can cast CSV columns without reading the target table.
            for m_name, m_def in ddl_models.items():
                if not isinstance(m_def, dict) or not m_def.get("managed_by"):
                    continue
                cols = m_def.get("columns", {})
                if cols:
                    col_parts = []
                    for c_name, c_def in cols.items():
                        if isinstance(c_def, str):
                            c_type = c_def
                        elif isinstance(c_def, dict):
                            c_type = c_def.get("type", "string")
                        else:
                            c_type = "string"
                        col_parts.append(f"{c_name}:{c_type}")
                    params.append(f"--schema_{m_name}={'|'.join(col_parts)}")

            pt_config: dict[str, Any] | None = {"parameters": params} if params else None

            tasks.insert(0, WorkflowTask(
                name=pt_name,
                stage="ingest",
                task_type="python",
                python_file=pt["file"],
                additional_config=pt_config,
                depends_on=[],
                compute_type=compute_type,
            ))
            ingest_task_names.append(pt_name)
            continue

        # Non-managed python tasks → after all model tasks (existing behaviour)
        pt_name = f"py_{pt['name']}"
        pt_deps: list[str] = []
        model_tasks = [t.name for t in tasks if t.task_type in ("dbt", "sql") and t.models]
        if model_tasks:
            pt_deps.append(model_tasks[-1])

        pt_config = None
        if domain_name:
            pt_config = {"parameters": [f"--domain={domain_name}"]}

        tasks.append(WorkflowTask(
            name=pt_name,
            stage="enrich",
            task_type="python",
            python_file=pt["file"],
            additional_config=pt_config,
            depends_on=pt_deps,
            compute_type=compute_type,
        ))

    # Rewire root model tasks: if they have no upstream model deps,
    # make them depend on the ingest python task(s) so the DAG flows
    # ingest → models.
    if ingest_task_names:
        for task in tasks:
            if task.task_type in ("dbt", "sql") and task.models and not task.depends_on:
                task.depends_on = list(ingest_task_names)

    # ── Custom tasks ──────────────────────────────────
    for stage in STAGES:
        for ct in custom_by_stage[stage]:
            ct_name = f"{stage}_{ct['task_key']}"
            ct_deps: list[str] = []

            if ct["depends_on"]:
                all_task_names = {t.name for t in tasks}
                for dep in ct["depends_on"]:
                    resolved = _resolve_task_dependency(dep, all_task_names, model_to_task)
                    if resolved and resolved not in ct_deps:
                        ct_deps.append(resolved)
            else:
                stage_tasks_so_far = [t.name for t in tasks if t.stage == stage]
                if stage_tasks_so_far:
                    ct_deps.append(stage_tasks_so_far[-1])

            ct_config = ct.get("config", {})
            ct_timeout = ct_config.pop("timeout_seconds", None)

            tasks.append(WorkflowTask(
                name=ct_name,
                stage=stage,
                task_type=ct["task_type"],
                python_file=ct["file"] if ct["task_type"] == "python" else None,
                sql_file=ct["file"] if ct["task_type"] == "sql" else None,
                additional_config=ct_config or None,
                depends_on=ct_deps,
                compute_type=compute_type,
                timeout_minutes=ct_timeout // 60 if ct_timeout else 60,
            ))

    return Workflow(
        name=wf_prefix,
        tasks=tasks,
        schedule=schedule,
        environment=environment,
        catalog=resolved_catalog,
        schema=schema,
        compute_type=compute_type,
        warehouse_id=warehouse_id,
        dbt_vars=dbt_vars,
    )


def build_domain_workflows(
    forge_config: dict,
    graph: dict,
    sql_mode: bool = False,
) -> list[Workflow]:
    """Build one Databricks Workflow per domain.

    In v1, domain membership comes from contract metadata rather than
    model-name suffixes. Each workflow contains contracts tagged for that
    domain plus shared contracts with no domain assignment.

    Returns a list of Workflow objects — one per domain defined in
    ``forge_config["domains"]``.  Falls back to a single shared workflow
    if no domains are configured or ``domain_workflows: shared`` is set.
    """
    domains = forge_config.get("domains", {})
    mode = forge_config.get("domain_workflows", "separate")  # "separate" | "shared"
    project_id = forge_config.get("id", forge_config.get("name", "unnamed"))

    if not domains or mode == "shared":
        process_workflows = [build_workflow(forge_config, graph, sql_mode=sql_mode)]
    else:
        process_workflows = []
        contracts = graph.get("contracts", {})
        edges = graph.get("edges", [])

        for domain_name in domains:
            workflow_suffix = "" if domain_name == project_id else f"_{domain_name}"

            # Filter contracts: keep this domain's contracts plus shared/system
            # contracts that do not declare a business domain.
            domain_cids: set[str] = set()
            for cid, contract in contracts.items():
                meta_domain = contract.get("_meta", {}).get("domain")
                if meta_domain == domain_name:
                    domain_cids.add(cid)
                elif meta_domain in (None, False):
                    domain_cids.add(cid)

            # Filter edges to only those between included contracts
            domain_edges = [e for e in edges
                            if e["from"] in domain_cids and e["to"] in domain_cids]

            # Build a scoped graph
            domain_graph = {
                "contracts": {cid: contracts[cid] for cid in domain_cids if cid in contracts},
                "edges": domain_edges,
            }

            # Build the workflow with a domain-aware name.
            domain_config = {**forge_config}
            domain_config["_domain"] = domain_name
            wf = build_workflow(domain_config, domain_graph, sql_mode=sql_mode)
            wf.name = f"{wf.name}{workflow_suffix}"
            process_workflows.append(wf)

    # Build setup + process + teardown workflows
    workflows: list[Workflow] = []
    workflows.append(build_setup_workflow(forge_config))
    workflows.extend(process_workflows)
    workflows.append(build_teardown_workflow(forge_config, graph))

    return workflows


def build_setup_workflow(forge_config: dict) -> Workflow:
    """Build a SETUP workflow: dbt deps plus setup SQL tasks.

    This is a separate job that runs before the main PROCESS workflow.
    """
    project_name = forge_config.get("name", "unnamed")
    project_id = forge_config.get("id", project_name)
    environment = forge_config.get("environment", "dev")
    schema = forge_config.get("schema", "default")
    compute_type = forge_config.get("compute", {}).get("type", "serverless")

    # Resolve logical catalog name to concrete name
    profile_config = resolve_profile(forge_config)
    logical_catalog = profile_config.get("catalog", forge_config.get("catalog", "main"))
    dbt_vars = _resolve_dbt_vars(forge_config)
    resolved_catalog = dbt_vars.get(f"catalog_{logical_catalog}", logical_catalog)

    warehouse_id = None
    if compute_type == "serverless":
        try:
            databricks_profile = profile_config.get("databricks_profile", "DEFAULT")
            config = read_databrickscfg(databricks_profile)
            http_path = config.get("http_path")
            if http_path and "/warehouses/" in http_path:
                warehouse_id = http_path.split("/warehouses/")[-1]
        except Exception:
            pass

    # Setup only prepares dbt dependencies. Seed-like data is compiled into setup SQL.
    commands = ["dbt deps"]

    tasks = [
        WorkflowTask(
            name="setup",
            stage="ingest",
            task_type="dbt",
            models=[],
            dbt_commands=commands,
            depends_on=[],
            compute_type=compute_type,
        )
    ]

    # UDFs task — runs after setup so catalog/schema exist
    udf_sql = SQL_SETUP_DIR / "000_udfs.sql"
    if udf_sql.exists():
        tasks.append(WorkflowTask(
            name="create_udfs",
            stage="ingest",
            task_type="sql",
            sql_file=_sql_task_path(udf_sql),
            models=[],
            depends_on=["setup"],
            compute_type=compute_type,
        ))

    # Lineage graph task — creates tables + seeds DAG edges
    lineage_sql = SQL_SETUP_DIR / "001_lineage_graph.sql"
    if lineage_sql.exists():
        lineage_deps = ["create_udfs"] if udf_sql.exists() else ["setup"]
        tasks.append(WorkflowTask(
            name="seed_lineage_graph",
            stage="ingest",
            task_type="sql",
            sql_file=_sql_task_path(lineage_sql),
            models=[],
            depends_on=lineage_deps,
            compute_type=compute_type,
        ))

    # Quarantine tables task — creates transform_quarantine + ingest_quarantine
    sql_dir_setup = SQL_SETUP_DIR
    if sql_dir_setup.is_dir():
        for f in sorted(sql_dir_setup.iterdir()):
            if f.name.endswith("_quarantine_tables.sql"):
                q_deps = [tasks[-1].name] if tasks else ["setup"]
                tasks.append(WorkflowTask(
                    name="create_quarantine_tables",
                    stage="ingest",
                    task_type="sql",
                    sql_file=_sql_task_path(f),
                    models=[],
                    depends_on=q_deps,
                    compute_type=compute_type,
                ))
                break

    # Volumes task — CREATE VOLUME IF NOT EXISTS
    sql_dir = SQL_SETUP_DIR
    if sql_dir.is_dir():
        for f in sorted(sql_dir.iterdir()):
            if f.name.endswith("_volumes.sql"):
                try:
                    header = f.read_text().split("\n", 3)[:3]
                except Exception:
                    continue
                if any("-- Volumes" in line for line in header):
                    vol_deps = [tasks[-1].name] if tasks else ["setup"]
                    tasks.append(WorkflowTask(
                        name="create_volumes",
                        stage="ingest",
                        task_type="sql",
                        sql_file=_sql_task_path(f),
                        models=[],
                        depends_on=vol_deps,
                        compute_type=compute_type,
                    ))
                    break

    # Model definition SQL files — physical tables are defined in SETUP.
    sql_dir = SQL_SETUP_DIR
    if sql_dir.is_dir():
        last_setup_task = tasks[-1].name if tasks else "setup"
        for f in sorted(sql_dir.iterdir()):
            if f.suffix != ".sql":
                continue
            try:
                header = f.read_text().split("\n", 5)[:5]
            except Exception:
                continue
            is_model_setup = any(line.startswith("-- Model:") for line in header)
            is_seed = any("-- Seed:" in line for line in header)
            if not is_model_setup or is_seed:
                continue
            # Extract model name from "NNN_model_name.sql"
            parts = f.stem.split("_", 1)
            if not (parts[0].isdigit() and len(parts) > 1):
                continue
            model_name = parts[1]
            task_name = f"create_{model_name}"
            tasks.append(WorkflowTask(
                name=task_name,
                stage="ingest",
                task_type="sql",
                sql_file=_sql_task_path(f),
                models=[],
                depends_on=[last_setup_task],
                compute_type=compute_type,
            ))

    # Seed SQL files — seeds compiled to pure SQL (catalog-overridden seeds)
    if sql_dir.is_dir():
        last_setup_task = tasks[-1].name if tasks else "setup"
        for f in sorted(sql_dir.iterdir()):
            if f.suffix != ".sql":
                continue
            try:
                header = f.read_text().split("\n", 5)[:5]
            except Exception:
                continue
            is_seed = any("-- Seed:" in line for line in header)
            if not is_seed:
                continue
            parts = f.stem.split("_", 1)
            if not (parts[0].isdigit() and len(parts) > 1):
                continue
            seed_name = parts[1]
            task_name = f"seed_{seed_name}"
            tasks.append(WorkflowTask(
                name=task_name,
                stage="ingest",
                task_type="sql",
                sql_file=_sql_task_path(f),
                models=[],
                depends_on=[last_setup_task],
                compute_type=compute_type,
            ))

    return Workflow(
        name=f"SETUP_{project_id}",
        tasks=tasks,
        environment=environment,
        catalog=resolved_catalog,
        schema=schema,
        compute_type=compute_type,
        warehouse_id=warehouse_id,
        dbt_vars=dbt_vars,
    )


def build_teardown_workflow(forge_config: dict, graph: dict) -> Workflow:
    """Build a TEARDOWN workflow: drops tables/schemas from the lineage graph.

    This is a separate job that can be run to clean up resources.
    """
    project_name = forge_config.get("name", "unnamed")
    project_id = forge_config.get("id", project_name)
    environment = forge_config.get("environment", "dev")
    schema = forge_config.get("schema", "default")
    compute_type = forge_config.get("compute", {}).get("type", "serverless")

    # Resolve logical catalog name to concrete name
    profile_config = resolve_profile(forge_config)
    logical_catalog = profile_config.get("catalog", forge_config.get("catalog", "main"))
    dbt_vars = _resolve_dbt_vars(forge_config)
    resolved_catalog = dbt_vars.get(f"catalog_{logical_catalog}", logical_catalog)

    warehouse_id = None
    if compute_type == "serverless":
        try:
            databricks_profile = profile_config.get("databricks_profile", "DEFAULT")
            config = read_databrickscfg(databricks_profile)
            http_path = config.get("http_path")
            if http_path and "/warehouses/" in http_path:
                warehouse_id = http_path.split("/warehouses/")[-1]
        except Exception:
            pass

    # Collect model names from graph (reverse order for teardown)
    contracts = graph.get("contracts", {})
    model_names = []
    for cid, contract in contracts.items():
        dataset_type = contract["dataset"]["type"]
        if dataset_type not in ("model", "table", "view", "incremental"):
            continue
        if cid.startswith(("workflow.", "custom_task.", "check.", "volume.", "source.", "seed.")):
            continue
        tags = set(contract.get("tags", []))
        if tags & {"quarantine", "prior_version", "auto-generated"}:
            continue
        model_names.append(contract["dataset"]["name"])

    # Backup (deep-clone all tables) → then teardown (drop everything)
    tasks = [
        WorkflowTask(
            name="backup",
            stage="serve",
            task_type="sql",
            sql_file=_sql_task_path(SQL_TEARDOWN_DIR / "backup.sql"),
            models=[],
            depends_on=[],
            compute_type=compute_type,
        ),
        WorkflowTask(
            name="teardown",
            stage="serve",
            task_type="sql",
            sql_file=_sql_task_path(SQL_TEARDOWN_DIR / "teardown.sql"),
            models=[],
            depends_on=["backup"],
            compute_type=compute_type,
        ),
    ]

    return Workflow(
        name=f"TEARDOWN_{project_id}",
        tasks=tasks,
        environment=environment,
        catalog=resolved_catalog,
        schema=schema,
        compute_type=compute_type,
        warehouse_id=warehouse_id,
        dbt_vars=dbt_vars,
    )
