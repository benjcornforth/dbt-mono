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


# =============================================
# STAGE DEFINITIONS
# =============================================

STAGES = ["ingest", "stage", "clean", "enrich", "serve"]

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
    task_type: str = "dbt"             # "dbt", "python", "sql", "notebook"
    models: list[str] = field(default_factory=list)
    python_file: str | None = None      # path for spark_python_task
    sql_file: str | None = None         # path for sql_task
    notebook_path: str | None = None    # path for notebook_task
    additional_config: dict | None = None  # extra DAB keys (warehouse_id, base_parameters, etc.)
    depends_on: list[str] = field(default_factory=list)
    compute_type: str = "serverless"
    timeout_minutes: int = 60


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
                    **({"notebook_path": t.notebook_path} if t.notebook_path else {}),
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

        for task in self.tasks:
            task_def: dict[str, Any] = {
                "task_key": task.name,
                "timeout_seconds": task.timeout_minutes * 60,
            }

            if task.task_type == "python" and task.python_file:
                # spark_python_task — runs a .py file on the cluster
                task_def["spark_python_task"] = {
                    "python_file": task.python_file,
                    **(task.additional_config or {}),
                }
            elif task.task_type == "sql" and task.sql_file:
                # sql_task — runs a .sql file on a SQL warehouse
                task_def["sql_task"] = {
                    "file": {"path": task.sql_file},
                    **(task.additional_config or {}),
                }
            elif task.task_type == "notebook" and task.notebook_path:
                # notebook_task — runs a Databricks notebook
                task_def["notebook_task"] = {
                    "notebook_path": task.notebook_path,
                    **(task.additional_config or {}),
                }
            else:
                # dbt_task — runs dbt commands
                task_def["dbt_task"] = {
                    "project_directory": "dbt",
                    "commands": [
                        f"dbt run --select {' '.join(task.models)}"
                    ],
                    "catalog": self.catalog,
                    "schema": self.schema,
                }

            if task.compute_type == "serverless":
                task_def["environment_key"] = "default"
            else:
                task_def["job_cluster_key"] = "dedicated"

            if task.depends_on:
                task_def["depends_on"] = [
                    {"task_key": d} for d in task.depends_on
                ]

            job["resources"]["jobs"][self.name]["tasks"].append(task_def)

        return yaml.dump(job, sort_keys=False, default_flow_style=False)

    def to_mermaid(self) -> str:
        """Render the workflow DAG as a Mermaid diagram."""
        lines = ["graph LR"]

        # Stage subgraphs
        stage_tasks: dict[str, list[WorkflowTask]] = {s: [] for s in STAGES}
        for task in self.tasks:
            stage_tasks.get(task.stage, stage_tasks["enrich"]).append(task)

        stage_colours = {
            "ingest": "#e3f2fd",
            "stage": "#f3e5f5",
            "clean": "#fff3e0",
            "enrich": "#e8f5e9",
            "serve": "#fce4ec",
        }

        for stage in STAGES:
            tasks = stage_tasks[stage]
            if not tasks:
                continue
            colour = stage_colours.get(stage, "#ffffff")
            lines.append(f"    subgraph {stage.upper()}")
            lines.append(f"        style {stage.upper()} fill:{colour}")
            for task in tasks:
                safe = task.name.replace("-", "_")
                model_list = ", ".join(task.models[:3])
                if len(task.models) > 3:
                    model_list += f" +{len(task.models)-3}"
                if task.task_type == "python":
                    lines.append(f'        {safe}["{task.name}<br/>🐍 {task.python_file or model_list}"]')
                elif task.task_type == "sql":
                    lines.append(f'        {safe}["{task.name}<br/>📄 {task.sql_file}"]')
                elif task.task_type == "notebook":
                    lines.append(f'        {safe}["{task.name}<br/>📓 {task.notebook_path}"]')
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


# =============================================
# DATABRICKS BUNDLE CONFIG (databricks.yml)
# =============================================

def generate_bundle_config(forge_config: dict, job_yaml_paths: list[str] | None = None) -> str:
    """
    Generate a root databricks.yml that ties the DAB bundle together.

    This is the entry point file that `databricks bundle deploy` reads.
    It references the generated job YAMLs via include paths.
    """
    project_name = forge_config.get("name", "unnamed")
    project_id = forge_config.get("id", project_name)
    profile = forge_config.get("active_profile", "dev")

    # Resolve the workspace host from the active profile
    profiles = forge_config.get("profiles", {})
    active_prof = profiles.get(profile, {})
    databricks_profile = active_prof.get("databricks_profile", "DEFAULT")

    bundle: dict[str, Any] = {
        "bundle": {
            "name": f"{project_name}_{project_id}",
        },
        "workspace": {
            "profile": databricks_profile,
        },
        "include": job_yaml_paths or ["resources/jobs/*.yml"],
    }

    return yaml.dump(bundle, sort_keys=False, default_flow_style=False)


def run_bundle_command(command: str, target: str | None = None) -> dict:
    """
    Run `databricks bundle <command>` (deploy, destroy, validate).

    Returns {"success": bool, "output": str, "error": str | None}.
    """
    import subprocess

    cmd = ["databricks", "bundle", command]
    if target:
        cmd += ["--target", target]

    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
        )
        return {"success": True, "output": result.stdout, "error": None}
    except FileNotFoundError:
        return {
            "success": False,
            "output": "",
            "error": (
                "Databricks CLI not found or too old. Install v0.200+:\n"
                "  brew install databricks/tap/databricks\n"
                "Or: curl -fsSL https://raw.githubusercontent.com/"
                "databricks/setup-cli/main/install.sh | sh"
            ),
        }
    except subprocess.CalledProcessError as e:
        return {
            "success": False,
            "output": e.stdout or "",
            "error": (e.stderr or "").strip(),
        }


# =============================================
# CUSTOM TASKS – user-defined sql/notebook/python tasks
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
          - task_key: notify_slack
            notebook_task: notebooks/slack_alert.ipynb
            stage: serve
            depends_on: [customer_summary]
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
            task_type = "notebook"
            file_path = task["notebook_task"]
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


def build_workflow(
    forge_config: dict,
    graph: dict,
) -> Workflow:
    """
    Build a Databricks Workflow from the lineage graph.

    Default: one task per model (per_model_tasks=true).
    Each task depends on its actual upstream models from the lineage graph.
    Task naming: {stage}_{model_name} (e.g. ingest_raw_customers).

    Set per_model_tasks: false in forge.yml to group models by stage instead.

    Python tasks and custom tasks are merged in by stage.
    """
    project_name = forge_config.get("name", "unnamed")
    project_id = forge_config.get("id", project_name)
    wf_prefix = f"PROCESS_{project_id}"
    environment = forge_config.get("environment", "dev")
    catalog = forge_config.get("catalog", "main")
    schema = forge_config.get("schema", "default")
    compute_type = forge_config.get("compute", {}).get("type", "serverless")
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
        if contract["dataset"]["type"] in ("function", "workflow", "workflow_task"):
            continue
        if cid.startswith("workflow.") or cid.startswith("custom_task."):
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

    # ── Load python_tasks from forge.yml ──────────────
    from forge.python_task import load_python_tasks
    python_tasks = load_python_tasks(forge_config)
    python_by_stage: dict[str, list[dict]] = {s: [] for s in STAGES}
    for pt in python_tasks:
        python_by_stage.get(pt["stage"], python_by_stage["enrich"]).append(pt)

    # ── Load custom_tasks from forge.yml ──────────────
    custom_tasks = load_custom_tasks(forge_config)
    custom_by_stage: dict[str, list[dict]] = {s: [] for s in STAGES}
    for ct in custom_tasks:
        custom_by_stage.get(ct["stage"], custom_by_stage["enrich"]).append(ct)

    # ── Validate all referenced asset files exist ─────
    missing: list[str] = []
    for pt in python_tasks:
        if pt.get("file") and not Path(pt["file"]).exists():
            missing.append(f"python_task '{pt['name']}': {pt['file']}")
    for ct in custom_tasks:
        if ct.get("file") and not Path(ct["file"]).exists():
            missing.append(f"custom_task '{ct['task_key']}': {ct['file']}")
    if missing:
        raise FileNotFoundError(
            "Referenced task assets not found:\n"
            + "\n".join(f"  - {m}" for m in missing)
            + "\nCreate these files or remove the tasks from forge.yml."
        )

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
                tasks.append(WorkflowTask(
                    name=task_name,
                    stage=stage,
                    task_type="dbt",
                    models=[model_name],
                    depends_on=[],
                    compute_type=compute_type,
                ))
                model_to_task[model_name] = task_name

        # Pass 2: wire real lineage deps
        for task in tasks:
            if task.task_type != "dbt" or not task.models:
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
    for stage in STAGES:
        for pt in python_by_stage[stage]:
            pt_name = f"{stage}_py_{pt['name']}"
            pt_deps: list[str] = []
            for dep in pt.get("depends_on", []):
                resolved = _resolve_task_dependency(dep, {t.name for t in tasks}, model_to_task)
                if resolved and resolved not in pt_deps:
                    pt_deps.append(resolved)
            if not pt_deps:
                stage_dbt = [t.name for t in tasks if t.stage == stage and t.task_type == "dbt"]
                if stage_dbt:
                    pt_deps.append(stage_dbt[-1])

            tasks.append(WorkflowTask(
                name=pt_name,
                stage=stage,
                task_type="python",
                python_file=pt["file"],
                depends_on=pt_deps,
                compute_type=compute_type,
            ))

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
                notebook_path=ct["file"] if ct["task_type"] == "notebook" else None,
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
        catalog=catalog,
        schema=schema,
        compute_type=compute_type,
    )
