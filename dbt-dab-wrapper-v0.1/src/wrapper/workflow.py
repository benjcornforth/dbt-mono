# =============================================
# src/wrapper/workflow.py
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
#   from wrapper.workflow import build_workflow
#   wf = build_workflow(wrapper_config, graph)
#   wf.to_databricks_yml()   # → databricks.yml jobs section
#   wf.to_mermaid()          # → visual DAG
#   wf.to_dict()             # → serialisable dict
#
# The wrapper CLI command:
#   wrapper workflow            # prints the DAG
#   wrapper workflow --deploy   # deploys via DAB
#   wrapper workflow --mermaid  # emits Mermaid diagram
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
    models: list[str] = field(default_factory=list)
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
                    "models": t.models,
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
                "dbt_task": {
                    "project_directory": "dbt",
                    "commands": [
                        f"dbt run --select {' '.join(task.models)}"
                    ],
                    "catalog": self.catalog,
                    "schema": self.schema,
                },
                "timeout_seconds": task.timeout_minutes * 60,
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
    wrapper_config: dict,
    graph: dict,
) -> Workflow:
    """
    Build a Databricks Workflow from the lineage graph.

    Each stage gets one task containing all models
    assigned to that stage. Dependencies between tasks
    mirror the stage ordering.
    """
    project_name = wrapper_config.get("name", "unnamed")
    environment = wrapper_config.get("environment", "dev")
    catalog = wrapper_config.get("catalog", "main")
    schema = wrapper_config.get("schema", "default")
    compute_type = wrapper_config.get("compute", {}).get("type", "serverless")
    schedule = wrapper_config.get("schedule", None)

    depths = _compute_depths(graph)

    # Classify every model into a stage
    stage_models: dict[str, list[str]] = {s: [] for s in STAGES}
    contracts = graph.get("contracts", {})

    for cid, contract in contracts.items():
        # Skip auto-generated nodes (quarantine, prior_version, UDFs)
        tags = set(contract.get("tags", []))
        if tags & {"quarantine", "prior_version", "auto-generated"}:
            continue
        if contract["dataset"]["type"] == "function":
            continue

        model_name = contract["dataset"]["name"]
        depth = depths.get(cid, 0)
        has_quarantine = contract.get("provenance", {}).get("uses_quarantine", False)

        stage = _assign_stage(model_name, contract, depth, has_quarantine)
        stage_models[stage].append(model_name)

    # Build tasks — one per non-empty stage
    tasks: list[WorkflowTask] = []
    prev_task_name: str | None = None

    for stage in STAGES:
        models = stage_models[stage]
        if not models:
            continue

        task_name = f"{project_name}-{stage}"
        depends = [prev_task_name] if prev_task_name else []

        tasks.append(WorkflowTask(
            name=task_name,
            stage=stage,
            models=sorted(models),
            depends_on=depends,
            compute_type=compute_type,
        ))
        prev_task_name = task_name

    return Workflow(
        name=f"{project_name}-pipeline",
        tasks=tasks,
        schedule=schedule,
        environment=environment,
        catalog=catalog,
        schema=schema,
        compute_type=compute_type,
    )
