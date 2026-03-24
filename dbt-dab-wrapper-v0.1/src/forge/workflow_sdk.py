# =============================================
# src/forge/workflow_sdk.py
# =============================================
# THE CODE IS THE DOCUMENTATION
#
# Simplified workflow generation using Databricks SDK.
# Minimal code overhead - constructs workflows programmatically.
#
# Uses databricks-sdk to create and manage Databricks jobs directly,
# bypassing YAML generation for a more direct API approach.
#
# Usage:
#   from forge.workflow_sdk import WorkflowSDK
#   wf = WorkflowSDK(forge_config, graph)
#   wf.create_or_update_job()  # Creates job via SDK
#   wf.run_job()               # Triggers job execution
#   wf.get_job_status()        # Gets current status

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    JobSettings,
    Task,
    DbtTask,
    SqlTask,
    SparkPythonTask,
    NotebookTask,
    TaskDependency,
    JobEnvironment,
    EnvironmentSpec,
    PypiPackage,
)


@dataclass
class WorkflowTaskSDK:
    """One task in the Databricks workflow DAG using SDK objects."""
    name: str
    stage: str
    task_type: str = "dbt"  # "dbt", "python", "sql", "notebook"
    models: list[str] = field(default_factory=list)
    python_file: Optional[str] = None
    sql_file: Optional[str] = None
    notebook_path: Optional[str] = None
    additional_config: Optional[dict] = None
    depends_on: list[str] = field(default_factory=list)
    compute_type: str = "serverless"
    timeout_seconds: int = 3600
    warehouse_id: Optional[str] = None


class WorkflowSDK:
    """
    Databricks Workflow using SDK for minimal overhead.
    Constructs jobs programmatically without YAML generation.
    """

    def __init__(
        self,
        forge_config: dict,
        graph: dict,
        workspace_client: Optional[WorkspaceClient] = None
    ):
        self.forge_config = forge_config
        self.graph = graph
        self.workspace_client = workspace_client or self._create_workspace_client()

        # Extract config
        self.name = f"PROCESS_{forge_config.get('id', forge_config.get('name', 'unnamed'))}"
        self.environment = forge_config.get("environment", "dev")
        self.compute_type = forge_config.get("compute", {}).get("type", "serverless")
        self.warehouse_id = self._get_warehouse_id()

        # Build tasks from graph
        self.tasks = self._build_tasks()

    def _create_workspace_client(self) -> WorkspaceClient:
        """Create Databricks workspace client from config."""
        # Use the active profile from forge config
        active_profile = self.forge_config.get("active_profile", "dev")
        profile_config = self.forge_config.get("profiles", {}).get(active_profile, {})
        databricks_profile = profile_config.get("databricks_profile", "DEFAULT")

        return WorkspaceClient(profile=databricks_profile)

    def _get_warehouse_id(self) -> Optional[str]:
        """Extract warehouse ID for serverless compute."""
        if self.compute_type != "serverless":
            return None

        try:
            active_profile = self.forge_config.get("active_profile", "dev")
            profile_config = self.forge_config.get("profiles", {}).get(active_profile, {})
            databricks_profile = profile_config.get("databricks_profile", "DEFAULT")

            from forge.compute_resolver import read_databrickscfg
            config = read_databrickscfg(databricks_profile)
            http_path = config.get("http_path")
            if http_path and "/warehouses/" in http_path:
                return http_path.split("/warehouses/")[-1]
        except Exception:
            pass
        return None

    def _build_tasks(self) -> list[WorkflowTaskSDK]:
        """Build tasks from the lineage graph - matching original workflow behavior."""
        from forge.workflow import _assign_stage, _compute_depths, STAGES

        tasks = []
        contracts = self.graph.get("contracts", {})

        depths = _compute_depths(self.graph)

        # Classify every model into a stage + collect contract IDs
        stage_models: dict[str, list[str]] = {s: [] for s in STAGES}
        model_stages: dict[str, str] = {}            # model_name → stage
        model_to_cid: dict[str, str] = {}            # model_name → contract_id

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

            model_name = contract["dataset"]["name"]
            depth = depths.get(cid, 0)
            has_quarantine = contract.get("provenance", {}).get("uses_quarantine", False)

            stage = _assign_stage(model_name, contract, depth, has_quarantine)
            stage_models[stage].append(model_name)
            model_stages[model_name] = stage
            model_to_cid[model_name] = cid

        # Build lineage parent map: model_name → [upstream model names]
        edges = self.graph.get("edges", [])
        cid_to_model: dict[str, str] = {v: k for k, v in model_to_cid.items()}
        model_parents: dict[str, list[str]] = {m: [] for m in model_stages}
        for edge in edges:
            src_model = cid_to_model.get(edge["from"])
            tgt_model = cid_to_model.get(edge["to"])
            if src_model and tgt_model and src_model in model_stages and tgt_model in model_stages:
                if src_model not in model_parents[tgt_model]:
                    model_parents[tgt_model].append(src_model)

        # Create per-model tasks (matching original behavior)
        model_to_task: dict[str, str] = {}  # model_name → task_key

        for stage in STAGES:
            models = sorted(stage_models[stage])
            for model_name in models:
                task_name = f"{stage}_{model_name}"
                task = WorkflowTaskSDK(
                    name=task_name,
                    stage=stage,
                    task_type="dbt",
                    models=[model_name],
                    compute_type=self.compute_type,
                    warehouse_id=self.warehouse_id,
                )
                tasks.append(task)
                model_to_task[model_name] = task_name

        # Wire dependencies based on lineage
        for task in tasks:
            if task.task_type == "dbt" and task.models:
                model_name = task.models[0]
                for parent in model_parents.get(model_name, []):
                    parent_task = model_to_task.get(parent)
                    if parent_task and parent_task not in [d for d in task.depends_on]:
                        task.depends_on.append(parent_task)

        return tasks

    def _create_job_settings(self) -> JobSettings:
        """Create JobSettings object for the workflow."""
        tasks = []

        for task in self.tasks:
            # Create task based on type
            if task.task_type == "dbt":
                dbt_task = DbtTask(
                    project_directory="${workspace.file_path}",
                    commands=["dbt deps", f"dbt run --select {' '.join(task.models)}"],
                )
                if task.warehouse_id:
                    dbt_task.warehouse_id = task.warehouse_id

                task_obj = Task(
                    task_key=task.name,
                    dbt_task=dbt_task,
                    timeout_seconds=task.timeout_seconds,
                )

            elif task.task_type == "python":
                python_task = SparkPythonTask(
                    python_file=task.python_file,
                )
                task_obj = Task(
                    task_key=task.name,
                    spark_python_task=python_task,
                    timeout_seconds=task.timeout_seconds,
                )

            elif task.task_type == "sql":
                sql_task = SqlTask(
                    file={"path": task.sql_file}
                )
                task_obj = Task(
                    task_key=task.name,
                    sql_task=sql_task,
                    timeout_seconds=task.timeout_seconds,
                )

            elif task.task_type == "notebook":
                notebook_task = NotebookTask(
                    notebook_path=task.notebook_path,
                )
                task_obj = Task(
                    task_key=task.name,
                    notebook_task=notebook_task,
                    timeout_seconds=task.timeout_seconds,
                )

            # Add environment key for serverless
            if task.compute_type == "serverless":
                task_obj.environment_key = "default"

            # Add dependencies
            if task.depends_on:
                task_obj.depends_on = [
                    TaskDependency(task_key=dep) for dep in task.depends_on
                ]

            tasks.append(task_obj)

        # Create environments for serverless
        environments = []
        if any(t.compute_type == "serverless" for t in self.tasks):
            environments.append(JobEnvironment(
                environment_key="default",
                spec=EnvironmentSpec(
                    client="1",
                    dependencies=["dbt-databricks"]
                )
            ))

        return JobSettings(
            name=f"{self.name} ({self.environment})",
            tasks=tasks,
            environments=environments if environments else None,
        )

    def create_or_update_job(self) -> str:
        """Create or update the job using the SDK."""
        job_settings = self._create_job_settings()

        # Check if job exists
        existing_jobs = self.workspace_client.jobs.list(name=job_settings.name)
        existing_job = next((j for j in existing_jobs if j.settings.name == job_settings.name), None)

        if existing_job:
            # Update existing job
            self.workspace_client.jobs.update(
                job_id=existing_job.job_id,
                fields_to_remove=[],  # Could be more sophisticated
                job_settings=job_settings
            )
            return f"Updated job: {existing_job.job_id}"
        else:
            # Create new job
            response = self.workspace_client.jobs.create(job_settings=job_settings)
            return f"Created job: {response.job_id}"

    def run_job(self, job_id: str) -> str:
        """Trigger a job run by job ID."""
        run_response = self.workspace_client.jobs.run_now(job_id=job_id)
        return f"Started job run: {run_response.run_id}"

    def get_job_status(self, run_id: str) -> dict:
        """Get status of a job run."""
        run = self.workspace_client.jobs.get_run(run_id=run_id)
        return {
            "run_id": run.run_id,
            "state": run.state.life_cycle_state.value if run.state else None,
            "result": run.state.result_state.value if run.state and run.state.result_state else None,
        }

    def list_jobs(self) -> list[dict]:
        """List all jobs with this workflow name."""
        jobs = self.workspace_client.jobs.list(name=f"{self.name} ({self.environment})")
        return [
            {
                "job_id": job.job_id,
                "name": job.settings.name,
                "created_time": job.created_time,
            }
            for job in jobs
        ]

    def delete_job(self, job_id: str) -> None:
        """Delete a job by ID."""
        self.workspace_client.jobs.delete(job_id=job_id)