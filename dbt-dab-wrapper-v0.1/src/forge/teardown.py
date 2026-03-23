# =============================================
# src/forge/teardown.py
# =============================================
# THE CODE IS THE DOCUMENTATION
#
# Safe teardown plan generator.
# Philosophy: NEVER delete data. Only remove orchestration + code assets.
# Every step is reversible. Re-instate = forge deploy.
#
# Asset handling:
#   ✅ REMOVE  — DAB job definitions (orchestration only)
#   ✅ REMOVE  — UDFs (functions are code, always redeployable)
#   🔒 PRESERVE — Tables, views, schemas, data (NEVER touched)
#
# Mirrors the workflow generator pattern:
#   build_teardown_plan(config, graph) → TeardownPlan
#   plan.to_yaml()       → human-readable YAML
#   plan.to_summary()    → terminal-friendly summary
#   plan.execute(...)    → runs the steps (with backup first)

from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


# =============================================
# DATACLASSES
# =============================================

@dataclass
class TeardownAsset:
    """An asset that the teardown plan either removes or preserves."""
    asset_type: str          # dab_job, udf, table, schema, etc.
    name: str                # human-readable name
    action: str              # "remove", "preserve"
    method: str | None = None  # how it's removed (e.g. "DROP FUNCTION IF EXISTS")
    path: str | None = None    # file path if applicable
    note: str | None = None


@dataclass
class TeardownStep:
    """One executable step in the teardown plan."""
    step: int
    name: str
    action: str              # "copy", "delete_file", "sql", "log"
    target: str | None = None
    source: str | None = None
    statements: list[str] = field(default_factory=list)
    message: str | None = None


@dataclass
class TeardownPlan:
    """
    Complete teardown plan — generated from graph, never manually written.

    Mirrors Workflow dataclass pattern:
      plan.to_yaml()     → YAML file for inspection
      plan.to_summary()  → terminal output
      plan.execute(...)  → runs the steps
    """
    name: str
    environment: str
    project: str
    scope: str
    project_id: str
    generated_at: str
    removes: list[TeardownAsset] = field(default_factory=list)
    preserves: list[TeardownAsset] = field(default_factory=list)
    steps: list[TeardownStep] = field(default_factory=list)
    backup_dir: str = "resources/teardown/_backup"
    reinstate_command: str = "forge deploy"

    # ── Serialisation ─────────────────────────────────

    def to_dict(self) -> dict[str, Any]:
        return {
            "teardown": {
                "name": self.name,
                "environment": self.environment,
                "generated_at": self.generated_at,
                "project": self.project,
                "scope": self.scope,
                "id": self.project_id,
                "removes": [
                    {k: v for k, v in {
                        "type": a.asset_type,
                        "name": a.name,
                        "action": a.method or "remove",
                        "path": a.path,
                        "note": a.note,
                    }.items() if v is not None}
                    for a in self.removes
                ],
                "preserves": {
                    "tables": [a.name for a in self.preserves if a.asset_type == "table"],
                    "schemas": [a.name for a in self.preserves if a.asset_type == "schema"],
                    "note": "All tables and data are preserved. Nothing is dropped.",
                },
                "backup": {
                    "directory": self.backup_dir,
                    "note": "Workflow YAML is copied before removal",
                },
                "steps": [
                    {k: v for k, v in {
                        "step": s.step,
                        "name": s.name,
                        "action": s.action,
                        "source": s.source,
                        "target": s.target,
                        "statements": s.statements or None,
                        "message": s.message,
                    }.items() if v is not None}
                    for s in self.steps
                ],
                "reinstate": {
                    "command": self.reinstate_command,
                    "note": "Re-running deploy recreates the workflow and all orchestration from source",
                },
            }
        }

    def to_yaml(self) -> str:
        return yaml.dump(self.to_dict(), sort_keys=False, default_flow_style=False)

    def to_summary(self) -> str:
        """Terminal-friendly summary of the plan."""
        lines: list[str] = []
        lines.append(f"🛑  Teardown plan: {self.name}")
        lines.append(f"    Environment: {self.environment}")
        lines.append(f"    Generated:   {self.generated_at}")
        lines.append("")

        if self.removes:
            lines.append("  📋 REMOVES (orchestration + code only):")
            for a in self.removes:
                method = f" ({a.method})" if a.method else ""
                lines.append(f"    • {a.asset_type}: {a.name}{method}")
        else:
            lines.append("  📋 Nothing to remove.")

        lines.append("")
        tables = [a.name for a in self.preserves if a.asset_type == "table"]
        if tables:
            lines.append(f"  🔒 PRESERVES ({len(tables)} tables — data is NEVER deleted):")
            for t in tables:
                lines.append(f"    • {t}")

        lines.append("")
        lines.append(f"  📦 Backup dir: {self.backup_dir}")
        lines.append(f"  ♻️  Re-instate: {self.reinstate_command}")
        return "\n".join(lines)

    # ── Execution ─────────────────────────────────────

    def execute(self, dry_run: bool = True) -> list[str]:
        """
        Execute the teardown plan.

        dry_run=True (default): only prints what would happen.
        dry_run=False: actually runs steps.

        Returns list of log messages.
        """
        log: list[str] = []
        prefix = "[DRY RUN] " if dry_run else ""

        for step in self.steps:
            if step.action == "copy":
                log.append(f"{prefix}Step {step.step}: backup {step.source} → {step.target}")
                if not dry_run and step.source and step.target:
                    src = Path(step.source)
                    tgt = Path(step.target)
                    if src.exists():
                        tgt.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(src, tgt)
                        log.append(f"  ✅ Backed up")
                    else:
                        log.append(f"  ⚠️  Source not found (already removed?)")

            elif step.action == "delete_file":
                log.append(f"{prefix}Step {step.step}: remove file {step.target}")
                if not dry_run and step.target:
                    p = Path(step.target)
                    if p.exists():
                        p.unlink()
                        log.append(f"  ✅ Removed")
                    else:
                        log.append(f"  ⚠️  Already removed")

            elif step.action == "sql":
                for stmt in step.statements:
                    log.append(f"{prefix}Step {step.step}: {stmt}")
                if not dry_run:
                    log.append(f"  💡 SQL statements must be run via dbt run-operation or dbsql")
                    # Actual SQL execution is deferred to the CLI layer
                    # which has access to dbt + connection details

            elif step.action == "log":
                log.append(f"{prefix}Step {step.step}: {step.message}")

        return log


# =============================================
# BUILD TEARDOWN PLAN FROM GRAPH
# =============================================

def build_teardown_plan(
    forge_config: dict,
    graph: dict,
) -> TeardownPlan:
    """
    Build a safe teardown plan from the lineage graph.

    Reads the graph to discover:
      - DAB workflow YAML (to remove the job definition)
      - UDFs (to drop — functions are code, not data)
      - Tables (to PRESERVE — never touched)
      - Schemas (to PRESERVE — only dev-down drops dev schemas)

    Returns a TeardownPlan that can be inspected (--dry-run)
    or executed (--execute).
    """
    project_name = forge_config.get("name", "unnamed")
    project_id = forge_config.get("id", project_name)
    scope = forge_config.get("scope", "")
    environment = forge_config.get("environment", "dev")
    profile = resolve_active_profile(forge_config)
    env = profile.get("env", environment)
    catalog_pattern = forge_config.get("catalog_pattern", "{catalog}")
    schema_pattern = forge_config.get("schema_pattern", "{schema}")
    now = datetime.now(timezone.utc).isoformat()

    wf_name = f"PROCESS_{project_id}"
    wf_path = f"resources/jobs/{wf_name}.yml"
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup_dir = "resources/teardown/_backup"

    contracts = graph.get("contracts", {})

    # ── Classify assets ───────────────────────────────
    removes: list[TeardownAsset] = []
    preserves: list[TeardownAsset] = []

    # 1. DAB workflow job
    if Path(wf_path).exists():
        removes.append(TeardownAsset(
            asset_type="dab_job",
            name=wf_name,
            action="remove",
            method="delete workflow YAML + databricks bundle destroy",
            path=wf_path,
        ))

    # 2. UDFs (functions are code — safe to drop)
    for cid, contract in contracts.items():
        ds = contract["dataset"]
        if ds["type"] == "function":
            # Build qualified name from naming patterns
            udf_catalog = _resolve_catalog(catalog_pattern, env, scope, "meta")
            udf_schema = _resolve_schema(schema_pattern, env, project_id, scope)
            qualified = f"{udf_catalog}.{udf_schema}.{ds['name']}"
            removes.append(TeardownAsset(
                asset_type="udf",
                name=qualified,
                action="remove",
                method=f"DROP FUNCTION IF EXISTS {qualified}",
                note="Functions are code, not data — safe to remove",
            ))

    # 3. Tables (NEVER removed)
    for cid, contract in contracts.items():
        ds = contract["dataset"]
        tags = set(contract.get("tags", []))
        if ds["type"] == "table" and "auto-generated" not in tags:
            preserves.append(TeardownAsset(
                asset_type="table",
                name=ds["name"],
                action="preserve",
                note="Data is never deleted",
            ))

    # 4. Schemas (NEVER removed — only dev-down handles dev schemas)
    preserves.append(TeardownAsset(
        asset_type="schema",
        name=f"{env} schemas",
        action="preserve",
        note="Schemas are preserved. Use forge dev-down for dev schema cleanup.",
    ))

    # ── Build steps ───────────────────────────────────
    steps: list[TeardownStep] = []
    step_num = 0

    # Step: Backup workflow YAML
    if Path(wf_path).exists():
        step_num += 1
        backup_target = f"{backup_dir}/{wf_name}_{timestamp}.yml"
        steps.append(TeardownStep(
            step=step_num,
            name="backup_workflow",
            action="copy",
            source=wf_path,
            target=backup_target,
        ))

    # Step: Remove workflow YAML file
    if Path(wf_path).exists():
        step_num += 1
        steps.append(TeardownStep(
            step=step_num,
            name="remove_workflow_yaml",
            action="delete_file",
            target=wf_path,
        ))

    # Step: Drop UDFs
    udf_stmts = [a.method for a in removes if a.asset_type == "udf" and a.method]
    if udf_stmts:
        step_num += 1
        steps.append(TeardownStep(
            step=step_num,
            name="drop_udfs",
            action="sql",
            statements=udf_stmts,
        ))

    # Step: Confirm
    table_names = [a.name for a in preserves if a.asset_type == "table"]
    step_num += 1
    steps.append(TeardownStep(
        step=step_num,
        name="confirm",
        action="log",
        message=(
            f"✅ Teardown complete.\n"
            f"📋 Tables preserved: {', '.join(table_names)}\n"
            f"🔒 Schemas preserved (use forge dev-down for dev schemas)\n"
            f"📦 Backup: {backup_dir}/\n"
            f"♻️  To re-instate: forge deploy"
        ),
    ))

    return TeardownPlan(
        name=f"TEARDOWN_{project_id}",
        environment=env,
        project=project_name,
        scope=scope,
        project_id=project_id,
        generated_at=now,
        removes=removes,
        preserves=preserves,
        steps=steps,
        backup_dir=backup_dir,
    )


# =============================================
# HELPERS
# =============================================

def resolve_active_profile(forge_config: dict) -> dict:
    """Get the active profile dict from forge.yml."""
    active = forge_config.get("active_profile", "dev")
    profiles = forge_config.get("profiles", {})
    return profiles.get(active, {})


def _resolve_catalog(pattern: str, env: str, scope: str, catalog: str) -> str:
    return pattern.format(env=env, scope=scope, catalog=catalog)


def _resolve_schema(pattern: str, env: str, project_id: str, scope: str) -> str:
    import getpass
    user = getpass.getuser()
    skip = []  # not needed for teardown — we always use current env
    return pattern.format(env=env, id=project_id, scope=scope, user=user, schema=project_id)
