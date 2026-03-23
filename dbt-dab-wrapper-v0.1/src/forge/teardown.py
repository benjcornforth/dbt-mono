# =============================================
# src/forge/teardown.py
# =============================================
# THE CODE IS THE DOCUMENTATION
#
# Safe teardown plan generator + standalone backup.
# Philosophy: NEVER delete data. Only remove orchestration + code assets.
# Every step is reversible. Re-instate = forge deploy / forge restore.
#
# Asset handling:
#   📦 ARCHIVE — Each asset = one row in _backup_archive (data + definitions)
#   📦 SNAPSHOT — UDF source, workflow YAML, forge.yml, graph JSON (local)
#   ✅ REMOVE  — DAB job definitions (orchestration only)
#   ✅ REMOVE  — UDFs (functions are code, always redeployable)
#   🔒 PRESERVE — Tables, views, schemas, data (NEVER touched)
#
# Backup (standalone):                  Teardown (destructive):
#   forge backup        files only        forge teardown        dry-run
#   forge backup --data files + archive   forge teardown --execute
#   forge backup --list show backups      forge restore --snapshot {ts}
#
# Snapshot directory structure:
#   resources/backups/{timestamp}/
#     ├── forge.yml                   # full config snapshot
#     ├── graph.json                  # full graph snapshot
#     ├── PROCESS_{id}.yml            # workflow YAML
#     ├── functions/                  # UDF source files
#     │   ├── clean_email.sql
#     │   └── ...
#     ├── dbt_models/                 # dbt model source files
#     │   ├── customer_clean.sql
#     │   └── ...
#     └── BACKUP_manifest.yml         # backup metadata
#
# Restore:
#   forge restore                     # shows available snapshots
#   forge restore --snapshot {ts}     # restores from snapshot
#   forge restore --data              # queries archive table on Databricks
#   forge restore --table {name}      # restores table data from archive
#
# Mirrors the workflow generator pattern:
#   build_teardown_plan(config, graph) → TeardownPlan
#   plan.to_yaml()       → human-readable YAML
#   plan.to_summary()    → terminal-friendly summary
#   plan.execute(...)    → runs the steps (with backup first)

from __future__ import annotations

import getpass
import json
import shutil
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


def _git_info() -> dict[str, str]:
    """Best-effort git sha + branch. Returns empty strings if not in a repo."""
    info: dict[str, str] = {"sha": "", "branch": ""}
    try:
        info["sha"] = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        pass
    try:
        info["branch"] = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        pass
    return info


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
    action: str              # "copy", "copy_dir", "delete_file", "sql", "snapshot", "log"
    target: str | None = None
    source: str | None = None
    statements: list[str] = field(default_factory=list)
    message: str | None = None
    files: list[dict[str, str]] = field(default_factory=list)  # for snapshot: [{src, dst}]


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

    archive_table: str | None = None   # fully qualified _backup_archive table
    snapshot_dir: str | None = None    # timestamped snapshot directory

    def to_dict(self) -> dict[str, Any]:
        return {
            "teardown": {
                "name": self.name,
                "environment": self.environment,
                "generated_at": self.generated_at,
                "project": self.project,
                "scope": self.scope,
                "id": self.project_id,
                "archive": {
                    "table": self.archive_table,
                    "note": "All table data is archived as JSON rows before any changes",
                } if self.archive_table else None,
                "snapshot": {
                    "directory": self.snapshot_dir,
                    "contents": "forge.yml, graph.json, workflow YAML, UDF source files, teardown plan",
                    "note": "Complete project state captured for forge restore",
                } if self.snapshot_dir else None,
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
                    "note": "Workflow YAML + table data archived before removal",
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

        if self.archive_table:
            lines.append("")
            lines.append(f"  📦 ARCHIVES all table data to: {self.archive_table}")

        if self.snapshot_dir:
            lines.append(f"  📸 SNAPSHOTS all assets to: {self.snapshot_dir}")
            lines.append(f"     (forge.yml, graph, workflow YAML, UDF source files)")

        lines.append("")
        tables = [a.name for a in self.preserves if a.asset_type == "table"]
        if tables:
            lines.append(f"  🔒 PRESERVES ({len(tables)} tables — data is NEVER deleted):")
            for t in tables:
                lines.append(f"    • {t}")

        lines.append("")
        lines.append(f"  📦 Backup dir: {self.backup_dir}")
        lines.append(f"  ♻️  Re-instate: forge deploy")
        if self.snapshot_dir:
            ts = Path(self.snapshot_dir).name
            lines.append(f"  ♻️  Restore:    forge restore --snapshot {ts}")
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

            elif step.action == "snapshot":
                log.append(f"{prefix}Step {step.step}: snapshot all assets → {step.target}")
                if not dry_run and step.target:
                    snap_dir = Path(step.target)
                    snap_dir.mkdir(parents=True, exist_ok=True)
                    for f in step.files:
                        src = Path(f["src"])
                        dst = snap_dir / f["dst"]
                        if src.exists():
                            dst.parent.mkdir(parents=True, exist_ok=True)
                            if src.is_dir():
                                shutil.copytree(src, dst, dirs_exist_ok=True)
                            else:
                                shutil.copy2(src, dst)
                            log.append(f"  ✅ {f['dst']}")
                        else:
                            log.append(f"  ⚠️  {f['dst']} (source not found)")

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

    # ── Build archive table name ─────────────────────
    ops_catalog = _resolve_catalog(catalog_pattern, env, scope, "operations")
    ops_schema = _resolve_schema(schema_pattern, env, "backups", scope)
    archive_table = f"{ops_catalog}.{ops_schema}._backup_archive"

    # Collect qualified table names for archive
    default_catalog = _resolve_catalog(catalog_pattern, env, scope, profile.get("catalog", "bronze"))
    default_schema = _resolve_schema(schema_pattern, env, project_id, scope)
    table_names_qualified: list[tuple[str, str]] = []  # (short_name, qualified_name)
    for a in preserves:
        if a.asset_type == "table":
            table_names_qualified.append((a.name, f"{default_catalog}.{default_schema}.{a.name}"))

    # ── Build snapshot directory ──────────────────────
    snapshot_dir = f"{backup_dir}/{timestamp}"

    # Reuse shared snapshot file collector
    snapshot_files = collect_snapshot_files(forge_config)

    # ── Build steps ───────────────────────────────────
    steps: list[TeardownStep] = []
    step_num = 0

    # Step 1: Snapshot all assets (before any changes)
    step_num += 1
    steps.append(TeardownStep(
        step=step_num,
        name="snapshot_all_assets",
        action="snapshot",
        target=snapshot_dir,
        files=snapshot_files,
    ))

    # ── Identity + git context ────────────────────────
    user = getpass.getuser()
    git = _git_info()

    # Step 2: Create archive table (if not exists)
    step_num += 1
    archive_catalog_schema = archive_table.rsplit('.', 1)[0]
    steps.append(TeardownStep(
        step=step_num,
        name="create_archive_table",
        action="sql",
        statements=[
            f"CREATE SCHEMA IF NOT EXISTS {archive_catalog_schema}",
            f"CREATE TABLE IF NOT EXISTS {archive_table} ("
            f"  asset_name STRING,"
            f"  asset_type STRING,"
            f"  project STRING,"
            f"  environment STRING,"
            f"  archived_at TIMESTAMP,"
            f"  performed_by STRING,"
            f"  git_sha STRING,"
            f"  git_branch STRING,"
            f"  dab_name STRING,"
            f"  row_count BIGINT,"
            f"  snapshot_data STRING"
            f") USING DELTA",
        ],
    ))

    # Step: Archive each asset as a single row
    archive_stmts: list[str] = []
    meta_vals = (
        f"'{project_name}' AS project, "
        f"'{env}' AS environment, "
        f"current_timestamp() AS archived_at, "
        f"'{user}' AS performed_by, "
        f"'{git['sha']}' AS git_sha, "
        f"'{git['branch']}' AS git_branch, "
        f"'{wf_name}' AS dab_name"
    )
    # Tables — one row per table with collect_list
    if table_names_qualified:
        for short_name, qualified_name in table_names_qualified:
            archive_stmts.append(
                f"INSERT INTO {archive_table} "
                f"SELECT '{short_name}' AS asset_name, "
                f"'table' AS asset_type, "
                f"{meta_vals}, "
                f"COUNT(*) AS row_count, "
                f"to_json(collect_list(to_json(struct(*)))) AS snapshot_data "
                f"FROM {qualified_name}"
            )
    # Definitions — models, functions, configs from snapshot files
    for sf in snapshot_files:
        src_path = Path(sf['src'])
        if not src_path.exists():
            continue
        content = src_path.read_text()
        dst = sf['dst']
        if dst.startswith('functions/'):
            a_type = 'function'
        elif dst.startswith('dbt_models/'):
            a_type = 'model'
        elif dst.endswith('.yml') and 'PROCESS_' in dst:
            a_type = 'workflow'
        else:
            a_type = 'config'
        safe_content = content.replace("'", "''")  # SQL-escape single quotes
        archive_stmts.append(
            f"INSERT INTO {archive_table} VALUES ("
            f"'{dst}', '{a_type}', "
            f"'{project_name}', '{env}', "
            f"current_timestamp(), "
            f"'{user}', '{git['sha']}', '{git['branch']}', '{wf_name}', "
            f"NULL, '{safe_content}')"
        )
    if archive_stmts:
        step_num += 1
        steps.append(TeardownStep(
            step=step_num,
            name="archive_assets",
            action="sql",
            statements=archive_stmts,
        ))

    # Step: Remove workflow YAML file (already snapshotted in step 1)
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
            "Teardown complete.\n"
            f"Snapshot saved: {snapshot_dir}/\n"
            f"Table data archived to: {archive_table}\n"
            f"Tables preserved: {', '.join(table_names)}\n"
            f"Schemas preserved (use forge dev-down for dev schemas)\n"
            f"To re-instate: forge deploy\n"
            f"To restore: forge restore --snapshot {timestamp}"
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
        archive_table=archive_table,
        snapshot_dir=snapshot_dir,
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
    user = getpass.getuser()
    return pattern.format(env=env, id=project_id, scope=scope, user=user, schema=project_id)

# =============================================
# SNAPSHOT HELPERS
# =============================================

BACKUP_DIR = "resources/backups"
"""Default backup directory. Used by both forge backup and forge teardown."""


def collect_snapshot_files(forge_config: dict) -> list[dict[str, str]]:
    """
    Collect the list of files to include in a backup snapshot.

    Returns a list of {src, dst} dicts — portable file mappings.
    Reused by both build_backup() and build_teardown_plan().
    """
    project_id = forge_config.get("id", forge_config.get("name", ""))
    wf_name = f"PROCESS_{project_id}"
    wf_path = f"resources/jobs/{wf_name}.yml"

    snapshot_files: list[dict[str, str]] = []

    # forge.yml
    if Path("forge.yml").exists():
        snapshot_files.append({"src": "forge.yml", "dst": "forge.yml"})

    # Workflow YAML
    if Path(wf_path).exists():
        snapshot_files.append({"src": wf_path, "dst": f"{wf_name}.yml"})

    # UDF source files
    udf_dir = Path("dbt/functions")
    if udf_dir.exists():
        for udf_file in sorted(udf_dir.glob("*.sql")):
            snapshot_files.append({"src": str(udf_file), "dst": f"functions/{udf_file.name}"})
        for udf_file in sorted(udf_dir.glob("*.py")):
            snapshot_files.append({"src": str(udf_file), "dst": f"functions/{udf_file.name}"})

    # dbt model source files
    models_dir = Path("dbt/models")
    if models_dir.exists():
        for model_file in sorted(models_dir.rglob("*.sql")):
            rel = model_file.relative_to(models_dir)
            snapshot_files.append({"src": str(model_file), "dst": f"dbt_models/{rel}"})
        for model_file in sorted(models_dir.rglob("*.yml")):
            rel = model_file.relative_to(models_dir)
            snapshot_files.append({"src": str(model_file), "dst": f"dbt_models/{rel}"})

    return snapshot_files


@dataclass
class BackupResult:
    """Result of a standalone backup operation."""
    timestamp: str
    snapshot_dir: str
    files_copied: list[str]
    archive_statements: list[str] = field(default_factory=list)
    manifest: dict = field(default_factory=dict)


def build_backup(
    forge_config: dict,
    graph: dict,
    *,
    include_data: bool = False,
    backup_dir: str = BACKUP_DIR,
) -> BackupResult:
    """
    Create a standalone backup snapshot.

    Copies all project files (forge.yml, workflow YAML, UDF source,
    dbt models, graph JSON) into a timestamped backup directory.

    If include_data=True, also generates SQL statements to archive
    table data into the _teardown_archive table on Databricks.

    This is the engine behind both:
      forge backup          (routine, non-destructive)
      forge teardown        (snapshot step before removal)
    """
    project_name = forge_config.get("name", "unnamed")
    project_id = forge_config.get("id", project_name)
    scope = forge_config.get("scope", "")
    profile = resolve_active_profile(forge_config)
    env = profile.get("env", forge_config.get("environment", "dev"))
    catalog_pattern = forge_config.get("catalog_pattern", "{catalog}")
    schema_pattern = forge_config.get("schema_pattern", "{schema}")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    snapshot_dir = f"{backup_dir}/{timestamp}"

    # ── Copy files ────────────────────────────────────
    snap_path = Path(snapshot_dir)
    snap_path.mkdir(parents=True, exist_ok=True)

    snapshot_files = collect_snapshot_files(forge_config)
    files_copied: list[str] = []

    for f in snapshot_files:
        src = Path(f["src"])
        dst = snap_path / f["dst"]
        if src.exists():
            dst.parent.mkdir(parents=True, exist_ok=True)
            if src.is_dir():
                shutil.copytree(src, dst, dirs_exist_ok=True)
            else:
                shutil.copy2(src, dst)
            files_copied.append(f["dst"])

    # ── Write graph JSON ──────────────────────────────
    (snap_path / "graph.json").write_text(
        json.dumps(graph, indent=2, default=str)
    )
    files_copied.append("graph.json")

    # ── Identity + git context ────────────────────────
    user = getpass.getuser()
    git = _git_info()
    wf_name = f"PROCESS_{project_id}"

    # ── Archive SQL statements (optional) ─────────────
    archive_stmts: list[str] = []
    archive_table = None
    if include_data:
        ops_catalog = _resolve_catalog(catalog_pattern, env, scope, "operations")
        ops_schema = _resolve_schema(schema_pattern, env, "backups", scope)
        archive_table = f"{ops_catalog}.{ops_schema}._backup_archive"

        archive_catalog_schema = archive_table.rsplit('.', 1)[0]
        archive_stmts.append(
            f"CREATE SCHEMA IF NOT EXISTS {archive_catalog_schema}"
        )
        archive_stmts.append(
            f"CREATE TABLE IF NOT EXISTS {archive_table} ("
            f"  asset_name STRING,"
            f"  asset_type STRING,"
            f"  project STRING,"
            f"  environment STRING,"
            f"  archived_at TIMESTAMP,"
            f"  performed_by STRING,"
            f"  git_sha STRING,"
            f"  git_branch STRING,"
            f"  dab_name STRING,"
            f"  row_count BIGINT,"
            f"  snapshot_data STRING"
            f") USING DELTA"
        )

        meta_vals = (
            f"'{project_name}' AS project, "
            f"'{env}' AS environment, "
            f"current_timestamp() AS archived_at, "
            f"'{user}' AS performed_by, "
            f"'{git['sha']}' AS git_sha, "
            f"'{git['branch']}' AS git_branch, "
            f"'{wf_name}' AS dab_name"
        )

        # Tables — one row per table with collect_list
        contracts = graph.get("contracts", {})
        default_catalog = _resolve_catalog(catalog_pattern, env, scope, profile.get("catalog", "bronze"))
        default_schema = _resolve_schema(schema_pattern, env, project_id, scope)
        for _cid, contract in contracts.items():
            ds = contract["dataset"]
            tags = set(contract.get("tags", []))
            if ds["type"] == "table" and "auto-generated" not in tags:
                qualified = f"{default_catalog}.{default_schema}.{ds['name']}"
                archive_stmts.append(
                    f"INSERT INTO {archive_table} "
                    f"SELECT '{ds['name']}' AS asset_name, "
                    f"'table' AS asset_type, "
                    f"{meta_vals}, "
                    f"COUNT(*) AS row_count, "
                    f"to_json(collect_list(to_json(struct(*)))) AS snapshot_data "
                    f"FROM {qualified}"
                )

        # Definitions — models, functions, configs from snapshot files
        snapshot_files = collect_snapshot_files(forge_config)
        for sf in snapshot_files:
            src_path = Path(sf['src'])
            if not src_path.exists():
                continue
            content = src_path.read_text()
            dst = sf['dst']
            if dst.startswith('functions/'):
                a_type = 'function'
            elif dst.startswith('dbt_models/'):
                a_type = 'model'
            elif dst.endswith('.yml') and 'PROCESS_' in dst:
                a_type = 'workflow'
            else:
                a_type = 'config'
            safe_content = content.replace("'", "''")
            archive_stmts.append(
                f"INSERT INTO {archive_table} VALUES ("
                f"'{dst}', '{a_type}', "
                f"'{project_name}', '{env}', "
                f"current_timestamp(), "
                f"'{user}', '{git['sha']}', '{git['branch']}', '{wf_name}', "
                f"NULL, '{safe_content}')"
            )

    # ── Write manifest ────────────────────────────────
    manifest = {
        "backup": {
            "timestamp": timestamp,
            "project": project_name,
            "id": project_id,
            "environment": env,
            "scope": scope,
            "performed_by": user,
            "git_sha": git["sha"],
            "git_branch": git["branch"],
            "dab_name": wf_name,
            "files": files_copied,
            "data_archived": include_data,
            "archive_table": archive_table,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
    }
    (snap_path / "BACKUP_manifest.yml").write_text(
        yaml.dump(manifest, sort_keys=False, default_flow_style=False)
    )
    files_copied.append("BACKUP_manifest.yml")

    return BackupResult(
        timestamp=timestamp,
        snapshot_dir=snapshot_dir,
        files_copied=files_copied,
        archive_statements=archive_stmts,
        manifest=manifest,
    )


def list_backups(backup_dir: str = BACKUP_DIR) -> list[dict]:
    """
    List available backup snapshots.

    Returns list of dicts with timestamp, path, files, and metadata.
    Scans both resources/backups/ and legacy resources/teardown/_backup/.
    """
    all_snapshots: list[dict] = []

    for search_dir in [backup_dir, "resources/teardown/_backup"]:
        base = Path(search_dir)
        if not base.exists():
            continue
        for d in sorted(base.iterdir(), reverse=True):
            if d.is_dir() and len(d.name) == 15:  # YYYYMMDD_HHMMSS
                contents = [f.name for f in d.rglob("*") if f.is_file()]

                # Try new manifest first, then legacy teardown plan
                meta = {}
                manifest = d / "BACKUP_manifest.yml"
                plan_file = d / "TEARDOWN_plan.yml"
                if manifest.exists():
                    raw = yaml.safe_load(manifest.read_text()) or {}
                    meta = raw.get("backup", {})
                elif plan_file.exists():
                    raw = yaml.safe_load(plan_file.read_text()) or {}
                    meta = raw.get("teardown", {})

                all_snapshots.append({
                    "timestamp": d.name,
                    "path": str(d),
                    "files": contents,
                    "meta": meta,
                    "source": "backup" if manifest.exists() else "teardown",
                })

    # Deduplicate by timestamp (prefer backup over teardown)
    seen: dict[str, dict] = {}
    for s in all_snapshots:
        ts = s["timestamp"]
        if ts not in seen or s["source"] == "backup":
            seen[ts] = s
    return sorted(seen.values(), key=lambda s: s["timestamp"], reverse=True)


def restore_snapshot(
    snapshot_id: str,
    backup_dir: str = BACKUP_DIR,
) -> list[str]:
    """
    Restore project files from a backup or teardown snapshot.

    Searches both resources/backups/ and legacy resources/teardown/_backup/.
    Copies forge.yml, workflow YAML, UDF source files, and dbt models
    back to their original locations.

    Returns list of log messages.
    """
    # Search both backup locations
    snap_dir = None
    for search_dir in [backup_dir, "resources/teardown/_backup"]:
        candidate = Path(search_dir) / snapshot_id
        if candidate.exists():
            snap_dir = candidate
            break

    if snap_dir is None:
        return [f"Snapshot not found: {snapshot_id}"]

    log: list[str] = []

    # Restore forge.yml
    src = snap_dir / "forge.yml"
    if src.exists():
        shutil.copy2(src, "forge.yml")
        log.append("Restored forge.yml")

    # Restore workflow YAML
    for f in snap_dir.glob("PROCESS_*.yml"):
        dst = Path("resources/jobs") / f.name
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(f, dst)
        log.append(f"Restored {dst}")

    # Restore UDF source files
    funcs_dir = snap_dir / "functions"
    if funcs_dir.exists():
        dst_dir = Path("dbt/functions")
        dst_dir.mkdir(parents=True, exist_ok=True)
        for f in funcs_dir.iterdir():
            if f.is_file():
                shutil.copy2(f, dst_dir / f.name)
                log.append(f"Restored dbt/functions/{f.name}")

    # Restore dbt model source files
    models_dir = snap_dir / "dbt_models"
    if models_dir.exists():
        dst_dir = Path("dbt/models")
        dst_dir.mkdir(parents=True, exist_ok=True)
        for f in models_dir.rglob("*"):
            if f.is_file():
                rel = f.relative_to(models_dir)
                dst = dst_dir / rel
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(f, dst)
                log.append(f"Restored dbt/models/{rel}")

    if not log:
        log.append("No files to restore in this snapshot.")

    log.append("")
    log.append("Snapshot restored. Run 'forge deploy' to redeploy all assets.")

    return log


# =============================================
# ARCHIVE TABLE QUERIES (Databricks direct)
# =============================================

def _get_archive_table_name(forge_config: dict) -> str:
    """Resolve the fully-qualified _backup_archive table name from forge.yml."""
    profile = resolve_active_profile(forge_config)
    env = profile.get("env", forge_config.get("environment", "dev"))
    scope = forge_config.get("scope", "")
    catalog_pattern = forge_config.get("catalog_pattern", "{catalog}")
    schema_pattern = forge_config.get("schema_pattern", "{schema}")
    ops_catalog = _resolve_catalog(catalog_pattern, env, scope, "operations")
    ops_schema = _resolve_schema(schema_pattern, env, "backups", scope)
    return f"{ops_catalog}.{ops_schema}._backup_archive"


def _connect(forge_config: dict):
    """Get a databricks-sql-connector connection from forge.yml profile."""
    from forge.compute_resolver import resolve_profile, resolve_connection

    profile = resolve_profile(forge_config)
    conn_info = resolve_connection(profile)

    if not conn_info.is_databricks:
        raise ValueError(f"Archive queries require Databricks (current: {conn_info.platform})")
    if not conn_info.host or not conn_info.token:
        raise ValueError(
            "Missing host or token. Configure ~/.databrickscfg or set "
            "DBT_DATABRICKS_HOST / DBT_DATABRICKS_TOKEN."
        )

    try:
        from databricks import sql as dbsql
    except ImportError:
        raise ImportError(
            "Install databricks-sql-connector:\n"
            "  pip install databricks-sql-connector"
        )

    http_path = conn_info.http_path
    if not http_path:
        import os
        http_path = os.environ.get("DBT_DATABRICKS_HTTP_PATH", "")
    if not http_path:
        raise ValueError(
            "Missing http_path. Add it to ~/.databrickscfg or set "
            "DBT_DATABRICKS_HTTP_PATH."
        )

    return dbsql.connect(
        server_hostname=conn_info.host.replace("https://", ""),
        http_path=http_path,
        access_token=conn_info.token,
    )


def execute_archive(forge_config: dict, statements: list[str]) -> list[dict]:
    """Execute archive SQL statements via the Databricks SQL connector.

    Returns a list of dicts with keys: statement, success, error.
    """
    results: list[dict] = []
    conn = _connect(forge_config)
    try:
        cursor = conn.cursor()
        try:
            for stmt in statements:
                try:
                    cursor.execute(stmt)
                    results.append({"statement": stmt, "success": True, "error": None})
                except Exception as exc:
                    results.append({"statement": stmt, "success": False, "error": str(exc)})
        finally:
            cursor.close()
    finally:
        conn.close()
    return results


def query_archive(
    forge_config: dict,
    table_name: str | None = None,
) -> list[dict]:
    """
    Query the _backup_archive table on Databricks.

    Returns a list of dicts with: asset_name, asset_type, project,
    environment, archived_at, row_count, etc.

    If table_name is given, filters to that asset only.
    """
    archive_table = _get_archive_table_name(forge_config)

    sql = (
        f"SELECT asset_name, asset_type, project, environment, archived_at, "
        f"performed_by, git_sha, git_branch, dab_name, row_count "
        f"FROM {archive_table} "
    )
    if table_name:
        sql += f"WHERE asset_name = '{table_name}' "
    sql += "ORDER BY archived_at DESC, asset_type, asset_name"

    conn = _connect(forge_config)
    try:
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()
    finally:
        conn.close()


def restore_table_data(
    forge_config: dict,
    table_name: str,
    archived_at: str | None = None,
) -> list[str]:
    """
    Restore table data from the _backup_archive table.

    Reads the snapshot_data JSON array from the single archive row,
    explodes it back into individual rows, and INSERTs into the target
    table. Uses the most recent archive if archived_at is not specified.

    Returns list of log messages.
    """
    archive_table = _get_archive_table_name(forge_config)
    log: list[str] = []

    conn = _connect(forge_config)
    try:
        cursor = conn.cursor()
        try:
            # Find the archive row for this table
            meta_sql = (
                f"SELECT asset_name, project, environment, archived_at, row_count "
                f"FROM {archive_table} "
                f"WHERE asset_name = '{table_name}' AND asset_type = 'table' "
            )
            if archived_at:
                meta_sql += f"AND archived_at = '{archived_at}' "
            meta_sql += "ORDER BY archived_at DESC LIMIT 1"

            cursor.execute(meta_sql)
            meta_rows = cursor.fetchall()
            if not meta_rows:
                log.append(f"No archived data found for table: {table_name}")
                return log

            meta = meta_rows[0]
            ts = meta[3]  # archived_at
            row_count = meta[4] or 0
            log.append(f"Found archive for '{table_name}' from {ts}")
            log.append(f"  {row_count} rows to restore")

            # Resolve the target table's qualified name
            profile = resolve_active_profile(forge_config)
            env = profile.get("env", forge_config.get("environment", "dev"))
            scope = forge_config.get("scope", "")
            project_id = forge_config.get("id", forge_config.get("name", ""))
            catalog_pattern = forge_config.get("catalog_pattern", "{catalog}")
            schema_pattern = forge_config.get("schema_pattern", "{schema}")
            default_catalog = _resolve_catalog(catalog_pattern, env, scope,
                                                profile.get("catalog", "bronze"))
            default_schema = _resolve_schema(schema_pattern, env, project_id, scope)
            qualified_target = f"{default_catalog}.{default_schema}.{table_name}"

            # Get target table's columns to build the restore query
            col_sql = (
                f"SELECT column_name FROM {default_catalog}.information_schema.columns "
                f"WHERE table_schema = '{default_schema}' "
                f"AND table_name = '{table_name}' "
                f"ORDER BY ordinal_position"
            )
            cursor.execute(col_sql)
            columns = [row[0] for row in cursor.fetchall()]

            if not columns:
                log.append(f"  Target table {qualified_target} not found — skipping data restore")
                log.append(f"  Run 'forge deploy' first to recreate the table, then restore data")
                return log

            # Explode JSON array back into rows and INSERT
            col_extracts = ", ".join(
                f"get_json_object(row_json, '$.{col}')" for col in columns
            )
            restore_sql = (
                f"INSERT INTO {qualified_target} "
                f"SELECT {col_extracts} "
                f"FROM (SELECT explode(from_json(snapshot_data, "
                f"'ARRAY<STRING>')) AS row_json "
                f"FROM {archive_table} "
                f"WHERE asset_name = '{table_name}' AND asset_type = 'table' "
                f"AND archived_at = '{ts}')"
            )

            log.append(f"  Restoring into {qualified_target}...")
            cursor.execute(restore_sql)
            log.append(f"  Restored {row_count} rows into {qualified_target}")

        finally:
            cursor.close()
    finally:
        conn.close()

    return log
