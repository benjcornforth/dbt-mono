# =============================================
# src/forge/cli.py
# =============================================
# THE CODE IS THE DOCUMENTATION
# 
# This is the ONLY Python file the child ever sees.
# 
# Commands:
#   forge setup      ← creates everything in 2 seconds
#   forge deploy     ← builds graph + DAB + runs dbt
#   forge teardown   ← safely destroys
#   forge diff       ← shows graph changes
# 
# Everything else (quarantine, Python UDFs, compute resolver, portability)
# lives in the EXTERNAL dbt-dab-tools package (pulled via packages.yml).
# 
# A child types: forge setup → forge deploy → done.
# An engineer sees every line explained.

import os
import typer
import yaml
from pathlib import Path
import subprocess
from typing import Optional

from forge.graph import (
    build_graph,
    diff_graphs,
    render_mermaid,
    save_graph,
    load_graph,
    export_individual_contracts,
    walk_column_lineage,
    render_provenance_tree,
)
from forge.type_safe import build_models, generate_sdk_file
from forge.workflow import build_workflow, generate_bundle_config, run_bundle_command
from forge.teardown import (
    build_teardown_plan,
    build_backup,
    list_backups,
    restore_snapshot,
    query_archive,
    restore_table_data,
    execute_archive,
    BACKUP_DIR,
)
from forge.simple_ddl import (
    compile_all,
    compile_all_pure_sql,
    compile_all_udfs,
    apply_all_migrations,
    apply_all_migrations_dry_run,
    generate_agent_guide,
    compile_checks_sql,
    load_udfs,
)
from forge.compute_resolver import (
    resolve_profile,
    resolve_connection,
    resolve_model_schema,
    _expand_env_prefix,
    generate_profiles_yml,
    get_schema_variables,
    read_databrickscfg,
    list_databrickscfg_profiles,
)

app = typer.Typer(
    help="forge – dbt pipelines on Databricks, so simple a child could use it. "
         "One forge.yml. External dbt-dab-tools package. Graph diffs. "
         "Quarantine + provenance + Python UDFs built-in."
)

# =============================================
# CONFIG FILE – the ONLY thing a child ever edits
# =============================================
CONFIG_FILE = Path("forge.yml")

# DDL can live in a file (dbt/models.yml) or a directory (dbt/ddl/)
DDL_DEFAULT = "dbt/models.yml"


def _resolve_ddl(ddl: str) -> Path:
    """Resolve DDL path: dbt/ddl/ directory wins over dbt/models.yml file."""
    p = Path(ddl)
    # Auto-detect: prefer dbt/ddl/ directory if it exists
    ddl_dir = Path("dbt/ddl")
    if ddl_dir.is_dir():
        return ddl_dir
    # If user passed explicit path and it exists, use it
    if p.exists():
        return p
    return p  # fall back (caller checks .exists())


DEFAULT_CONFIG = {
    "name": "my_project",
    "active_profile": "dev",
    "profiles": {
        "dev": {
            "platform": "databricks",
            "databricks_profile": "DEFAULT",
            "catalog": "main",
            "schema": "dev_silver",
            "compute": {"type": "serverless", "auto_scale": True},
        },
        "prod": {
            "platform": "databricks",
            "databricks_profile": "PROD",
            "catalog": "main",
            "schema": "silver",
            "compute": {"type": "serverless"},
        },
    },
    "dbt": {"version": "1.8.0"},
    "features": {
        "graph": True,
        "quarantine": True,
        "validation_exceptions": True,
        "prior_version": True,
        "python_udfs": True
    },
    "portability": {
        "avoid_databricks_only": True,
        "postgres_compatible": True
    }
}

# =============================================
# SETUP – ludicrously simple
# =============================================
@app.command()
def setup(
    project_name: Optional[str] = typer.Option(None, "--name", help="Project name (defaults to folder name)")
):
    """forge setup → creates forge.yml + folders in 2 seconds"""
    typer.echo("🚀 Running forge setup... (child-level simple)")

    project_root = Path.cwd()
    if not CONFIG_FILE.exists():
        config = DEFAULT_CONFIG.copy()
        if project_name:
            config["name"] = project_name
        else:
            config["name"] = project_root.name

        CONFIG_FILE.write_text(yaml.dump(config, sort_keys=False, default_flow_style=False))
        typer.echo(f"✅ Created {CONFIG_FILE} – this is the ONLY file you ever edit!")

    # Create business-logic folders (dbt stays clean)
    (project_root / "dbt" / "models").mkdir(parents=True, exist_ok=True)
    (project_root / "dbt" / "seeds").mkdir(parents=True, exist_ok=True)
    (project_root / "dbt" / "sources").mkdir(parents=True, exist_ok=True)
    (project_root / "artifacts").mkdir(parents=True, exist_ok=True)

    typer.echo("✅ Created dbt/models/, seeds/, sources/ – pure business logic only")

    # Auto-generate profiles.yml from forge.yml
    config = yaml.safe_load(CONFIG_FILE.read_text())
    profiles_path = project_root / "profiles.yml"
    if not profiles_path.exists():
        generate_profiles_yml(config, output_path=profiles_path)
        typer.echo("✅ Generated profiles.yml from forge.yml profiles")

    typer.echo("🔗 All macros (quarantine, python_udf, prior_version) live in dbt-dab-tools (via packages.yml)")
    typer.echo("")
    typer.echo("💡 Databricks auth — choose one:")
    typer.echo("   a) databricks configure --profile DEFAULT   (recommended — zero env vars)")
    typer.echo("   b) export DBT_DATABRICKS_HOST=... DBT_DATABRICKS_TOKEN=...")
    typer.echo("")
    typer.echo("🎉 Setup complete! Now run:  forge deploy")

# =============================================
# DEPLOY – builds graph + DAB + runs dbt
# =============================================
@app.command()
def deploy(
    env: str = typer.Option("dev", "--env", help="Environment to deploy"),
    profile: Optional[str] = typer.Option(None, "--profile", "-p", help="Forge profile (from forge.yml profiles:)"),
):
    """forge deploy → reads forge.yml → auto-generates DAB → runs dbt"""
    typer.echo(f"🚀 Deploying to {env}...")

    if not CONFIG_FILE.exists():
        typer.echo("❌ No forge.yml found. Run 'forge setup' first!")
        raise typer.Exit(1)

    config = yaml.safe_load(CONFIG_FILE.read_text())
    prof = resolve_profile(config, profile_name=profile or env)
    conn = resolve_connection(prof)

    typer.echo(f"  📋 Profile: {prof['_name']} ({conn.platform})")
    typer.echo(f"  ✅ Resolved compute: {conn.compute_type}")
    if conn.host:
        typer.echo(f"  🔗 Host: {conn.host}")

    # Build env with resolved connection so dbt's {{ env_var(...) }} finds them
    dbt_env = os.environ.copy()
    if conn.host:
        dbt_env["DBT_DATABRICKS_HOST"] = conn.host
    if conn.token:
        dbt_env["DBT_DATABRICKS_TOKEN"] = conn.token
    if conn.http_path:
        dbt_env["DBT_DATABRICKS_HTTP_PATH"] = conn.http_path

    # Auto-regenerate profiles.yml so dbt uses current forge.yml settings
    generate_profiles_yml(config, output_path=Path("profiles.yml"))

    # Build dbt vars: catalog/schema variables + lineage provenance
    schema_vars = get_schema_variables(prof, config)
    dbt_vars = {
        **schema_vars,
        "git_commit": config.get("git_commit", "local"),
        "compute_type": conn.compute_type or "serverless",
    }
    dbt_vars_flag = ["--vars", yaml.dump(dbt_vars)]

    # Compile DDL → models + functions + sources
    ddl_path = _resolve_ddl(DDL_DEFAULT)
    if ddl_path.exists():
        compile_all(ddl_path, Path("dbt/models"), forge_config=config)
        typer.echo("  ✅ Compiled models, functions, sources")

    # Deploy UDFs first (functions must exist before models reference them)
    functions_dir = Path("dbt/functions")
    if functions_dir.is_dir():
        udf_files = sorted(functions_dir.glob("*.sql"))
        if udf_files:
            typer.echo("🔧 Deploying UDFs via dbt run-operation...")
            # Resolve Jinja placeholders — run_query receives raw SQL, not Jinja
            expanded_prof = _expand_env_prefix(prof, config)
            target_catalog = expanded_prof.get("catalog", "main")
            target_schema = expanded_prof.get("schema", "default")

            # Separate SQL UDFs from Python/Pandas UDFs — warehouses that don't
            # support LANGUAGE PYTHON would otherwise block all UDF deployment
            sql_files = [f for f in udf_files if "LANGUAGE PYTHON" not in f.read_text().upper()]
            py_files = [f for f in udf_files if "LANGUAGE PYTHON" in f.read_text().upper()]

            def _deploy_udf_batch(files: list[Path], label: str) -> bool:
                sql = "\n".join(f.read_text() for f in files)
                sql = sql.replace("{{ target.catalog }}", target_catalog)
                sql = sql.replace("{{ target.schema }}", target_schema)
                try:
                    subprocess.run(
                        ["dbt", "run-operation", "deploy_udfs",
                         "--args", yaml.dump({"udfs_sql": sql}),
                         "--project-dir", "."] + dbt_vars_flag,
                        check=True, capture_output=True, text=True, env=dbt_env,
                    )
                    typer.echo(f"  ✅ Deployed {label}: {', '.join(f.stem for f in files)}")
                    return True
                except subprocess.CalledProcessError as e:
                    output = (e.stdout or "") + (e.stderr or "")
                    typer.echo(f"  ❌ {label} deploy failed:")
                    for line in output.strip().splitlines()[-10:]:
                        typer.echo(f"     {line}")
                    return False
                except FileNotFoundError:
                    typer.echo("  ⚠️  UDF deploy skipped (dbt not found).")
                    return False

            ok = True
            if sql_files:
                ok = _deploy_udf_batch(sql_files, "SQL UDF(s)")
            if not ok:
                raise typer.Exit(1)
            if py_files:
                py_ok = _deploy_udf_batch(py_files, "Python UDF(s)")
                if not py_ok:
                    typer.echo("  💡 Python UDFs require a Pro or Serverless SQL warehouse.")
                    typer.echo("     Check your warehouse type, or deploy Python UDFs via a cluster.")

    # Run dbt seed (load CSV source data into tables)
    seed_dir = Path("dbt/seeds")
    if seed_dir.exists() and any(seed_dir.glob("*.csv")):
        typer.echo("🌱 Seeding source data...")
        try:
            subprocess.run(["dbt", "seed", "--project-dir", "."] + dbt_vars_flag, check=True, env=dbt_env)
        except subprocess.CalledProcessError as e:
            typer.echo(f"  ⚠️  Seed failed (exit {e.returncode}). Check seed CSV files.")
        except FileNotFoundError:
            pass

    # Run dbt (SQL-only models – zero cold starts on serverless)
    typer.echo("✅ Running dbt...")
    try:
        subprocess.run(["dbt", "run", "--project-dir", "."] + dbt_vars_flag, check=True, env=dbt_env)
    except FileNotFoundError:
        typer.echo("💡 Tip: install dbt-databricks with poetry if needed")

    # Auto-generate DAB workflow YAML (always up-to-date with latest graph)
    job_yaml_paths = []
    try:
        graph = build_graph(config)
        wf = build_workflow(config, graph)
        wf_path = Path(f"resources/jobs/{wf.name}.yml")
        wf_path.parent.mkdir(parents=True, exist_ok=True)
        wf_path.write_text(wf.to_databricks_yml())
        job_yaml_paths.append(str(wf_path))
        typer.echo(f"📋 DAB workflow → {wf_path}")
    except FileNotFoundError as exc:
        typer.echo(f"❌ {exc}")
        raise typer.Exit(code=1)
    except Exception as exc:
        typer.echo(f"  ⚠️  Workflow YAML skipped: {exc}")

    # Generate root databricks.yml and run bundle deploy
    if job_yaml_paths:
        try:
            bundle_yml = generate_bundle_config(config, job_yaml_paths)
            bundle_path = Path("databricks.yml")
            bundle_path.write_text(bundle_yml)
            typer.echo(f"📦 DAB bundle config → {bundle_path}")

            typer.echo("🚀 Running databricks bundle deploy...")
            result = run_bundle_command("deploy")
            if result["success"]:
                typer.echo("✅ Bundle deployed to Databricks.")
                if result["output"]:
                    for line in result["output"].strip().splitlines():
                        typer.echo(f"  {line}")
            else:
                typer.echo(f"  ⚠️  Bundle deploy failed: {result['error']}")
                typer.echo("  You can run manually: databricks bundle deploy")
        except Exception as exc:
            typer.echo(f"  ⚠️  Bundle deploy skipped: {exc}")

    typer.echo("🎉 Deploy complete! Run 'forge diff' to see graph changes.")

# =============================================
# BACKUP – standalone snapshot (no destruction)
# =============================================
@app.command()
def backup(
    data: bool = typer.Option(False, "--data", help="Also archive table data to Databricks"),
    list_all: bool = typer.Option(False, "--list", "-l", help="List existing backups"),
    profile: Optional[str] = typer.Option(None, "--profile", "-p", help="Profile to use"),
):
    """forge backup → snapshot all project assets (routine, non-destructive)"""
    if not CONFIG_FILE.exists():
        typer.echo("❌ No forge.yml found.")
        raise typer.Exit(1)

    config = yaml.safe_load(CONFIG_FILE.read_text())
    if profile:
        prof = resolve_profile(config, profile_name=profile)
        config["environment"] = prof.get("env", config.get("environment", "dev"))

    if list_all:
        snaps = list_backups()
        if not snaps:
            typer.echo("No backups found.")
            return
        typer.echo(f"📸 Available backups ({len(snaps)}):")
        typer.echo("")
        for s in snaps:
            source_tag = f"  [{s['source']}]" if s.get("source") == "teardown" else ""
            typer.echo(f"  {s['timestamp']}  ({len(s['files'])} files){source_tag}")
            meta = s.get("meta", {})
            if meta.get("environment"):
                typer.echo(f"    env: {meta['environment']}  project: {meta.get('project', meta.get('id', '?'))}")
            if meta.get("data_archived"):
                typer.echo(f"    data archived to: {meta.get('archive_table', '?')}")
        typer.echo("")
        typer.echo("To restore: forge restore --snapshot <timestamp>")
        return

    graph = build_graph(config)
    result = build_backup(config, graph, include_data=data)

    typer.echo(f"📸 Backup created: {result.snapshot_dir}/")
    typer.echo(f"   {len(result.files_copied)} files snapshotted")
    for f in result.files_copied:
        typer.echo(f"   ✅ {f}")

    if result.archive_statements:
        typer.echo("")
        typer.echo("📦 Archiving table data to Databricks...")
        try:
            results = execute_archive(config, result.archive_statements)
            for r in results:
                stmt = r["statement"]
                typer.echo(f"  🔧 {stmt[:80]}..." if len(stmt) > 80 else f"  🔧 {stmt}")
                if r["success"]:
                    typer.echo(f"  ✅ Done")
                else:
                    typer.echo(f"  ⚠️  Failed: {r['error']}")
                    typer.echo(f"     Execute manually: {stmt}")
        except Exception as exc:
            typer.echo(f"  ⚠️  Could not connect to Databricks: {exc}")
            typer.echo("  Statements to execute manually:")
            for stmt in result.archive_statements:
                typer.echo(f"     {stmt}")

    typer.echo("")
    typer.echo(f"♻️  To restore: forge restore --snapshot {result.timestamp}")

# =============================================
# TEARDOWN – safe destroy (default preserves data, --wipe-all drops tables)
# =============================================
@app.command()
def teardown(
    execute: bool = typer.Option(False, "--execute", help="Actually run teardown (default is dry-run)"),
    wipe_all: bool = typer.Option(False, "--wipe-all", help="DROP all tables — permanently deletes data (requires --execute)"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Write teardown plan to file"),
    profile: Optional[str] = typer.Option(None, "--profile", "-p", help="Profile to use"),
):
    """forge teardown → safe teardown plan (dry-run by default, --execute to run)"""
    if not CONFIG_FILE.exists():
        typer.echo("❌ No forge.yml found.")
        raise typer.Exit(1)

    if wipe_all and not execute:
        typer.echo("❌ --wipe-all requires --execute. This is a destructive operation.")
        typer.echo("   Usage: forge teardown --execute --wipe-all")
        raise typer.Exit(1)

    config = yaml.safe_load(CONFIG_FILE.read_text())
    if profile:
        prof = resolve_profile(config, profile_name=profile)
        config["environment"] = prof.get("env", config.get("environment", "dev"))

    graph = build_graph(config)
    plan = build_teardown_plan(config, graph, wipe_all=wipe_all)

    # Always write the plan YAML for audit
    plan_path = Path(output or f"resources/teardown/{plan.name}.yml")
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.write_text(plan.to_yaml())
    typer.echo(f"📋 Teardown plan → {plan_path}")

    # Show summary
    typer.echo("")
    typer.echo(plan.to_summary())
    typer.echo("")

    if not execute:
        hint = "forge teardown --execute"
        if wipe_all:
            hint += " --wipe-all"
        typer.echo(f"ℹ️  This was a dry run. To execute: {hint}")
        return

    # Confirm destructive --wipe-all before proceeding
    if wipe_all:
        dropped = [a.name for a in plan.removes if a.asset_type == "table"]
        typer.echo(f"🚨 WARNING: This will permanently DROP {len(dropped)} table(s):")
        for t in dropped:
            typer.echo(f"   • {t}")
        typer.echo("")
        confirm = typer.prompt("Type the project id to confirm", default="")
        if confirm != config.get("id", config.get("name", "")):
            typer.echo("❌ Aborted. Project id did not match.")
            raise typer.Exit(1)

    # Execute with confirmation
    typer.echo("⚠️  Executing teardown...")
    log = plan.execute(dry_run=False)
    for line in log:
        typer.echo(f"  {line}")

    # Run UDF SQL via dbt if there are SQL steps
    sql_steps = [s for s in plan.steps if s.action == "sql"]
    all_sql_stmts = [stmt for step in sql_steps for stmt in step.statements]
    if all_sql_stmts:
        try:
            results = execute_archive(config, all_sql_stmts)
            for r in results:
                stmt = r["statement"]
                typer.echo(f"  🔧 Running: {stmt}")
                if r["success"]:
                    typer.echo(f"  ✅ Done")
                else:
                    typer.echo(f"  ⚠️  Failed: {r['error']}")
                    typer.echo(f"     Execute manually: {stmt}")
        except Exception as exc:
            typer.echo(f"  ⚠️  Could not connect to Databricks: {exc}")
            typer.echo("  Statements to execute manually:")
            for stmt in all_sql_stmts:
                typer.echo(f"     {stmt}")

    # Destroy DAB bundle (removes workflows from Databricks)
    if Path("databricks.yml").exists():
        typer.echo("🗑️  Running databricks bundle destroy...")
        result = run_bundle_command("destroy")
        if result["success"]:
            typer.echo("✅ Bundle destroyed on Databricks.")
        else:
            typer.echo(f"  ⚠️  Bundle destroy failed: {result['error']}")
            typer.echo("  You can run manually: databricks bundle destroy")

    typer.echo("")
    typer.echo("🎉 Teardown complete.")
    typer.echo(f"♻️  To re-instate everything: forge deploy")
    if plan.snapshot_dir:
        ts = Path(plan.snapshot_dir).name
        typer.echo(f"♻️  To restore from snapshot: forge restore --snapshot {ts}")

# =============================================
# RESTORE – bring back from teardown snapshot
# =============================================
@app.command()
def restore(
    snapshot: Optional[str] = typer.Option(None, "--snapshot", "-s", help="Snapshot timestamp to restore (e.g. 20250101_120000)"),
    data: bool = typer.Option(False, "--data", help="Query the archive table on Databricks for archived data"),
    table: Optional[str] = typer.Option(None, "--table", "-t", help="Restore data for a specific table from the archive"),
    profile: Optional[str] = typer.Option(None, "--profile", "-p", help="Profile to use for Databricks connection"),
):
    """forge restore → list snapshots, query archive table, or restore from snapshot"""

    # ── Data mode: poll the archive table on Databricks ──
    if data or table:
        if not CONFIG_FILE.exists():
            typer.echo("❌ No forge.yml found.")
            raise typer.Exit(1)

        config = yaml.safe_load(CONFIG_FILE.read_text())
        if profile:
            prof = resolve_profile(config, profile_name=profile)
            config["environment"] = prof.get("env", config.get("environment", "dev"))

        if table:
            # Restore data for a specific table
            typer.echo(f"♻️  Restoring table data: {table}")
            try:
                log = restore_table_data(config, table)
                for line in log:
                    typer.echo(f"  {line}")
            except Exception as exc:
                typer.echo(f"❌ {exc}")
                raise typer.Exit(1)
        else:
            # List what's in the archive table
            typer.echo("📦 Querying archive table on Databricks...")
            try:
                rows = query_archive(config)
                if not rows:
                    typer.echo("  No archived data found.")
                    return

                typer.echo(f"  {len(rows)} archive entries found:")
                typer.echo("")
                for r in rows:
                    git_tag = ""
                    if r.get("git_sha"):
                        git_tag = f"  {r['git_branch']}@{r['git_sha']}"
                    row_info = f"{r['row_count']:>8,} rows" if r.get("row_count") else "        —   "
                    typer.echo(
                        f"  [{r['asset_type']:8s}]  {r['asset_name']:30s}  {row_info}  "
                        f"{r['archived_at']}  ({r['environment']})")
                    typer.echo(
                        f"             by: {r.get('performed_by', '?'):12s}  "
                        f"dab: {r.get('dab_name', '?')}{git_tag}")
                typer.echo("")
                typer.echo("To restore a table: forge restore --table <name>")
            except Exception as exc:
                typer.echo(f"❌ Could not query archive: {exc}")
                raise typer.Exit(1)
        return

    # ── Snapshot mode: local file snapshots ──
    if snapshot is None:
        # List available snapshots
        snaps = list_backups()
        if not snaps:
            typer.echo("No backups found.")
            typer.echo("Tip: use 'forge backup' to create one, or 'forge restore --data' to query Databricks")
            return

        typer.echo(f"📸 Available snapshots ({len(snaps)}):")
        typer.echo("")
        for s in snaps:
            source_tag = f"  [{s['source']}]" if s.get("source") == "teardown" else ""
            typer.echo(f"  {s['timestamp']}  ({len(s['files'])} files){source_tag}")
            meta = s.get("meta", {})
            if meta.get("environment"):
                typer.echo(f"    env: {meta['environment']}  project: {meta.get('project', meta.get('id', '?'))}")
        typer.echo("")
        typer.echo("To restore files:  forge restore --snapshot <timestamp>")
        typer.echo("To query archive:  forge restore --data")
        return

    # Restore from snapshot
    typer.echo(f"♻️  Restoring from snapshot: {snapshot}")
    log = restore_snapshot(snapshot)
    for line in log:
        typer.echo(f"  {line}")

# =============================================
# DIFF – graph magic
# =============================================
@app.command()
def diff(
    mermaid: bool = typer.Option(False, "--mermaid", help="Output Mermaid diff diagram"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Write diff output to file"),
):
    """forge diff → shows what changed since last graph snapshot"""
    if not CONFIG_FILE.exists():
        typer.echo("❌ No forge.yml found. Run 'forge setup' first!")
        raise typer.Exit(1)

    config = yaml.safe_load(CONFIG_FILE.read_text())
    snapshot_path = Path("artifacts") / "graph.json"

    new_graph = build_graph(config)
    old_graph = load_graph(snapshot_path)

    if old_graph is None:
        typer.echo("📊 No previous graph snapshot found — building baseline.")
        save_graph(new_graph, snapshot_path)
        typer.echo(f"✅ Saved graph snapshot → {snapshot_path}")
        typer.echo("   Run 'forge diff' again after making changes.")
        return

    result = diff_graphs(old_graph, new_graph)

    if mermaid:
        diagram = render_mermaid(new_graph, diff=result)
        if output:
            Path(output).parent.mkdir(parents=True, exist_ok=True)
            Path(output).write_text(diagram)
            typer.echo(f"✅ Mermaid diff diagram → {output}")
        else:
            typer.echo(diagram)
    else:
        typer.echo(f"📊 Diff: {result['old_commit']} → {result['new_commit']}\n")
        typer.echo(result["summary"])

        if output:
            import json
            Path(output).parent.mkdir(parents=True, exist_ok=True)
            Path(output).write_text(json.dumps(result, indent=2, default=str))
            typer.echo(f"\n✅ Full diff JSON → {output}")

    # Save new snapshot for next diff
    save_graph(new_graph, snapshot_path)
    typer.echo(f"\n📸 Updated graph snapshot → {snapshot_path}")


# =============================================
# EXPLAIN – column-level provenance
# =============================================
@app.command()
def explain(
    model_dot_column: str = typer.Argument(..., help="Model.column to explain (e.g. customer_summary.total_revenue)"),
    ddl: str = typer.Option(DDL_DEFAULT, "--ddl", help="Path to DDL file or directory"),
    mermaid_flag: bool = typer.Option(False, "--mermaid", help="Output Mermaid provenance diagram"),
    full: bool = typer.Option(False, "--full", help="Show full detail including upstream checks"),
    light: bool = typer.Option(False, "--light", help="Lightweight view: expressions + columns only"),
    version_filter: Optional[str] = typer.Option(None, "--version", help="Filter to specific methodology version (e.g. v2)"),
    origin_flag: bool = typer.Option(False, "--origin", help="Show data origin / source provenance"),
    json_flag: bool = typer.Option(False, "--json", help="Output JSON for scripting"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Write output to file"),
):
    """forge explain <model>.<column> → shows full provenance tree"""
    parts = model_dot_column.split(".", 1)
    if len(parts) != 2:
        typer.echo("❌ Usage: forge explain <model_name>.<column_name>")
        typer.echo("   Example: forge explain customer_summary.total_revenue")
        raise typer.Exit(1)

    model_name, column_name = parts
    ddl_path = _resolve_ddl(ddl)
    dbt_dir = ddl_path.parent if ddl_path.is_file() else ddl_path.parent

    if not ddl_path.exists():
        typer.echo(f"❌ {ddl} not found. Create dbt/models.yml or dbt/ddl/ directory.")
        raise typer.Exit(1)

    if not CONFIG_FILE.exists():
        typer.echo("\u274c No forge.yml found. Run 'forge setup' first!")
        raise typer.Exit(1)

    config = yaml.safe_load(CONFIG_FILE.read_text())
    graph = build_graph(config, dbt_project_dir=dbt_dir)
    tree = walk_column_lineage(graph, model_name, column_name, dbt_dir)

    if "error" in tree:
        typer.echo(f"❌ {tree['error']}")
        raise typer.Exit(1)

    # Apply version filter — prune nodes that don't match
    if version_filter:
        tree = _filter_tree_by_version(tree, version_filter)

    if json_flag:
        import json
        result = json.dumps(tree, indent=2, default=str)
    elif mermaid_flag:
        result = render_provenance_tree(tree, mermaid=True)
    else:
        result = render_provenance_tree(tree, full=full, light=light, show_origin=origin_flag)

    if output:
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        Path(output).write_text(result)
        typer.echo(f"✅ Written to {output}")
    else:
        typer.echo(result)


def _filter_tree_by_version(tree: dict, version: str) -> dict:
    """Recursively prune nodes that don't match the given version."""
    if "error" in tree:
        return tree
    filtered = dict(tree)
    filtered["upstream"] = [
        _filter_tree_by_version(child, version)
        for child in tree.get("upstream", [])
        if child.get("version") == version or child.get("upstream")
    ]
    return filtered

# =============================================
# CODEGEN – type-safe Python SDK from dbt schema
# =============================================
@app.command()
def codegen(
    output: str = typer.Option("sdk/models.py", "--output", "-o", help="Output file path"),
    check: bool = typer.Option(False, "--check", help="CI mode: fail if generated file is stale"),
):
    """forge codegen → generates type-safe Pydantic models from dbt schema"""
    if not CONFIG_FILE.exists():
        typer.echo("❌ No forge.yml found. Run 'forge setup' first!")
        raise typer.Exit(1)

    config = yaml.safe_load(CONFIG_FILE.read_text())
    schema = config.get("schema", "default")
    output_path = Path(output)

    # Read existing content for --check comparison
    old_content = output_path.read_text() if output_path.exists() else None

    out = generate_sdk_file(schema=schema, output_path=output_path)
    new_content = out.read_text()

    if check:
        if old_content is None:
            typer.echo(f"❌ {output} does not exist. Run 'forge codegen' to create it.")
            raise typer.Exit(1)
        if old_content != new_content:
            # Restore old content since this is check-only
            out.write_text(old_content)
            typer.echo(f"❌ {output} is stale. Run 'forge codegen' to update.")
            raise typer.Exit(1)
        typer.echo(f"✅ {output} is up to date.")
    else:
        typer.echo(f"✅ Generated {out} with type-safe models for schema '{schema}'")
        typer.echo("   Import with: from sdk.models import StgOrders, CustomerClean, ...")
        typer.echo("   Wrong types → Pydantic raises immediately. No bad inserts.")

# =============================================
# WORKFLOW – DAG orchestration
# =============================================
@app.command()
def workflow(
    mermaid: bool = typer.Option(False, "--mermaid", help="Output Mermaid diagram"),
    dab: bool = typer.Option(False, "--dab", help="Output databricks.yml jobs section"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Write output to file (default: resources/jobs/<name>.yml for --dab)"),
    profile: Optional[str] = typer.Option(None, "--profile", "-p", help="Forge profile (from forge.yml profiles:)"),
):
    """forge workflow → generates stage-based Databricks Workflow DAG"""
    if not CONFIG_FILE.exists():
        typer.echo("❌ No forge.yml found. Run 'forge setup' first!")
        raise typer.Exit(1)

    config = yaml.safe_load(CONFIG_FILE.read_text())
    if profile:
        prof = resolve_profile(config, profile_name=profile)
        # Merge profile values into config for workflow builder
        config["catalog"] = prof.get("catalog", config.get("catalog", "main"))
        config["schema"] = prof.get("schema", config.get("schema", "silver"))
        config["compute"] = prof.get("compute", config.get("compute", {}))
        config["environment"] = prof["_name"]

    graph = build_graph(config)
    wf = build_workflow(config, graph)

    if mermaid:
        result = wf.to_mermaid()
    elif dab:
        result = wf.to_databricks_yml()
    else:
        # Default: print task summary
        result = yaml.dump(wf.to_dict(), sort_keys=False, default_flow_style=False)

    # Default output path for --dab mode
    if dab and not output:
        output = f"resources/jobs/{wf.name}.yml"

    if output:
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        Path(output).write_text(result)
        typer.echo(f"✅ Workflow written to {output}")
    else:
        typer.echo(result)

    stages_used = [t.stage for t in wf.tasks]
    stage_summary = " → ".join(dict.fromkeys(stages_used))  # unique, ordered
    typer.echo(f"\n📊 Pipeline: {stage_summary} ({len(wf.tasks)} tasks)")
    for task in wf.tasks:
        deps = f" ← {', '.join(task.depends_on)}" if task.depends_on else ""
        typer.echo(f"  [{task.stage}] {task.name}{deps}")


# =============================================
# PYTHON-TASK – scaffold + manage Python tasks
# =============================================
@app.command("python-task")
def python_task(
    name: str = typer.Argument(..., help="Name for the Python task (e.g. enrich_customers)"),
    stage: str = typer.Option("enrich", "--stage", "-s", help="Pipeline stage: ingest/stage/clean/enrich/serve"),
    output: str = typer.Option("python", "--output", "-o", help="Output directory for task file"),
    description: str = typer.Option("", "--desc", help="Short description of the task"),
):
    """forge python-task <name> → scaffolds a Python task file + registers in forge.yml"""
    from forge.python_task import scaffold_python_task

    out = scaffold_python_task(
        name=name,
        output_dir=Path(output),
        stage=stage,
        description=description,
    )
    typer.echo(f"✅ Created {out}")

    # Auto-register in forge.yml if not already present
    if CONFIG_FILE.exists():
        config = yaml.safe_load(CONFIG_FILE.read_text())
        py_tasks = config.get("python_tasks", [])
        existing_names = {t["name"] for t in py_tasks}
        if name not in existing_names:
            # Append to forge.yml without rewriting (preserves comments)
            entry_yml = yaml.dump(
                [{"name": name, "file": str(out), "stage": stage}],
                sort_keys=False, default_flow_style=False,
            )
            raw = CONFIG_FILE.read_text()
            if "python_tasks:" not in raw:
                raw += "\n# =============================================\n"
                raw += "# PYTHON TASKS — included in forge workflow\n"
                raw += "# =============================================\n"
                raw += "python_tasks:\n"
            # Indent entries under python_tasks:
            for line in entry_yml.strip().splitlines():
                raw += f"  {line}\n"
            CONFIG_FILE.write_text(raw)
            typer.echo(f"📝 Registered '{name}' in forge.yml python_tasks")
    else:
        typer.echo("⚠️  No forge.yml found — add python_tasks entry manually")

    typer.echo(f"   Edit {out} then run: forge workflow")


# =============================================
# COMPILE – models.yml → SQL (no coding needed)
# =============================================
@app.command()
def compile(
    ddl: str = typer.Option(DDL_DEFAULT, "--ddl", help="Path to DDL file or directory"),
    output: str = typer.Option("dbt/models", "--output", "-o", help="Output directory for SQL files"),
    pure_sql: bool = typer.Option(False, "--pure-sql", help="Emit standalone SQL (no dbt/Jinja). Runs directly on SQL warehouse."),
    profile: Optional[str] = typer.Option(None, "--profile", "-p", help="Forge profile (from forge.yml profiles:)"),
):
    """forge compile → turns DDL into SQL + schema.yml (no SQL needed)"""
    ddl_path = _resolve_ddl(ddl)
    if not ddl_path.exists():
        typer.echo(f"❌ {ddl} not found. Create dbt/models.yml or dbt/ddl/ directory.")
        raise typer.Exit(1)

    if pure_sql:
        # Pure-SQL mode: no Jinja, no dbt, runs on any SQL warehouse
        if not CONFIG_FILE.exists():
            typer.echo("❌ No forge.yml found. Run 'forge setup' first!")
            raise typer.Exit(1)
        config = yaml.safe_load(CONFIG_FILE.read_text())
        prof = resolve_profile(config, profile_name=profile)
        catalog = prof.get("catalog", "main")
        schema = prof.get("schema", "silver")
        compute_type = prof.get("compute", {}).get("type", "serverless") if isinstance(prof.get("compute"), dict) else "serverless"
        platform = prof.get("platform", "databricks")

        sql_dir = Path("sql")
        results = compile_all_pure_sql(
            ddl_path, sql_dir,
            catalog=catalog, schema=schema,
            compute_type=compute_type, platform=platform,
            profile=prof, forge_config=config,
        )

        for name, path in results.items():
            if name == "_udfs":
                typer.echo(f"  🔧 {path.name} → UDF definitions")
            elif name.endswith("_quarantine"):
                typer.echo(f"  🔶 {path.name} → quarantine sidecar")
            else:
                typer.echo(f"  ✅ {path.name}")

        model_count = len([k for k in results if not k.startswith("_") and not k.endswith("_quarantine")])
        typer.echo(f"")
        typer.echo(f"🎉 Compiled {model_count} models → sql/ (pure SQL, no dbt needed)")
        typer.echo(f"   Run files in order on any SQL warehouse.")
        return

    # Standard dbt mode
    output_dir = Path(output)
    config = yaml.safe_load(CONFIG_FILE.read_text()) if CONFIG_FILE.exists() else None
    results = compile_all(ddl_path, output_dir, forge_config=config)

    for name, path in results.items():
        if name == "_schema":
            typer.echo(f"  📋 schema.yml → {path}")
        elif name == "_sources":
            typer.echo(f"  📦 _sources.yml → {path}")
        elif name.startswith("_udf_"):
            typer.echo(f"  🔧 {path.name} → {path}")
        else:
            typer.echo(f"  ✅ {name}.sql → {path}")

    model_count = len([k for k in results if not k.startswith("_")])
    udf_count = len([k for k in results if k.startswith("_udf_")])
    extras = []
    if udf_count:
        extras.append(f"{udf_count} UDFs → dbt/functions/")
    if "_sources" in results:
        extras.append("sources → dbt/sources/")
    extra_note = f" + {', '.join(extras)}" if extras else ""
    typer.echo(f"🎉 Compiled {model_count} models{extra_note} from {ddl_path}. Run 'forge deploy' next.")


# =============================================
# MIGRATE – apply migration YAMLs
# =============================================
@app.command()
def migrate(
    ddl: str = typer.Option(DDL_DEFAULT, "--ddl", help="Path to DDL file or directory"),
    migrations: str = typer.Option("dbt/migrations", "--migrations", "-m", help="Migrations directory"),
    recompile: bool = typer.Option(True, help="Recompile SQL after migrating"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview changes without applying"),
):
    """forge migrate → applies migration YAMLs to models.yml + recompiles"""
    ddl_path = _resolve_ddl(ddl)
    mig_dir = Path(migrations)

    if not ddl_path.exists():
        typer.echo(f"❌ {ddl} not found. Create dbt/models.yml or dbt/ddl/ directory.")
        raise typer.Exit(1)

    if not mig_dir.exists():
        typer.echo(f"📁 No migrations directory at {migrations}. Nothing to apply.")
        raise typer.Exit(0)

    if dry_run:
        results = apply_all_migrations_dry_run(ddl_path, mig_dir)
        if not results:
            typer.echo("✅ All migrations already applied. Nothing to do.")
            return
        typer.echo("🔍 DRY RUN — no files will be modified:\n")
        for r in results:
            typer.echo(f"  📦 {r['migration']}: {r['description']}")
            for model, changes in r["changes"].items():
                for c in changes:
                    typer.echo(f"    → {model}: {c}")
        typer.echo(f"\n📋 {len(results)} migration(s) would be applied. Remove --dry-run to apply.")
        return

    results = apply_all_migrations(ddl_path, mig_dir)

    if not results:
        typer.echo("✅ All migrations already applied. Nothing to do.")
        return

    for r in results:
        typer.echo(f"  📦 {r['migration']}: {r['description']}")
        for model, changes in r["changes"].items():
            for c in changes:
                typer.echo(f"    → {model}: {c}")

    if recompile:
        output_dir = ddl_path.parent / "models" if ddl_path.is_file() else ddl_path.parent / "models"
        config = yaml.safe_load(CONFIG_FILE.read_text()) if CONFIG_FILE.exists() else None
        compile_all(ddl_path, output_dir, forge_config=config)
        typer.echo(f"🔄 Recompiled SQL from updated {ddl}")

    typer.echo(f"🎉 Applied {len(results)} migration(s). Run 'forge deploy' next.")


# =============================================
# GUIDE – auto-generated project documentation
# =============================================
@app.command()
def guide(
    output: str = typer.Option(".instructions.md", "--output", "-o", help="Output file path"),
    ddl: str = typer.Option(DDL_DEFAULT, "--ddl", help="Path to DDL file or directory"),
):
    """forge guide → generates a project-specific agent guide / onboarding doc"""
    if not CONFIG_FILE.exists():
        typer.echo("❌ No forge.yml found. Run 'forge setup' first!")
        raise typer.Exit(1)

    config = yaml.safe_load(CONFIG_FILE.read_text())
    ddl_path = _resolve_ddl(ddl) if _resolve_ddl(ddl).exists() else None
    output_path = Path(output)

    generate_agent_guide(config, ddl_path=ddl_path, output_path=output_path)
    typer.echo(f"✅ Generated project guide → {output_path}")
    typer.echo("   Share with new team members or AI agents for instant onboarding.")


# =============================================
# UDFS – manage SQL/Python UDFs from models.yml
# =============================================
@app.command()
def udfs(
    ddl: str = typer.Option(DDL_DEFAULT, "--ddl", help="Path to DDL file or directory"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Write UDF SQL to file"),
):
    """forge udfs → shows and compiles UDFs defined in DDL"""
    ddl_path = _resolve_ddl(ddl)
    if not ddl_path.exists():
        typer.echo(f"❌ {ddl} not found.")
        raise typer.Exit(1)

    udf_defs = load_udfs(ddl_path)
    if not udf_defs:
        typer.echo("📋 No UDFs defined. Add a udfs: block to your DDL to get started.")
        return

    compiled = compile_all_udfs(ddl_path)

    typer.echo(f"\n🔧 {len(compiled)} UDF(s) defined:\n")
    for udf_name in compiled:
        udf_def = udf_defs[udf_name]
        lang = udf_def.get("language", "sql").upper()
        returns = udf_def.get("returns", "STRING")
        params = udf_def.get("params", [])
        param_names = []
        for p in params:
            if isinstance(p, dict):
                param_names.append(f"{p['name']}: {p['type']}")
            elif isinstance(p, str):
                param_names.append(p)
        param_str = ", ".join(param_names) if param_names else "(none)"
        if lang == "PANDAS":
            icon = "🟠"
            display_lang = "PANDAS"
        elif lang == "PYTHON":
            icon = "🟣"
            display_lang = "PYTHON"
        else:
            icon = "🔵"
            display_lang = lang
        typer.echo(f"  {icon} {udf_name}({param_str}) → {returns}  [{display_lang}]")

    if output:
        out_lines = []
        for udf_name, udf_sql in compiled.items():
            out_lines.append(udf_sql)
            out_lines.append("")
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        Path(output).write_text("\n".join(out_lines))
        typer.echo(f"\n✅ UDF SQL written to {output}")

    typer.echo(f"\n💡 Use in columns: {{ udf: \"loyalty_tier(total_revenue)\" }}")


# =============================================
# VALIDATE – run checks defined in models.yml
# =============================================
@app.command()
def validate(
    ddl: str = typer.Option(DDL_DEFAULT, "--ddl", help="Path to DDL file or directory"),
    model: Optional[str] = typer.Option(None, "--model", "-m", help="Validate one model only"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Write check SQL to file"),
):
    """forge validate → shows checks defined for each model"""
    ddl_path = _resolve_ddl(ddl)
    if not ddl_path.exists():
        typer.echo(f"❌ {ddl} not found.")
        raise typer.Exit(1)

    results = compile_checks_sql(ddl_path, model_filter=model)

    if not results:
        typer.echo("📋 No checks defined. Add a checks: block to your DDL to get started.")
        return

    total_checks = 0
    for model_name, checks in results.items():
        typer.echo(f"\n  📊 {model_name}: {len(checks)} check(s)")
        for chk in checks:
            icon = "🔴" if chk['severity'] == 'error' else "🟡" if chk['severity'] == 'warn' else "⚪"
            typer.echo(f"    {icon} {chk['name']} [{chk['scope']}] → {chk['severity']}")
            total_checks += 1

    if output:
        # Write the compiled check SQL to a file for inspection
        out_lines = []
        for model_name, checks in results.items():
            out_lines.append(f"-- ═══ Checks for {model_name} ═══")
            for chk in checks:
                out_lines.append(f"-- {chk['name']} [{chk['scope']}] severity={chk['severity']}")
                out_lines.append(chk['sql'] + ";")
                out_lines.append("")
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        Path(output).write_text("\n".join(out_lines))
        typer.echo(f"\n✅ Check SQL written to {output}")

    typer.echo(f"\n🎯 {total_checks} total check(s) across {len(results)} model(s).")


# =============================================
# DEV – isolated development environment
# =============================================
@app.command(name="dev-up")
def dev_up(
    schema_suffix: Optional[str] = typer.Option(None, "--schema", "-s", help="Dev schema suffix (default: username)"),
    seed: bool = typer.Option(True, help="Seed sample data after setup"),
):
    """forge dev-up → creates isolated dev schema + seeds sample data"""
    if not CONFIG_FILE.exists():
        typer.echo("❌ No forge.yml found. Run 'forge setup' first!")
        raise typer.Exit(1)

    config = yaml.safe_load(CONFIG_FILE.read_text())
    prof = resolve_profile(config, profile_name="dev")
    base_schema = prof.get("schema", "silver")

    import getpass
    suffix = schema_suffix or getpass.getuser()
    dev_schema = f"dev_{base_schema}_{suffix}"

    typer.echo(f"🚀 Setting up dev environment...")
    typer.echo(f"  📦 Dev schema: {dev_schema}")

    # Compile models first
    ddl_path = _resolve_ddl(DDL_DEFAULT)
    if ddl_path.exists():
        compile_all(ddl_path, Path("dbt/models"), forge_config=config)
        typer.echo("  ✅ Compiled models")

    # Run dbt with schema override
    dbt_cmd = [
        "dbt", "run",
        "--project-dir", ".",
        "--target", "dev",
        "--vars", yaml.dump({"dev_schema": dev_schema}),
    ]

    if seed:
        typer.echo("  🌱 Seeding sample data...")
        try:
            subprocess.run(
                ["dbt", "seed", "--project-dir", ".", "--target", "dev"],
                check=True, capture_output=True,
            )
            typer.echo("  ✅ Seeds loaded")
        except (FileNotFoundError, subprocess.CalledProcessError) as e:
            typer.echo(f"  ⚠️  Seed failed (non-fatal): {e}")

    typer.echo(f"🎉 Dev environment ready! Schema: {dev_schema}")
    typer.echo(f"   Run 'forge dev' to start watch mode.")
    typer.echo(f"   Run 'forge dev-down' to tear down.")


@app.command(name="dev")
def dev(
    ddl: str = typer.Option(DDL_DEFAULT, "--ddl", help="Path to DDL file or directory"),
    output: str = typer.Option("dbt/models", "--output", "-o", help="Output directory for SQL files"),
    poll_interval: float = typer.Option(1.0, "--poll", help="Seconds between file change polls"),
):
    """forge dev → watches DDL files, auto-compiles on save"""
    ddl_path = _resolve_ddl(ddl)
    if not ddl_path.exists():
        typer.echo(f"❌ {ddl} not found. Create dbt/models.yml or dbt/ddl/ directory.")
        raise typer.Exit(1)

    output_dir = Path(output)
    typer.echo(f"👀 Watching {ddl_path} for changes (Ctrl+C to stop)...")
    typer.echo(f"   Auto-compiles → {output_dir}/")
    typer.echo("")

    import time

    def _get_mtime() -> float:
        """Get latest mtime across all DDL files (supports dir or single file)."""
        if ddl_path.is_dir():
            mtimes = [f.stat().st_mtime for f in ddl_path.glob("*.yml")]
            return max(mtimes) if mtimes else 0
        return ddl_path.stat().st_mtime

    last_mtime = _get_mtime()

    # Initial compile
    try:
        config = yaml.safe_load(CONFIG_FILE.read_text()) if CONFIG_FILE.exists() else None
        results = compile_all(ddl_path, output_dir, forge_config=config)
        model_count = len([k for k in results if k != "_schema"])
        typer.echo(f"  ✅ Initial compile: {model_count} models")
    except Exception as e:
        typer.echo(f"  ❌ Compile error: {e}")

    try:
        while True:
            time.sleep(poll_interval)
            current_mtime = _get_mtime()
            if current_mtime != last_mtime:
                last_mtime = current_mtime
                typer.echo(f"  🔄 Change detected — recompiling...")
                try:
                    results = compile_all(ddl_path, output_dir, forge_config=config)
                    model_count = len([k for k in results if k != "_schema"])
                    typer.echo(f"  ✅ Compiled {model_count} models")
                except Exception as e:
                    typer.echo(f"  ❌ Compile error: {e}")
    except KeyboardInterrupt:
        typer.echo("\n👋 Watch mode stopped.")


@app.command(name="dev-down")
def dev_down(
    schema_suffix: Optional[str] = typer.Option(None, "--schema", "-s", help="Dev schema suffix (default: username)"),
):
    """forge dev-down → tears down isolated dev schema"""
    if not CONFIG_FILE.exists():
        typer.echo("❌ No forge.yml found.")
        raise typer.Exit(1)

    config = yaml.safe_load(CONFIG_FILE.read_text())
    prof = resolve_profile(config, profile_name="dev")
    base_schema = prof.get("schema", "silver")

    import getpass
    suffix = schema_suffix or getpass.getuser()
    dev_schema = f"dev_{base_schema}_{suffix}"

    typer.echo(f"🛑 Tearing down dev environment...")
    typer.echo(f"  📦 Dropping schema: {dev_schema}")

    catalog = prof.get("catalog", "main")
    drop_sql = f"DROP SCHEMA IF EXISTS {catalog}.{dev_schema} CASCADE"

    try:
        subprocess.run(
            ["dbt", "run-operation", "run_query", "--args",
             yaml.dump({"sql": drop_sql}),
             "--project-dir", ".", "--target", "dev"],
            check=True, capture_output=True,
        )
        typer.echo(f"  ✅ Schema {dev_schema} dropped")
    except (FileNotFoundError, subprocess.CalledProcessError):
        typer.echo(f"  ⚠️  Could not auto-drop schema. Run manually:")
        typer.echo(f"     {drop_sql}")

    typer.echo(f"🎉 Dev environment torn down.")


# =============================================
# PROFILES – list, inspect, generate profiles.yml
# =============================================
@app.command(name="profiles")
def profiles_cmd(
    generate: bool = typer.Option(False, "--generate", help="Auto-generate profiles.yml from forge.yml"),
    show_connection: bool = typer.Option(False, "--show-connection", help="Show resolved connection details"),
    profile: Optional[str] = typer.Option(None, "--profile", "-p", help="Show details for one profile"),
):
    """forge profiles → list profiles, generate profiles.yml, show connections"""
    if not CONFIG_FILE.exists():
        typer.echo("❌ No forge.yml found. Run 'forge setup' first!")
        raise typer.Exit(1)

    config = yaml.safe_load(CONFIG_FILE.read_text())

    if generate:
        text = generate_profiles_yml(config, output_path=Path("profiles.yml"))
        typer.echo("✅ Generated profiles.yml from forge.yml profiles")
        typer.echo(f"\n{text}")
        return

    forge_profiles = config.get("profiles", {})
    active = config.get("active_profile", "(none)")

    if not forge_profiles:
        typer.echo("📋 No profiles defined in forge.yml (using legacy flat config).")
        typer.echo("   Add a profiles: block to enable multi-environment support.")
        return

    # Show ~/.databrickscfg profiles for context
    dbr_profiles = list_databrickscfg_profiles()
    if dbr_profiles:
        typer.echo(f"🔗 ~/.databrickscfg profiles: {', '.join(dbr_profiles)}")
        typer.echo("")

    typer.echo(f"📋 Forge profiles (active: {active}):\n")

    for name, prof in forge_profiles.items():
        is_active = " ← active" if name == active else ""
        platform = prof.get("platform", "databricks")
        dbr_ref = prof.get("databricks_profile", "")
        env_tag = prof.get("env", "")
        expanded = _expand_env_prefix(prof, config)
        catalog = expanded.get("catalog", config.get("catalog", "main"))
        schema = expanded.get("schema", config.get("schema", "silver"))
        compute = prof.get("compute", {})
        compute_type = compute.get("type", "serverless") if isinstance(compute, dict) else compute

        typer.echo(f"  {'▸' if name == active else '·'} {name}{is_active}")
        typer.echo(f"      platform: {platform}  |  catalog: {catalog}  |  schema: {schema}  |  compute: {compute_type}")
        if env_tag:
            typer.echo(f"      env: {env_tag}")
        if dbr_ref:
            typer.echo(f"      databricks_profile: {dbr_ref}")

        # Show schemas/catalogs mapping (expanded from env: if needed)
        schemas_map = expanded.get("schemas", {})
        catalogs_map = expanded.get("catalogs", {})
        if schemas_map:
            schema_parts = [f"{layer}={s}" for layer, s in schemas_map.items()]
            typer.echo(f"      schemas: {', '.join(schema_parts)}")
        if catalogs_map:
            catalog_parts = [f"{layer}={c}" for layer, c in catalogs_map.items()]
            typer.echo(f"      catalogs: {', '.join(catalog_parts)}")

        if (show_connection or profile == name):
            try:
                resolved = resolve_connection(prof)
                if resolved.host:
                    typer.echo(f"      host: {resolved.host}")
                if resolved.http_path:
                    typer.echo(f"      http_path: {resolved.http_path}")
                if resolved.port:
                    typer.echo(f"      port: {resolved.port}")
                if resolved.database:
                    typer.echo(f"      database: {resolved.database}")
                typer.echo(f"      ✅ Connection resolved")
            except Exception as e:
                typer.echo(f"      ⚠️  {e}")

        typer.echo("")


# =============================================
# MAIN ENTRY POINT – matches pyproject.toml
# =============================================
def main():
    """This is what Poetry calls when you type 'forge'"""
    app()

if __name__ == "__main__":
    main()

# =============================================
# Why this file is perfect for us
# =============================================
# • Typer = beautiful help text, zero flags needed
# • setup/deploy/teardown/diff = exactly the 3 commands we promised
# • All heavy lifting (compute, graph, macros) stays in EXTERNAL dbt-dab-tools package
# • SQL preferred, Python UDFs allowed via macros
# • Portability baked in (target_platform switch works later)
# • Every single line has a comment → THE CODE IS THE DOCUMENTATION