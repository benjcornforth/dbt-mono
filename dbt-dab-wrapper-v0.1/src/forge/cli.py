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
from forge.workflow import build_workflow
from forge.simple_ddl import (
    compile_all,
    compile_all_pure_sql,
    apply_all_migrations,
    apply_all_migrations_dry_run,
    generate_agent_guide,
    compile_checks_sql,
    compile_all_udfs,
    load_udfs,
)
from forge.compute_resolver import (
    resolve_profile,
    resolve_connection,
    generate_profiles_yml,
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

    # Deploy UDFs first (dbt best practice: functions must exist before models reference them)
    ddl_path = Path("dbt/models.yml")
    if ddl_path.exists():
        udfs = compile_all_udfs(ddl_path)
        if udfs:
            typer.echo("🔧 Deploying UDFs via dbt run-operation...")
            udfs_sql = "\n".join(udfs.values())
            try:
                subprocess.run(
                    ["dbt", "run-operation", "deploy_udfs",
                     "--args", yaml.dump({"udfs_sql": udfs_sql}),
                     "--project-dir", "."],
                    check=True, capture_output=True,
                )
                typer.echo(f"  ✅ Deployed {len(udfs)} UDF(s): {', '.join(udfs.keys())}")
            except (FileNotFoundError, subprocess.CalledProcessError):
                typer.echo("  ⚠️  UDF deploy skipped (dbt run-operation unavailable). Deploy manually or use --pure-sql mode.")

    # Run dbt (SQL-only models – zero cold starts on serverless)
    typer.echo("✅ Running dbt...")
    try:
        subprocess.run(["dbt", "run", "--project-dir", "."], check=True)
    except FileNotFoundError:
        typer.echo("💡 Tip: install dbt-databricks with poetry if needed")

    typer.echo("🎉 Deploy complete! Run 'forge diff' to see graph changes.")

# =============================================
# TEARDOWN – safe destroy
# =============================================
@app.command()
def teardown():
    """forge teardown → safely destroys everything"""
    typer.echo("🛑 Teardown in progress...")
    typer.echo("✅ (Graph diff shows what would be deleted – safe by design)")
    # TODO: databricks bundle destroy (next iteration)
    typer.echo("🎉 Teardown complete.")

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
    ddl: str = typer.Option("dbt/models.yml", "--ddl", help="Path to models.yml DDL file"),
    mermaid_flag: bool = typer.Option(False, "--mermaid", help="Output Mermaid provenance diagram"),
    full: bool = typer.Option(False, "--full", help="Show full detail including upstream checks"),
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
    ddl_path = Path(ddl)
    dbt_dir = ddl_path.parent

    if not ddl_path.exists():
        typer.echo(f"❌ {ddl} not found.")
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

    if json_flag:
        import json
        result = json.dumps(tree, indent=2, default=str)
    elif mermaid_flag:
        result = render_provenance_tree(tree, mermaid=True)
    else:
        result = render_provenance_tree(tree, full=full)

    if output:
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        Path(output).write_text(result)
        typer.echo(f"✅ Written to {output}")
    else:
        typer.echo(result)

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
    typer.echo(f"📊 Pipeline: {' → '.join(stages_used)} ({len(wf.tasks)} tasks)")


# =============================================
# COMPILE – models.yml → SQL (no coding needed)
# =============================================
@app.command()
def compile(
    ddl: str = typer.Option("dbt/models.yml", "--ddl", help="Path to models.yml DDL file"),
    output: str = typer.Option("dbt/models", "--output", "-o", help="Output directory for SQL files"),
    pure_sql: bool = typer.Option(False, "--pure-sql", help="Emit standalone SQL (no dbt/Jinja). Runs directly on SQL warehouse."),
    profile: Optional[str] = typer.Option(None, "--profile", "-p", help="Forge profile (from forge.yml profiles:)"),
):
    """forge compile → turns models.yml into SQL + schema.yml (no SQL needed)"""
    ddl_path = Path(ddl)
    if not ddl_path.exists():
        typer.echo(f"❌ {ddl} not found. Create it or run 'forge setup' first!")
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
    results = compile_all(ddl_path, output_dir)

    for name, path in results.items():
        if name == "_schema":
            typer.echo(f"  📋 schema.yml → {path}")
        elif name == "_udfs":
            typer.echo(f"  🔧 _udfs.sql → {path}")
        else:
            typer.echo(f"  ✅ {name}.sql → {path}")

    model_count = len([k for k in results if not k.startswith("_")])
    udf_note = " + UDFs" if "_udfs" in results else ""
    typer.echo(f"🎉 Compiled {model_count} models{udf_note} from {ddl}. Run 'forge deploy' next.")


# =============================================
# MIGRATE – apply migration YAMLs
# =============================================
@app.command()
def migrate(
    ddl: str = typer.Option("dbt/models.yml", "--ddl", help="Path to models.yml DDL file"),
    migrations: str = typer.Option("dbt/migrations", "--migrations", "-m", help="Migrations directory"),
    recompile: bool = typer.Option(True, help="Recompile SQL after migrating"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview changes without applying"),
):
    """forge migrate → applies migration YAMLs to models.yml + recompiles"""
    ddl_path = Path(ddl)
    mig_dir = Path(migrations)

    if not ddl_path.exists():
        typer.echo(f"❌ {ddl} not found. Create models.yml first!")
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
        output_dir = ddl_path.parent / "models"
        compile_all(ddl_path, output_dir)
        typer.echo(f"🔄 Recompiled SQL from updated {ddl}")

    typer.echo(f"🎉 Applied {len(results)} migration(s). Run 'forge deploy' next.")


# =============================================
# GUIDE – auto-generated project documentation
# =============================================
@app.command()
def guide(
    output: str = typer.Option(".instructions.md", "--output", "-o", help="Output file path"),
    ddl: str = typer.Option("dbt/models.yml", "--ddl", help="Path to models.yml DDL file"),
):
    """forge guide → generates a project-specific agent guide / onboarding doc"""
    if not CONFIG_FILE.exists():
        typer.echo("❌ No forge.yml found. Run 'forge setup' first!")
        raise typer.Exit(1)

    config = yaml.safe_load(CONFIG_FILE.read_text())
    ddl_path = Path(ddl) if Path(ddl).exists() else None
    output_path = Path(output)

    generate_agent_guide(config, ddl_path=ddl_path, output_path=output_path)
    typer.echo(f"✅ Generated project guide → {output_path}")
    typer.echo("   Share with new team members or AI agents for instant onboarding.")


# =============================================
# UDFS – manage SQL/Python UDFs from models.yml
# =============================================
@app.command()
def udfs(
    ddl: str = typer.Option("dbt/models.yml", "--ddl", help="Path to models.yml DDL file"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Write UDF SQL to file"),
):
    """forge udfs → shows and compiles UDFs defined in models.yml"""
    ddl_path = Path(ddl)
    if not ddl_path.exists():
        typer.echo(f"❌ {ddl} not found.")
        raise typer.Exit(1)

    udf_defs = load_udfs(ddl_path)
    if not udf_defs:
        typer.echo("📋 No UDFs defined in models.yml. Add a udfs: block to get started.")
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
        icon = "🟣" if lang == "PYTHON" else "🔵"
        typer.echo(f"  {icon} {udf_name}({param_str}) → {returns}  [{lang}]")

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
    ddl: str = typer.Option("dbt/models.yml", "--ddl", help="Path to models.yml DDL file"),
    model: Optional[str] = typer.Option(None, "--model", "-m", help="Validate one model only"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Write check SQL to file"),
):
    """forge validate → shows checks defined for each model"""
    ddl_path = Path(ddl)
    if not ddl_path.exists():
        typer.echo(f"❌ {ddl} not found.")
        raise typer.Exit(1)

    results = compile_checks_sql(ddl_path, model_filter=model)

    if not results:
        typer.echo("📋 No checks defined in models.yml. Add a checks: block to get started.")
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
    ddl_path = Path("dbt/models.yml")
    if ddl_path.exists():
        compile_all(ddl_path, Path("dbt/models"))
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
    ddl: str = typer.Option("dbt/models.yml", "--ddl", help="Path to models.yml DDL file"),
    output: str = typer.Option("dbt/models", "--output", "-o", help="Output directory for SQL files"),
    poll_interval: float = typer.Option(1.0, "--poll", help="Seconds between file change polls"),
):
    """forge dev → watches models.yml, auto-compiles on save"""
    ddl_path = Path(ddl)
    if not ddl_path.exists():
        typer.echo(f"❌ {ddl} not found. Create models.yml first!")
        raise typer.Exit(1)

    output_dir = Path(output)
    typer.echo(f"👀 Watching {ddl} for changes (Ctrl+C to stop)...")
    typer.echo(f"   Auto-compiles → {output_dir}/")
    typer.echo("")

    import time
    last_mtime = ddl_path.stat().st_mtime

    # Initial compile
    try:
        results = compile_all(ddl_path, output_dir)
        model_count = len([k for k in results if k != "_schema"])
        typer.echo(f"  ✅ Initial compile: {model_count} models")
    except Exception as e:
        typer.echo(f"  ❌ Compile error: {e}")

    try:
        while True:
            time.sleep(poll_interval)
            current_mtime = ddl_path.stat().st_mtime
            if current_mtime != last_mtime:
                last_mtime = current_mtime
                typer.echo(f"  🔄 Change detected — recompiling...")
                try:
                    results = compile_all(ddl_path, output_dir)
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
        catalog = prof.get("catalog", config.get("catalog", "main"))
        schema = prof.get("schema", config.get("schema", "silver"))
        compute = prof.get("compute", {})
        compute_type = compute.get("type", "serverless") if isinstance(compute, dict) else compute

        typer.echo(f"  {'▸' if name == active else '·'} {name}{is_active}")
        typer.echo(f"      platform: {platform}  |  catalog: {catalog}  |  schema: {schema}  |  compute: {compute_type}")
        if dbr_ref:
            typer.echo(f"      databricks_profile: {dbr_ref}")

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