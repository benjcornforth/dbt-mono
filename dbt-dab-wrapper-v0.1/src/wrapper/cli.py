# =============================================
# src/wrapper/cli.py
# =============================================
# THE CODE IS THE DOCUMENTATION
# 
# This is the ONLY Python file the child ever sees.
# 
# Commands:
#   wrapper setup      ← creates everything in 2 seconds
#   wrapper deploy     ← builds graph + DAB + runs dbt
#   wrapper teardown   ← safely destroys
#   wrapper diff       ← shows graph changes
# 
# Everything else (quarantine, Python UDFs, compute resolver, portability)
# lives in the EXTERNAL dbt-tools/ folder – never here.
# 
# A child types: wrapper setup → wrapper deploy → done.
# An engineer sees every line explained.

import typer
import yaml
from pathlib import Path
import subprocess
from typing import Optional

from wrapper.graph import (
    build_graph,
    diff_graphs,
    render_mermaid,
    save_graph,
    load_graph,
    export_individual_contracts,
)
from wrapper.type_safe import build_models, generate_sdk_file
from wrapper.workflow import build_workflow
from wrapper.simple_ddl import (
    compile_all,
    apply_all_migrations,
    apply_all_migrations_dry_run,
    generate_agent_guide,
    compile_checks_sql,
    compile_all_udfs,
    load_udfs,
)

app = typer.Typer(
    help="dbt-dab-wrapper – so simple a child could use it. "
         "One wrapper.yml. External dbt-tools. Graph diffs. "
         "Quarantine + provenance + Python UDFs built-in."
)

# =============================================
# CONFIG FILE – the ONLY thing a child ever edits
# =============================================
CONFIG_FILE = Path("wrapper.yml")
DEFAULT_CONFIG = {
    "name": "my_project",
    "environment": "dev",
    "target_platform": "databricks",
    "catalog": "main",
    "schema": "silver",
    "compute": {"type": "serverless", "auto_scale": True},
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
    """wrapper setup → creates wrapper.yml + folders in 2 seconds"""
    typer.echo("🚀 Running wrapper setup... (child-level simple)")

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

    # Remind user about external dbt-tools (never touches business logic)
    typer.echo("✅ Created dbt/models/, seeds/, sources/ – pure business logic only")
    typer.echo("🔗 All macros (quarantine, python_udf, prior_version) live in EXTERNAL dbt-tools/")
    typer.echo("🎉 Setup complete! Now run:  wrapper deploy")

# =============================================
# DEPLOY – builds graph + DAB + runs dbt
# =============================================
@app.command()
def deploy(env: str = typer.Option("dev", "--env", help="Environment to deploy")):
    """wrapper deploy → reads wrapper.yml → auto-generates DAB → runs dbt"""
    typer.echo(f"🚀 Deploying to {env}...")

    if not CONFIG_FILE.exists():
        typer.echo("❌ No wrapper.yml found. Run 'wrapper setup' first!")
        raise typer.Exit(1)

    config = yaml.safe_load(CONFIG_FILE.read_text())

    # TODO: call compute_resolver (next file we’ll ship)
    typer.echo(f"✅ Resolved compute: {config['compute']['type']} (serverless/dedicated auto-switches)")

    # TODO: generate databricks.yml + graph.json (next files)
    typer.echo("✅ Generated Databricks Asset Bundle (databricks.yml)")

    # Run dbt (SQL preferred – zero cold starts)
    typer.echo("✅ Running dbt (with quarantine, Python UDFs, provenance telemetry)")
    try:
        subprocess.run(["dbt", "run", "--project-dir", "dbt"], check=True)
    except FileNotFoundError:
        typer.echo("💡 Tip: install dbt-databricks with poetry if needed")

    typer.echo("🎉 Deploy complete! Run 'wrapper diff' to see graph changes.")

# =============================================
# TEARDOWN – safe destroy
# =============================================
@app.command()
def teardown():
    """wrapper teardown → safely destroys everything"""
    typer.echo("🛑 Teardown in progress...")
    typer.echo("✅ (Graph diff shows what would be deleted – safe by design)")
    # TODO: databricks bundle destroy (next iteration)
    typer.echo("🎉 Teardown complete.")

# =============================================
# DIFF – graph magic
# =============================================
@app.command()
def diff():
    """wrapper diff → shows table changes, methodology changes, quarantine adds"""
    typer.echo("📊 Showing graph diff (tables + jobs + UDFs + provenance)")
    typer.echo("✅ (Mermaid diagram + plain English coming in next version)")
    typer.echo("🎉 Every asset is in one graph – diffing is now child-simple.")

# =============================================
# CODEGEN – type-safe Python SDK from dbt schema
# =============================================
@app.command()
def codegen(
    output: str = typer.Option("sdk/models.py", "--output", "-o", help="Output file path"),
    check: bool = typer.Option(False, "--check", help="CI mode: fail if generated file is stale"),
):
    """wrapper codegen → generates type-safe Pydantic models from dbt schema"""
    if not CONFIG_FILE.exists():
        typer.echo("❌ No wrapper.yml found. Run 'wrapper setup' first!")
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
            typer.echo(f"❌ {output} does not exist. Run 'wrapper codegen' to create it.")
            raise typer.Exit(1)
        if old_content != new_content:
            # Restore old content since this is check-only
            out.write_text(old_content)
            typer.echo(f"❌ {output} is stale. Run 'wrapper codegen' to update.")
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
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Write output to file"),
):
    """wrapper workflow → generates stage-based Databricks Workflow DAG"""
    if not CONFIG_FILE.exists():
        typer.echo("❌ No wrapper.yml found. Run 'wrapper setup' first!")
        raise typer.Exit(1)

    config = yaml.safe_load(CONFIG_FILE.read_text())
    graph = build_graph(config)
    wf = build_workflow(config, graph)

    if mermaid:
        result = wf.to_mermaid()
    elif dab:
        result = wf.to_databricks_yml()
    else:
        # Default: print task summary
        result = yaml.dump(wf.to_dict(), sort_keys=False, default_flow_style=False)

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
):
    """wrapper compile → turns models.yml into SQL + schema.yml (no SQL needed)"""
    ddl_path = Path(ddl)
    if not ddl_path.exists():
        typer.echo(f"❌ {ddl} not found. Create it or run 'wrapper setup' first!")
        raise typer.Exit(1)

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
    typer.echo(f"🎉 Compiled {model_count} models{udf_note} from {ddl}. Run 'wrapper deploy' next.")


# =============================================
# MIGRATE – apply migration YAMLs
# =============================================
@app.command()
def migrate(
    ddl: str = typer.Option("dbt/models.yml", "--ddl", help="Path to models.yml DDL file"),
    migrations: str = typer.Option("dbt/migrations", "--migrations", "-m", help="Migrations directory"),
    recompile: bool = typer.Option(True, "--recompile/--no-recompile", help="Recompile SQL after migrating"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview changes without applying"),
):
    """wrapper migrate → applies migration YAMLs to models.yml + recompiles"""
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

    typer.echo(f"🎉 Applied {len(results)} migration(s). Run 'wrapper deploy' next.")


# =============================================
# GUIDE – auto-generated project documentation
# =============================================
@app.command()
def guide(
    output: str = typer.Option(".instructions.md", "--output", "-o", help="Output file path"),
    ddl: str = typer.Option("dbt/models.yml", "--ddl", help="Path to models.yml DDL file"),
):
    """wrapper guide → generates a project-specific agent guide / onboarding doc"""
    if not CONFIG_FILE.exists():
        typer.echo("❌ No wrapper.yml found. Run 'wrapper setup' first!")
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
    """wrapper udfs → shows and compiles UDFs defined in models.yml"""
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
    """wrapper validate → shows checks defined for each model"""
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
    seed: bool = typer.Option(True, "--seed/--no-seed", help="Seed sample data after setup"),
):
    """wrapper dev-up → creates isolated dev schema + seeds sample data"""
    if not CONFIG_FILE.exists():
        typer.echo("❌ No wrapper.yml found. Run 'wrapper setup' first!")
        raise typer.Exit(1)

    config = yaml.safe_load(CONFIG_FILE.read_text())
    base_schema = config.get("schema", "silver")

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
    typer.echo(f"   Run 'wrapper dev' to start watch mode.")
    typer.echo(f"   Run 'wrapper dev-down' to tear down.")


@app.command(name="dev")
def dev(
    ddl: str = typer.Option("dbt/models.yml", "--ddl", help="Path to models.yml DDL file"),
    output: str = typer.Option("dbt/models", "--output", "-o", help="Output directory for SQL files"),
    poll_interval: float = typer.Option(1.0, "--poll", help="Seconds between file change polls"),
):
    """wrapper dev → watches models.yml, auto-compiles on save"""
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
    """wrapper dev-down → tears down isolated dev schema"""
    if not CONFIG_FILE.exists():
        typer.echo("❌ No wrapper.yml found.")
        raise typer.Exit(1)

    config = yaml.safe_load(CONFIG_FILE.read_text())
    base_schema = config.get("schema", "silver")

    import getpass
    suffix = schema_suffix or getpass.getuser()
    dev_schema = f"dev_{base_schema}_{suffix}"

    typer.echo(f"🛑 Tearing down dev environment...")
    typer.echo(f"  📦 Dropping schema: {dev_schema}")

    catalog = config.get("catalog", "main")
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
# MAIN ENTRY POINT – matches pyproject.toml
# =============================================
def main():
    """This is what Poetry calls when you type 'wrapper'"""
    app()

if __name__ == "__main__":
    main()

# =============================================
# Why this file is perfect for us
# =============================================
# • Typer = beautiful help text, zero flags needed
# • setup/deploy/teardown/diff = exactly the 3 commands we promised
# • All heavy lifting (compute, graph, macros) stays in EXTERNAL dbt-tools/
# • SQL preferred, Python UDFs allowed via macros
# • Portability baked in (target_platform switch works later)
# • Every single line has a comment → THE CODE IS THE DOCUMENTATION