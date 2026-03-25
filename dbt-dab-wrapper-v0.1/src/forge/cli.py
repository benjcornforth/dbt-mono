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
# lives in the local Forge runtime and project macros.
# 
# A child types: forge setup → forge deploy → done.
# An engineer sees every line explained.

import os
import json
import copy
import shutil
import typer
import yaml
from datetime import datetime, timezone
from contextlib import contextmanager
from pathlib import Path
import subprocess
from typing import Optional

from forge.graph import (
    build_graph,
    diff_graphs,
    render_mermaid,
    render_current_table_relationships_mermaid,
    save_graph,
    load_graph,
    export_individual_contracts,
    walk_column_lineage,
    render_provenance_tree,
)
from forge.project_paths import DEFAULT_SDK_OUTPUT, GENERATED_CODE_ROOT, PROJECT_CODE_ROOT, PROJECT_PYTHON_DIR
from forge.project_spec import ProjectSpec, load_project_spec
from forge.type_safe import build_models, generate_sdk_file
from forge.workflow import build_workflow, build_domain_workflows, generate_bundle_config, render_workflows_mermaid, run_bundle_command
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
         "One forge.yml. Local bundled macros. Graph diffs. "
         "Quarantine + provenance + Python UDFs built-in."
)

# =============================================
# CONFIG FILE – the ONLY thing a child ever edits
# =============================================

def _find_project_root(start: Path = None) -> Path:
    """Find the directory containing forge.yml.
    Searches: cwd → parent directories → immediate child directories.
    Returns cwd as fallback if not found."""
    current = (start or Path.cwd()).resolve()
    # 1. Walk up from cwd
    check = current
    while True:
        if (check / "forge.yml").exists():
            return check
        parent = check.parent
        if parent == check:            # filesystem root
            break
        check = parent
    # 2. Check immediate child directories (one level)
    try:
        candidates = [d for d in current.iterdir()
                      if d.is_dir() and (d / "forge.yml").exists()]
        if len(candidates) == 1:
            return candidates[0]
    except PermissionError:
        pass
    return current                     # fallback


_PROJECT_ROOT = _find_project_root()
CONFIG_FILE = _PROJECT_ROOT / "forge.yml"

# Anchor cwd to project root so all relative paths (dbt/ddl, resources/, etc.) resolve correctly
# even when `forge` is invoked from a parent or sibling directory.
if _PROJECT_ROOT != Path.cwd():
    os.chdir(_PROJECT_ROOT)

# Canonical DDL source of truth.
DDL_DEFAULT = "dbt/ddl"


def _resolve_ddl(ddl: str) -> Path:
    """Resolve the canonical DDL directory under dbt/ddl/."""
    return Path(ddl)


def _require_profile_name(
    config: dict,
    *,
    command_name: str,
    profile: str | None = None,
    target: str | None = None,
) -> str:
    profiles = config.get("profiles", {})
    selected = target or profile

    if not profiles:
        typer.echo(f"❌ {command_name} requires a profiles: block in forge.yml.")
        raise typer.Exit(1)

    if not selected:
        typer.echo(f"❌ {command_name} requires an explicit target/profile.")
        typer.echo("   Specify --target <name> or --profile <name>.")
        typer.echo(f"   Available profiles: {', '.join(profiles.keys())}")
        raise typer.Exit(1)

    if selected not in profiles:
        typer.echo(f"❌ Unknown target/profile '{selected}'.")
        typer.echo(f"   Available profiles: {', '.join(profiles.keys())}")
        raise typer.Exit(1)

    return selected


def _artifact_targets_root() -> Path:
    return _PROJECT_ROOT / "artifacts" / "targets"


def _target_artifact_root(target: str) -> Path:
    return _artifact_targets_root() / target


def _default_compile_output(profile_name: str) -> Path:
    return _target_artifact_root(profile_name) / "dbt" / "models"


def _default_pure_sql_output(profile_name: str) -> Path:
    return _target_artifact_root(profile_name) / "sql"


def _graph_snapshot_path(profile_name: str | None = None) -> Path:
    if profile_name:
        return _target_artifact_root(profile_name) / "graph.json"
    return _PROJECT_ROOT / "artifacts" / "graph.json"


def _default_workflow_output_dir(profile_name: str) -> Path:
    return _target_artifact_root(profile_name) / "resources" / "jobs"


def _expected_generated_paths(project_spec: ProjectSpec, config: dict, profile_name: str) -> dict[str, list[dict[str, str]]]:
    target_root = _target_artifact_root(profile_name)
    project_id = config.get("id", "project")

    def _status(path: Path) -> str:
        return "present" if path.exists() else "missing"

    models: list[dict[str, str]] = []
    for name, definition in sorted(project_spec.models.items()):
        layer = definition.get("layer", "default") if isinstance(definition, dict) else "default"
        out_path = target_root / "dbt" / "models" / project_id / layer / f"{name}.sql"
        source_path = project_spec.assets[next(ref for ref, asset in project_spec.assets.items() if asset.section == "models" and ref.name == name)].source_path
        models.append({
            "name": name,
            "class": definition.get("class", "") if isinstance(definition, dict) else "",
            "domain": definition.get("domain", "") if isinstance(definition, dict) else "",
            "layer": layer,
            "managed_by": definition.get("managed_by", "dbt") if isinstance(definition, dict) else "dbt",
            "source": str(source_path),
            "generated": str(out_path.relative_to(_PROJECT_ROOT)),
            "status": _status(out_path),
        })

    def _entries(section: str, subdir: str, suffix: str = ".sql") -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        for ref, asset in sorted(project_spec.assets.items(), key=lambda item: item[0].key):
            if asset.section != section:
                continue
            out_path = target_root / "dbt" / subdir / f"{ref.name}{suffix}"
            rows.append({
                "name": ref.name,
                "class": ref.asset_class or "",
                "domain": ref.domain or "",
                "layer": ref.layer or "",
                "source": str(asset.source_path),
                "generated": str(out_path.relative_to(_PROJECT_ROOT)),
                "status": _status(out_path),
            })
        return rows

    sources_path = target_root / "dbt" / "sources" / "_sources.yml"
    schema_path = target_root / "dbt" / "models" / "schema.yml"
    pure_sql_root = target_root / "sql"
    jobs_root = target_root / "resources" / "jobs"

    return {
        "models": models,
        "udfs": _entries("udfs", "functions"),
        "volumes": _entries("volumes", "volumes"),
        "seeds": [
            {
                "name": name,
                "class": definition.get("class", "") if isinstance(definition, dict) else "",
                "domain": definition.get("domain", "") if isinstance(definition, dict) else "",
                "layer": definition.get("layer", "") if isinstance(definition, dict) else "",
                "source": str(project_spec.assets[next(ref for ref, asset in project_spec.assets.items() if asset.section == "seeds" and ref.name == name)].source_path),
                "generated": str(sources_path.relative_to(_PROJECT_ROOT)),
                "status": _status(sources_path),
            }
            for name, definition in sorted(project_spec.seeds.items())
        ],
        "sources": [
            {
                "name": name,
                "class": definition.get("class", "") if isinstance(definition, dict) else "",
                "domain": definition.get("domain", "") if isinstance(definition, dict) else "",
                "layer": definition.get("layer", "") if isinstance(definition, dict) else "",
                "source": str(project_spec.assets[next(ref for ref, asset in project_spec.assets.items() if asset.section == "sources" and ref.name == name)].source_path),
                "generated": str(sources_path.relative_to(_PROJECT_ROOT)),
                "status": _status(sources_path),
            }
            for name, definition in sorted(project_spec.sources.items())
        ],
        "artifacts": {
            "schema": {"path": str(schema_path.relative_to(_PROJECT_ROOT)), "status": _status(schema_path)},
            "sources": {"path": str(sources_path.relative_to(_PROJECT_ROOT)), "status": _status(sources_path)},
            "sql_setup": sorted(str(path.relative_to(_PROJECT_ROOT)) for path in (pure_sql_root / "setup").glob("*.sql")) if (pure_sql_root / "setup").is_dir() else [],
            "sql_process": sorted(str(path.relative_to(_PROJECT_ROOT)) for path in (pure_sql_root / "process").glob("*.sql")) if (pure_sql_root / "process").is_dir() else [],
            "sql_teardown": sorted(str(path.relative_to(_PROJECT_ROOT)) for path in (pure_sql_root / "teardown").glob("*.sql")) if (pure_sql_root / "teardown").is_dir() else [],
            "jobs": sorted(str(path.relative_to(_PROJECT_ROOT)) for path in jobs_root.glob("*.yml")) if jobs_root.is_dir() else [],
            "graph": {"path": str((_target_artifact_root(profile_name) / "graph.json").relative_to(_PROJECT_ROOT)), "status": _status(_target_artifact_root(profile_name) / "graph.json")},
            "deploy_manifest": {"path": str((_target_artifact_root(profile_name) / "deploy_manifest.json").relative_to(_PROJECT_ROOT)), "status": _status(_target_artifact_root(profile_name) / "deploy_manifest.json")},
        },
    }


def _render_inspect_markdown(*, config: dict, profile_name: str, summary: dict[str, object]) -> str:
    lines: list[str] = []
    lines.append(f"# Forge Inspect — {config.get('name', 'project')} ({profile_name})")
    lines.append("")
    lines.append(f"Project id: `{config.get('id', 'project')}`")
    lines.append(f"Target root: `artifacts/targets/{profile_name}`")
    lines.append("")
    lines.append("## Authored DDL Inventory")
    lines.append("")
    for section in ("models", "udfs", "seeds", "volumes", "sources"):
        rows = summary[section]
        lines.append(f"### {section.capitalize()} ({len(rows)})")
        lines.append("")
        if not rows:
            lines.append("(none)")
            lines.append("")
            continue
        lines.append("| Name | Class | Domain | Layer | Source | Generated | Status |")
        lines.append("|------|-------|--------|-------|--------|-----------|--------|")
        for row in rows:
            managed_by = f" / {row['managed_by']}" if section == "models" and row.get("managed_by") else ""
            lines.append(
                f"| {row['name']}{managed_by} | {row['class'] or '-'} | {row['domain'] or '-'} | {row['layer'] or '-'} | `{row['source']}` | `{row['generated']}` | {row['status']} |"
            )
        lines.append("")

    artifacts = summary["artifacts"]
    lines.append("## Generated Artifact Inventory")
    lines.append("")
    lines.append("| Artifact | Path | Status |")
    lines.append("|----------|------|--------|")
    lines.append(f"| schema.yml | `{artifacts['schema']['path']}` | {artifacts['schema']['status']} |")
    lines.append(f"| _sources.yml | `{artifacts['sources']['path']}` | {artifacts['sources']['status']} |")
    lines.append(f"| graph snapshot | `{artifacts['graph']['path']}` | {artifacts['graph']['status']} |")
    lines.append(f"| deploy manifest | `{artifacts['deploy_manifest']['path']}` | {artifacts['deploy_manifest']['status']} |")
    lines.append("")
    for label, key in (("Setup SQL", "sql_setup"), ("Process SQL", "sql_process"), ("Teardown SQL", "sql_teardown"), ("Workflow YAML", "jobs")):
        lines.append(f"### {label} ({len(artifacts[key])})")
        lines.append("")
        if not artifacts[key]:
            lines.append("(none)")
        else:
            for path in artifacts[key]:
                lines.append(f"- `{path}`")
        lines.append("")

    lines.append("## Generation Commands")
    lines.append("")
    lines.append(f"- `forge compile --profile {profile_name}` → dbt models, functions, sources, schema.yml")
    lines.append(f"- `forge compile --pure-sql --profile {profile_name}` → setup/process/teardown SQL")
    lines.append(f"- `forge workflow --dab --profile {profile_name}` → workflow YAML under target resources/jobs")
    lines.append(f"- `forge diff --profile {profile_name}` → graph snapshot")
    lines.append(f"- `forge build --target {profile_name}` → deploy manifest + staged bundle")
    lines.append("")
    return "\n".join(lines) + "\n"


def _prepare_target_project(target: str, config: dict) -> tuple[Path, dict, dict]:
    target_root = _target_artifact_root(target)
    _reset_dir(target_root)
    target_config, profile = _resolve_target_config(config, target)
    _stage_target_support_files(target_root, target_config)
    return target_root, target_config, profile


def _compile_target_dbt_project(target: str, config: dict) -> tuple[Path, dict, dict, dict[str, Path]]:
    target_root, target_config, profile = _prepare_target_project(target, config)
    with _pushd(target_root):
        results = compile_all(Path("dbt") / "ddl", Path("dbt") / "models", forge_config=target_config)
    return target_root, target_config, profile, results


def _build_dbt_env(conn) -> dict[str, str]:
    dbt_env = os.environ.copy()
    if conn.host:
        dbt_env["DBT_DATABRICKS_HOST"] = conn.host
    if conn.token:
        dbt_env["DBT_DATABRICKS_TOKEN"] = conn.token
    if conn.http_path:
        dbt_env["DBT_DATABRICKS_HTTP_PATH"] = conn.http_path
    return dbt_env


def _build_dbt_runtime_vars(config: dict, prof: dict, conn) -> tuple[dict[str, str], list[str]]:
    schema_vars = get_schema_variables(prof, config)
    dbt_vars = {
        **schema_vars,
        "git_commit": config.get("git_commit", "local"),
        "compute_type": conn.compute_type or "serverless",
    }
    return schema_vars, ["--vars", yaml.dump(dbt_vars)]


def _verify_required_catalogs(conn, prof: dict, schema_vars: dict[str, str]) -> None:
    if conn.platform != "databricks":
        return

    required_catalogs = set(schema_vars.get(key, "") for key in schema_vars if key.startswith("catalog_"))
    if not required_catalogs:
        return

    typer.echo("🔍 Checking catalogs...")
    try:
        db_profile = prof.get("databricks_profile", "DEFAULT")
        result = subprocess.run(
            ["databricks", "unity-catalog", "catalogs", "list", "--profile", db_profile],
            check=True, capture_output=True, text=True,
        )
        catalogs_data = json.loads(result.stdout)
        existing_catalogs = set()
        items = catalogs_data if isinstance(catalogs_data, list) else catalogs_data.get("catalogs", [])
        for cat in items:
            if isinstance(cat, dict) and "name" in cat:
                existing_catalogs.add(cat["name"].lower())
            elif isinstance(cat, str):
                existing_catalogs.add(cat.lower())

        missing = sorted(c for c in required_catalogs if c.lower() not in existing_catalogs)
        if missing:
            typer.echo(f"  ❌ Missing catalog(s): {', '.join(missing)}")
            typer.echo("     Create them first:")
            for cat in missing:
                typer.echo(f"       CREATE CATALOG IF NOT EXISTS `{cat}`;")
            typer.echo("     Or update forge.yml scope/catalog_pattern to match existing catalogs.")
            raise typer.Exit(1)
        typer.echo(f"  ✅ All catalogs verified: {', '.join(sorted(required_catalogs))}")
    except subprocess.CalledProcessError:
        typer.echo("  ⚠️  Could not verify catalogs (databricks CLI error). Continuing...")
    except (FileNotFoundError, json.JSONDecodeError):
        typer.echo("  ⚠️  Could not verify catalogs (databricks CLI not found). Continuing...")


def _run_dbt_operation_sql(sql_text: str, *, dbt_args: list[str], dbt_env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["dbt", "run-operation", "deploy_udfs", "--args", yaml.dump({"udfs_sql": sql_text})] + dbt_args,
        check=True,
        capture_output=True,
        text=True,
        env=dbt_env,
    )


def _deploy_staged_volumes(schema_vars: dict[str, str], *, dbt_args: list[str], dbt_env: dict[str, str]) -> None:
    volumes_dir = Path("dbt/volumes")
    if not volumes_dir.is_dir():
        return

    vol_files = sorted(volumes_dir.glob("*.sql"))
    if not vol_files:
        return

    typer.echo("📦 Deploying Volumes...")
    for vol_file in vol_files:
        vol_sql = vol_file.read_text()
        for var_name, var_val in schema_vars.items():
            vol_sql = vol_sql.replace(f"{{{{ var(\"{var_name}\") }}}}", var_val)
        try:
            _run_dbt_operation_sql(vol_sql, dbt_args=dbt_args, dbt_env=dbt_env)
            typer.echo(f"  ✅ Volume: {vol_file.stem}")
        except subprocess.CalledProcessError as exc:
            output = (exc.stdout or "") + (exc.stderr or "")
            if "already exists" in output.lower():
                typer.echo(f"  ✅ Volume: {vol_file.stem} (exists)")
            else:
                typer.echo(f"  ⚠️  Volume {vol_file.stem} failed:")
                for line in output.strip().splitlines()[-5:]:
                    typer.echo(f"     {line}")
        except FileNotFoundError:
            typer.echo(f"  ⚠️  Skipped {vol_file.stem} (dbt not found)")


def _deploy_staged_udfs(prof: dict, config: dict, *, dbt_args: list[str], dbt_env: dict[str, str]) -> None:
    functions_dir = Path("dbt/functions")
    if not functions_dir.is_dir():
        return

    udf_files = sorted(functions_dir.glob("*.sql"))
    if not udf_files:
        return

    typer.echo("🔧 Deploying UDFs via dbt run-operation...")
    expanded_prof = _expand_env_prefix(prof, config)
    target_catalog = expanded_prof.get("catalog", "main")
    target_schema = expanded_prof.get("schema", "default")
    ensure_schema_sql = f"CREATE SCHEMA IF NOT EXISTS `{target_catalog}`.`{target_schema}`"

    sql_files = [path for path in udf_files if "LANGUAGE PYTHON" not in path.read_text().upper()]
    py_files = [path for path in udf_files if "LANGUAGE PYTHON" in path.read_text().upper()]

    def _deploy_udf_batch(files: list[Path], label: str) -> bool:
        sql_text = "\n".join(path.read_text() for path in files)
        sql_text = sql_text.replace("{{ target.catalog }}", target_catalog)
        sql_text = sql_text.replace("{{ target.schema }}", target_schema)
        sql_text = f"{ensure_schema_sql};\n{sql_text}"
        try:
            _run_dbt_operation_sql(sql_text, dbt_args=dbt_args, dbt_env=dbt_env)
            typer.echo(f"  ✅ Deployed {label}: {', '.join(path.stem for path in files)}")
            return True
        except subprocess.CalledProcessError as exc:
            output = (exc.stdout or "") + (exc.stderr or "")
            typer.echo(f"  ❌ {label} deploy failed:")
            for line in output.strip().splitlines()[-10:]:
                typer.echo(f"     {line}")
            return False
        except FileNotFoundError:
            typer.echo("  ⚠️  UDF deploy skipped (dbt not found).")
            return False

    if sql_files and not _deploy_udf_batch(sql_files, "SQL UDF(s)"):
        raise typer.Exit(1)
    if py_files and not _deploy_udf_batch(py_files, "Python UDF(s)"):
        typer.echo("  💡 Python UDFs require a Pro or Serverless SQL warehouse.")
        typer.echo("     Check your warehouse type, or deploy Python UDFs via a cluster.")


def _run_staged_dbt_commands(*, dbt_args: list[str], dbt_env: dict[str, str]) -> None:
    typer.echo("✅ Running dbt...")
    try:
        subprocess.run(["dbt", "run"] + dbt_args, check=True, env=dbt_env)
    except FileNotFoundError:
        typer.echo("💡 Tip: install dbt-databricks with poetry if needed")


def _run_local_staged_deploy(build_result: dict[str, Path | list[str] | str], *, schema_vars: dict[str, str], prof: dict, config: dict, dbt_vars_flag: list[str], dbt_env: dict[str, str]) -> None:
    typer.echo("📍 Running in local mode from the staged target bundle...")
    local_dbt_args = ["--project-dir", ".", "--profiles-dir", "."] + dbt_vars_flag

    with _pushd(build_result["target_root"]):
        _deploy_staged_volumes(schema_vars, dbt_args=local_dbt_args, dbt_env=dbt_env)
        _deploy_staged_udfs(prof, config, dbt_args=local_dbt_args, dbt_env=dbt_env)
        _run_staged_dbt_commands(dbt_args=local_dbt_args, dbt_env=dbt_env)


def _deploy_staged_bundle(target_root: Path, target_name: str) -> None:
    with _pushd(target_root):
        typer.echo("🚀 Running databricks bundle deploy from staged target root...")
        result = run_bundle_command("deploy", target=target_name)

    if result["success"]:
        typer.echo("✅ Bundle deployed to Databricks.")
        if result["output"]:
            for line in result["output"].strip().splitlines():
                typer.echo(f"  {line}")
        return

    typer.echo(f"  ⚠️  Bundle deploy failed: {result['error']}")
    typer.echo(f"  Run manually from {target_root}: databricks bundle deploy --target {target_name}")


@contextmanager
def _pushd(path: Path):
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


def _reset_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _copy_tree_if_exists(source: Path, destination: Path) -> None:
    if source.is_dir():
        shutil.copytree(source, destination, dirs_exist_ok=True)


def _copy_file_if_exists(source: Path, destination: Path) -> None:
    if source.is_file():
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)


def _is_markdown_output(output: str | None) -> bool:
    return bool(output) and Path(output).suffix.lower() == ".md"


def _render_mermaid_markdown(diagrams: list[tuple[str, str]]) -> str:
    sections: list[str] = []
    for _, diagram in diagrams:
        sections.append(f"```mermaid\n{diagram}\n```")
    return "\n\n".join(sections) + "\n"


def _build_visual_documentation_markdown(
    pipeline_markdown: str,
    table_lineage_markdown: str,
    column_lineage_markdown: str,
    *,
    model_dot_column: str,
) -> str:
    return "\n".join([
        "# Visual Documentation",
        "",
        "## Pipeline Workflow",
        "",
        pipeline_markdown.rstrip(),
        "",
        "## Table Lineage",
        "",
        table_lineage_markdown.rstrip(),
        "",
        f"## Column Lineage: {model_dot_column}",
        "",
        column_lineage_markdown.rstrip(),
        "",
    ])


def _materialize_profile_config(config: dict, profile_name: str) -> tuple[dict, dict]:
    profile = resolve_profile(config, profile_name=profile_name)
    materialized_config = copy.deepcopy(config)
    for legacy_key in [key for key in materialized_config if key.startswith("active") and key.endswith("profile")]:
        materialized_config.pop(legacy_key, None)
    materialized_config["profiles"] = {
        profile_name: {
            key: value
            for key, value in profile.items()
            if key != "_name"
        }
    }
    materialized_config["catalog"] = profile.get("catalog", materialized_config.get("catalog", "main"))
    materialized_config["schema"] = profile.get("schema", materialized_config.get("schema", "default"))
    materialized_config["compute"] = profile.get("compute", materialized_config.get("compute", {}))
    materialized_config["environment"] = profile.get("env", profile["_name"])
    return materialized_config, profile


def _resolve_target_config(config: dict, profile_name: str) -> tuple[dict, dict]:
    return _materialize_profile_config(config, profile_name)


def _stage_target_support_files(target_root: Path, target_config: dict) -> None:
    _copy_tree_if_exists(_PROJECT_ROOT / "dbt" / "ddl", target_root / "dbt" / "ddl")
    _copy_tree_if_exists(_PROJECT_ROOT / PROJECT_CODE_ROOT, target_root / PROJECT_CODE_ROOT)
    _copy_tree_if_exists(_PROJECT_ROOT / GENERATED_CODE_ROOT, target_root / GENERATED_CODE_ROOT)
    _copy_tree_if_exists(_PROJECT_ROOT / "dbt" / "seeds", target_root / "dbt" / "seeds")
    _copy_tree_if_exists(_PROJECT_ROOT / "src" / "forge", target_root / "forge")
    _copy_tree_if_exists(_PROJECT_ROOT / "macros", target_root / "macros")
    _copy_file_if_exists(_PROJECT_ROOT / "dbt_project.yml", target_root / "dbt_project.yml")
    generate_profiles_yml(target_config, output_path=target_root / "profiles.yml")
    (target_root / "forge.yml").write_text(
        yaml.dump(target_config, sort_keys=False, default_flow_style=False)
    )
    (target_root / "resources" / "jobs").mkdir(parents=True, exist_ok=True)


def _build_and_stage_wheel(target_root: Path) -> list[str]:
    subprocess.run(["poetry", "build"], check=True, cwd=_PROJECT_ROOT)
    source_dist = _PROJECT_ROOT / "dist"
    target_dist = target_root / "dist"
    target_dist.mkdir(parents=True, exist_ok=True)

    copied: list[str] = []
    for wheel in sorted(source_dist.glob("*.whl")):
        shutil.copy2(wheel, target_dist / wheel.name)
        copied.append(str(Path("dist") / wheel.name))
    if not copied:
        raise FileNotFoundError("No wheel files were produced in dist/ after poetry build.")
    return copied


def _write_deploy_manifest(
    target_root: Path,
    *,
    target: str,
    target_config: dict,
    pure_sql_results: dict[str, Path],
    dbt_results: dict[str, Path],
    job_artifacts: list[dict[str, str]],
    wheel_paths: list[str],
) -> Path:
    def _normalize_artifact_path(path: Path) -> str:
        return str(path.relative_to(target_root)) if path.is_absolute() else str(path)

    payload = {
        "target": target,
        "profile": target,
        "platform": (target_config.get("profiles", {}).get(target, {}) or {}).get("platform", "unknown"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "git_commit": target_config.get("git_commit", "local"),
        "artifacts": {
            "sql": sorted(_normalize_artifact_path(path) for path in pure_sql_results.values()),
            "dbt": sorted(
                _normalize_artifact_path(path)
                for key, path in dbt_results.items()
                if not key.startswith("_schema") and not key.startswith("_sources")
            ),
            "jobs": sorted(job_artifacts, key=lambda job: job["name"]),
            "bundle": "databricks.yml",
            "wheel": wheel_paths,
        },
    }
    manifest_path = target_root / "deploy_manifest.json"
    manifest_path.write_text(json.dumps(payload, indent=2))
    return manifest_path


def _build_target_bundle(target: str, config: dict, sql_mode: bool) -> dict[str, Path | list[str] | str]:
    target_root, target_config, profile = _prepare_target_project(target, config)
    wheel_paths = _build_and_stage_wheel(target_root)

    with _pushd(target_root):
        ddl_path = Path("dbt") / "ddl"

        pure_sql_results = compile_all_pure_sql(
            ddl_path,
            Path("sql"),
            catalog=target_config.get("catalog", "main"),
            schema=target_config.get("schema", "default"),
            compute_type=(target_config.get("compute", {}) or {}).get("type", "serverless"),
            platform=profile.get("platform", "databricks"),
            profile=profile,
            forge_config=target_config,
        )
        from forge.simple_ddl import compile_teardown_sql, compile_backup_sql
        compile_backup_sql(ddl_path, Path("sql"), profile=profile, forge_config=target_config)
        compile_teardown_sql(ddl_path, Path("sql"), profile=profile, forge_config=target_config)

        dbt_results = compile_all(ddl_path, Path("dbt") / "models", forge_config=target_config)

        graph = build_graph(target_config, dbt_project_dir=Path("dbt"), workflow_dir=Path("resources") / "jobs")
        workflows = build_domain_workflows(target_config, graph, sql_mode=sql_mode, sql_root=Path("sql"))
        job_artifacts: list[dict[str, str]] = []
        for wf in workflows:
            wf_path = Path("resources") / "jobs" / f"{wf.name}.yml"
            wf_path.parent.mkdir(parents=True, exist_ok=True)
            wf_path.write_text(wf.to_databricks_yml())
            job_artifacts.append({
                "name": wf.name,
                "file": str(wf_path),
            })

        bundle_yml = generate_bundle_config(target_config, sql_mode=sql_mode, prebuilt_wheel=True)
        bundle_path = Path("databricks.yml")
        bundle_path.write_text(bundle_yml)

        manifest_path = _write_deploy_manifest(
            target_root,
            target=target,
            target_config=target_config,
            pure_sql_results=pure_sql_results,
            dbt_results=dbt_results,
            job_artifacts=job_artifacts,
            wheel_paths=wheel_paths,
        )

    return {
        "target_root": target_root,
        "bundle_path": target_root / "databricks.yml",
        "manifest_path": manifest_path,
        "job_paths": [str(target_root / job["file"]) for job in job_artifacts],
    }


def _build_default_config(project_name: str, default_domain: str) -> dict:
    domain_name = default_domain.strip().lower().replace("-", "_") or "default"
    return {
        "name": project_name,
        "scope": "fd",
        "placements": {
            "families": {
                "domain": {
                    "allowed_classes": ["domain"],
                    "allowed_layers": ["bronze", "silver", "gold"],
                    "catalog_pattern": "{env}_{scope}_{layer}",
                    "schema_pattern": "{user}_{domain}",
                },
                "shared_meta": {
                    "allowed_classes": ["shared"],
                    "allowed_layers": ["meta"],
                    "catalog_pattern": "{env}_{scope}_meta",
                    "schema_pattern": "{user}_{namespace}",
                },
                "shared_ops": {
                    "allowed_classes": ["shared"],
                    "allowed_layers": ["operations"],
                    "catalog_pattern": "{env}_{scope}_ops",
                    "schema_pattern": "{user}_{namespace}",
                },
                "system_meta": {
                    "allowed_classes": ["system"],
                    "allowed_layers": ["meta"],
                    "catalog_pattern": "{env}_{scope}_meta",
                    "schema_pattern": "{user}_{system}",
                },
                "system_ops": {
                    "allowed_classes": ["system"],
                    "allowed_layers": ["operations"],
                    "catalog_pattern": "{env}_{scope}_ops",
                    "schema_pattern": "{user}_{system}",
                },
            }
        },
        "domains": {
            domain_name: {
                "placement_family": "domain",
            }
        },
        "shared_namespaces": {
            "config": {
                "placement_family": "shared_meta",
                "layer": "meta",
            },
            "metadata": {
                "placement_family": "shared_meta",
                "layer": "meta",
            },
            "operations": {
                "placement_family": "shared_ops",
                "layer": "operations",
            },
        },
        "system_assets": {
            "lineage": {
                "enabled": True,
                "placement_family": "system_meta",
                "layer": "meta",
                "system": "lineage",
            },
            "quarantine": {
                "enabled": True,
                "placement_family": "system_meta",
                "layer": "meta",
                "system": "quarantine",
            },
            "backups": {
                "enabled": True,
                "placement_family": "system_ops",
                "layer": "operations",
                "system": "backups",
            },
            "deployment_state": {
                "enabled": True,
                "placement_family": "system_ops",
                "layer": "operations",
                "system": "deploy",
            },
        },
        "profiles": {
            "dev": {
                "platform": "databricks",
                "databricks_profile": "DEFAULT",
                "env": "dev",
                "catalog": "main",
                "schema": "silver",
                "compute": {"type": "serverless", "auto_scale": True},
            },
            "prod": {
                "platform": "databricks",
                "databricks_profile": "PROD",
                "env": "prd",
                "catalog": "main",
                "schema": "silver",
                "compute": {"type": "serverless"},
            },
            "local": {
                "platform": "postgres",
                "connection": {
                    "host": "localhost",
                    "port": 5432,
                    "database": project_name,
                },
                "schema": domain_name,
                "catalog": "public",
                "overrides": {
                    "placement_families": {
                        "domain": {"catalog": "public"},
                        "shared_meta": {"catalog": "public"},
                        "shared_ops": {"catalog": "public"},
                        "system_meta": {"catalog": "public"},
                        "system_ops": {"catalog": "public"},
                    }
                },
                "compute": {"type": "local"},
            },
        },
        "dbt": {"version": "1.8.0"},
        "features": {
            "graph": True,
            "quarantine": True,
            "validation_exceptions": True,
            "prior_version": True,
            "python_udfs": True,
        },
        "portability": {
            "avoid_databricks_only": True,
            "postgres_compatible": True,
        },
    }

# =============================================
# SETUP – ludicrously simple
# =============================================
@app.command()
def setup(
    project_name: Optional[str] = typer.Option(None, "--name", help="Project name (defaults to folder name)"),
    domain: str = typer.Option("sales", "--domain", help="Default domain scaffolded under dbt/ddl/domain/<domain>/..."),
):
    """forge setup → creates forge.yml + folders in 2 seconds"""
    typer.echo("🚀 Running forge setup... (child-level simple)")

    project_root = Path.cwd()
    resolved_project_name = project_name or project_root.name
    if not CONFIG_FILE.exists():
        config = _build_default_config(resolved_project_name, domain)

        CONFIG_FILE.write_text(yaml.dump(config, sort_keys=False, default_flow_style=False))
        typer.echo(f"✅ Created {CONFIG_FILE} – this is the ONLY file you ever edit!")
    else:
        config = yaml.safe_load(CONFIG_FILE.read_text()) or {}

    domains = config.get("domains", {}) if isinstance(config.get("domains", {}), dict) else {}
    default_domain = next(iter(domains), domain.strip().lower().replace("-", "_") or "sales")

    # Create canonical v1 DDL tree plus generated-output folders.
    (project_root / "dbt" / "models").mkdir(parents=True, exist_ok=True)
    domain_root = project_root / "dbt" / "ddl" / "domain" / default_domain
    for relative_path in (
        Path("bronze/models"),
        Path("bronze/volumes"),
        Path("silver/models"),
        Path("silver/udfs"),
        Path("gold/models"),
    ):
        (domain_root / relative_path).mkdir(parents=True, exist_ok=True)

    shared_root = project_root / "dbt" / "ddl" / "shared"
    for relative_path in (
        Path("meta/seeds"),
        Path("meta/models"),
        Path("operations/models"),
    ):
        (shared_root / relative_path).mkdir(parents=True, exist_ok=True)

    (project_root / PROJECT_PYTHON_DIR).mkdir(parents=True, exist_ok=True)
    (project_root / DEFAULT_SDK_OUTPUT.parent).mkdir(parents=True, exist_ok=True)
    (project_root / "artifacts").mkdir(parents=True, exist_ok=True)

    typer.echo(
        f"✅ Created dbt/ddl/domain/{default_domain}/{{bronze,silver,gold}}/... + dbt/ddl/shared/{{meta,operations}}/..."
    )

    # Auto-generate profiles.yml from forge.yml
    profiles_path = project_root / "profiles.yml"
    if not profiles_path.exists():
        generate_profiles_yml(config, output_path=profiles_path)
        typer.echo("✅ Generated profiles.yml from forge.yml profiles")

    typer.echo("🔗 Core macros (quarantine, lineage, python_udf, prior_version) are bundled in this project under macros/")
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
    env: Optional[str] = typer.Option(None, "--env", help="Target/profile to deploy"),
    profile: Optional[str] = typer.Option(None, "--profile", "-p", help="Forge profile (from forge.yml profiles:)"),
    local: bool = typer.Option(False, "--local", help="Run setup/dbt locally instead of uploading as DAB tasks"),
    sql: bool = typer.Option(True, "--sql/--no-sql", help="Use sql_task (pure SQL) instead of dbt_task. Default: on. --no-sql to use dbt_task."),
):
    """forge deploy → reads forge.yml → auto-generates DAB → runs dbt

    Default: generates workflow with setup/run tasks and deploys via DAB.
    --local: runs setup and dbt locally from CLI, then deploys bundle.
    """
    if not CONFIG_FILE.exists():
        typer.echo("❌ No forge.yml found. Run 'forge setup' first!")
        raise typer.Exit(1)

    config = yaml.safe_load(CONFIG_FILE.read_text())
    selected_profile = _require_profile_name(config, command_name="forge deploy", profile=profile, target=env)
    typer.echo(f"🚀 Deploying to {selected_profile}...")
    prof = resolve_profile(config, profile_name=selected_profile)
    conn = resolve_connection(prof)

    typer.echo(f"  📋 Profile: {prof['_name']} ({conn.platform})")
    typer.echo(f"  ✅ Resolved compute: {conn.compute_type}")
    if conn.host:
        typer.echo(f"  🔗 Host: {conn.host}")

    dbt_env = _build_dbt_env(conn)

    # Auto-regenerate profiles.yml so dbt uses current forge.yml settings
    generate_profiles_yml(config, output_path=Path("profiles.yml"))

    schema_vars, dbt_vars_flag = _build_dbt_runtime_vars(config, prof, conn)

    _verify_required_catalogs(conn, prof, schema_vars)

    target_name = prof["_name"]
    typer.echo(f"📦 Building staged target bundle for {target_name}...")
    try:
        build_result = _build_target_bundle(target_name, config, sql_mode=sql)
        typer.echo(f"  ✅ Built target bundle → {build_result['target_root']}")
    except subprocess.CalledProcessError as exc:
        typer.echo(f"❌ Target bundle build failed while running: {' '.join(exc.cmd) if exc.cmd else 'subprocess'}")
        raise typer.Exit(1)
    except Exception as exc:
        typer.echo(f"❌ Target bundle build failed: {exc}")
        raise typer.Exit(1)

    if local:
        _run_local_staged_deploy(
            build_result,
            schema_vars=schema_vars,
            prof=prof,
            config=config,
            dbt_vars_flag=dbt_vars_flag,
            dbt_env=dbt_env,
        )

    try:
        _deploy_staged_bundle(build_result["target_root"], target_name)
    except Exception as exc:
        typer.echo(f"  ⚠️  Bundle deploy skipped: {exc}")

    typer.echo(f"🎉 Deploy complete! Run 'forge diff --profile {target_name}' to see graph changes.")


@app.command()
def build(
    target: Optional[str] = typer.Option(None, "--target", "-t", help="Profile/target name to render under artifacts/targets/<target>"),
    all_targets: bool = typer.Option(False, "--all-targets", help="Render one staged deployment bundle per configured profile"),
    sql: bool = typer.Option(True, "--sql/--no-sql", help="Use sql_task (pure SQL) instead of dbt_task. Default: on."),
):
    """forge build → render a target-scoped deployment bundle under artifacts/targets/<target>"""
    if not CONFIG_FILE.exists():
        typer.echo("❌ No forge.yml found. Run 'forge setup' first!")
        raise typer.Exit(1)

    config = yaml.safe_load(CONFIG_FILE.read_text())
    profiles = config.get("profiles", {})

    if all_targets and target:
        typer.echo("❌ Use either --target or --all-targets, not both.")
        raise typer.Exit(1)

    selected_targets = [target] if target else ([*profiles.keys()] if all_targets else [])
    if not selected_targets:
        typer.echo("❌ forge build requires an explicit target/profile.")
        typer.echo("   Specify --target <name> or use --all-targets.")
        if profiles:
            typer.echo(f"   Available profiles: {', '.join(profiles.keys())}")
        raise typer.Exit(1)

    for target_name in selected_targets:
        if target_name not in profiles:
            typer.echo(f"❌ Unknown target/profile '{target_name}'.")
            raise typer.Exit(1)

        typer.echo(f"🏗️  Building target bundle for {target_name}...")
        try:
            result = _build_target_bundle(target_name, config, sql_mode=sql)
        except subprocess.CalledProcessError as exc:
            typer.echo(f"❌ Build failed while running: {' '.join(exc.cmd) if exc.cmd else 'subprocess'}")
            raise typer.Exit(1)
        except Exception as exc:
            typer.echo(f"❌ Build failed for {target_name}: {exc}")
            raise typer.Exit(1)

        typer.echo(f"  ✅ Target root → {result['target_root']}")
        typer.echo(f"  📦 Bundle config → {result['bundle_path']}")
        typer.echo(f"  🧾 Deploy manifest → {result['manifest_path']}")

    typer.echo("🎉 Target bundle build complete.")

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
    selected_profile = profile or resolve_profile(config).get("_name")
    workflow_dir = None
    if selected_profile:
        config, _ = _materialize_profile_config(config, selected_profile)
        workflow_dir = _default_workflow_output_dir(selected_profile)

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

    graph = build_graph(config, workflow_dir=workflow_dir)
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
    selected_profile = profile or resolve_profile(config).get("_name")
    workflow_dir = None
    if selected_profile:
        config, _ = _materialize_profile_config(config, selected_profile)
        workflow_dir = _default_workflow_output_dir(selected_profile)

    graph = build_graph(config, workflow_dir=workflow_dir)
    plan = build_teardown_plan(config, graph, wipe_all=wipe_all)

    # Always write the plan YAML for audit
    plan_path = Path(output or f"local/teardown/{plan.name}.yml")
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
def graph(
    profile: Optional[str] = typer.Option(None, "--profile", "-p", help="Forge profile (from forge.yml profiles:)"),
    direction: str = typer.Option("LR", "--direction", help="Mermaid direction: LR, RL, TB, or BT"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Write Mermaid graph to file"),
):
    """forge graph → renders the current asset graph as Mermaid from source code"""
    if not CONFIG_FILE.exists():
        typer.echo("❌ No forge.yml found. Run 'forge setup' first!")
        raise typer.Exit(1)

    config = yaml.safe_load(CONFIG_FILE.read_text())
    selected_profile = profile or resolve_profile(config).get("_name")
    workflow_dir = None
    if selected_profile:
        selected_profile = _require_profile_name(config, command_name="forge graph", profile=selected_profile)
        config, _ = _materialize_profile_config(config, selected_profile)
        workflow_dir = _default_workflow_output_dir(selected_profile)

    direction_value = direction.upper()
    if direction_value not in {"LR", "RL", "TB", "BT"}:
        typer.echo("❌ --direction must be one of: LR, RL, TB, BT")
        raise typer.Exit(1)

    diagram = render_mermaid(build_graph(config, workflow_dir=workflow_dir), direction=direction_value)
    result = _render_mermaid_markdown([("Asset Graph", diagram)]) if _is_markdown_output(output) else diagram

    if output:
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        Path(output).write_text(result)
        typer.echo(f"✅ Mermaid graph → {output}")
    else:
        typer.echo(diagram)


@app.command()
def diff(
    profile: Optional[str] = typer.Option(None, "--profile", "-p", help="Forge profile (from forge.yml profiles:)"),
    mermaid: bool = typer.Option(False, "--mermaid", help="Output Mermaid diff diagram"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Write diff output to file"),
):
    """forge diff → shows what changed since last graph snapshot"""
    if not CONFIG_FILE.exists():
        typer.echo("❌ No forge.yml found. Run 'forge setup' first!")
        raise typer.Exit(1)

    config = yaml.safe_load(CONFIG_FILE.read_text())
    selected_profile = _require_profile_name(config, command_name="forge diff", profile=profile)
    config, _ = _materialize_profile_config(config, selected_profile)
    snapshot_path = _graph_snapshot_path(selected_profile)
    legacy_snapshot_path = _graph_snapshot_path()

    workflow_dir = _default_workflow_output_dir(selected_profile)
    new_graph = build_graph(config, workflow_dir=workflow_dir)
    old_graph = load_graph(snapshot_path)

    if old_graph is None and legacy_snapshot_path != snapshot_path:
        old_graph = load_graph(legacy_snapshot_path)
        if old_graph is not None:
            typer.echo(f"📦 Using legacy graph snapshot → {legacy_snapshot_path}")

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
            content = _render_mermaid_markdown([("Graph Diff", diagram)]) if _is_markdown_output(output) else diagram
            Path(output).write_text(content)
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
    profile: Optional[str] = typer.Option(None, "--profile", "-p", help="Forge profile (from forge.yml profiles:)"),
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
        typer.echo(f"❌ {ddl} not found. Create the canonical dbt/ddl/ tree first.")
        raise typer.Exit(1)

    if not CONFIG_FILE.exists():
        typer.echo("\u274c No forge.yml found. Run 'forge setup' first!")
        raise typer.Exit(1)

    config = yaml.safe_load(CONFIG_FILE.read_text())
    selected_profile = profile or resolve_profile(config).get("_name")
    workflow_dir = None
    if selected_profile:
        selected_profile = _require_profile_name(config, command_name="forge explain", profile=selected_profile)
        config, _ = _materialize_profile_config(config, selected_profile)
        workflow_dir = _default_workflow_output_dir(selected_profile)

    graph = build_graph(config, dbt_project_dir=dbt_dir, workflow_dir=workflow_dir)
    tree = walk_column_lineage(graph, model_name, column_name, dbt_dir, forge_config=config)

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
        diagram = render_provenance_tree(tree, mermaid=True)
        result = _render_mermaid_markdown([(f"Column Lineage: {model_dot_column}", diagram)]) if _is_markdown_output(output) else diagram
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
    output: str = typer.Option(DEFAULT_SDK_OUTPUT.as_posix(), "--output", "-o", help="Output file path"),
    check: bool = typer.Option(False, "--check", help="CI mode: fail if generated file is stale"),
    profile: str | None = typer.Option(None, "--profile", help="Forge profile to use for generated metadata discovery"),
):
    """forge codegen → generates type-safe Pydantic models from dbt schema"""
    if not CONFIG_FILE.exists():
        typer.echo("❌ No forge.yml found. Run 'forge setup' first!")
        raise typer.Exit(1)

    config = yaml.safe_load(CONFIG_FILE.read_text())
    schema = config.get("schema", "default")
    output_path = Path(output)
    selected_profile = _require_profile_name(config, command_name="forge codegen", profile=profile)

    # Read existing content for --check comparison
    old_content = output_path.read_text() if output_path.exists() else None

    out = generate_sdk_file(schema=schema, output_path=output_path, profile_name=selected_profile)
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
        typer.echo(f"   Generated models now live under {DEFAULT_SDK_OUTPUT.parent.as_posix()}/, separate from custom Python in {PROJECT_PYTHON_DIR.as_posix()}/.")
        typer.echo("   Wrong types → Pydantic raises immediately. No bad inserts.")

# =============================================
# WORKFLOW – DAG orchestration
# =============================================
@app.command()
def workflow(
    mermaid: bool = typer.Option(False, "--mermaid", help="Output Mermaid diagram"),
    dab: bool = typer.Option(False, "--dab", help="Output databricks.yml jobs section"),
    sql: bool = typer.Option(True, "--sql/--no-sql", help="Use sql_task (pure SQL) instead of dbt_task. Default: on. --no-sql to use dbt_task."),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Write output to file (default: artifacts/targets/<profile>/resources/jobs/<name>.yml for --dab)"),
    profile: Optional[str] = typer.Option(None, "--profile", "-p", help="Forge profile (from forge.yml profiles:)"),
):
    """forge workflow → generates stage-based Databricks Workflow DAG"""
    if not CONFIG_FILE.exists():
        typer.echo("❌ No forge.yml found. Run 'forge setup' first!")
        raise typer.Exit(1)

    config = yaml.safe_load(CONFIG_FILE.read_text())
    selected_profile = _require_profile_name(config, command_name="forge workflow", profile=profile)
    config, _ = _materialize_profile_config(config, selected_profile)

    workflow_dir = _default_workflow_output_dir(selected_profile)
    graph = build_graph(config, workflow_dir=workflow_dir)
    workflows = build_domain_workflows(config, graph, sql_mode=sql, sql_root=_default_pure_sql_output(selected_profile))

    if mermaid:
        diagram = render_workflows_mermaid(workflows)
        result = _render_mermaid_markdown([("Pipeline Workflow", diagram)]) if _is_markdown_output(output) else diagram
    elif dab:
        result = "\n---\n".join(wf.to_databricks_yml() for wf in workflows)
    else:
        result = "\n---\n".join(
            yaml.dump(wf.to_dict(), sort_keys=False, default_flow_style=False)
            for wf in workflows
        )

    # Default output path for --dab mode: write each workflow to its own file
    if dab and not output:
        workflow_out_dir = _default_workflow_output_dir(selected_profile)
        for wf in workflows:
            wf_out = workflow_out_dir / f"{wf.name}.yml"
            wf_out.parent.mkdir(parents=True, exist_ok=True)
            wf_out.write_text(wf.to_databricks_yml())
            typer.echo(f"✅ Workflow written to {wf_out}")
    elif output:
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        Path(output).write_text(result)
        typer.echo(f"✅ Workflow written to {output}")
    else:
        typer.echo(result)

    for wf in workflows:
        stages_used = [t.stage for t in wf.tasks]
        stage_summary = " → ".join(dict.fromkeys(stages_used))
        typer.echo(f"\n📊 {wf.name}: {stage_summary} ({len(wf.tasks)} tasks)")
        for task in wf.tasks:
            deps = f" ← {', '.join(task.depends_on)}" if task.depends_on else ""
            typer.echo(f"  [{task.stage}] {task.name}{deps}")


@app.command("visual-docs")
def visual_docs(
    profile: Optional[str] = typer.Option(None, "--profile", "-p", help="Forge profile (from forge.yml profiles:)"),
    column: str = typer.Option("customer_summary.total_revenue", "--column", help="Model.column to render for the column lineage doc"),
    output_dir: str = typer.Option("docs", "--output-dir", help="Directory where visual documentation markdown files are written"),
):
    """forge visual-docs → regenerates all Mermaid markdown docs in one command"""
    if not CONFIG_FILE.exists():
        typer.echo("❌ No forge.yml found. Run 'forge setup' first!")
        raise typer.Exit(1)

    config = yaml.safe_load(CONFIG_FILE.read_text())
    selected_profile = _require_profile_name(config, command_name="forge visual-docs", profile=profile)
    config, _ = _materialize_profile_config(config, selected_profile)

    parts = column.split(".", 1)
    if len(parts) != 2:
        typer.echo("❌ --column must be in the form <model_name>.<column_name>")
        raise typer.Exit(1)
    model_name, column_name = parts

    docs_dir = Path(output_dir)
    docs_dir.mkdir(parents=True, exist_ok=True)

    pipeline_path = docs_dir / "pipeline.md"
    table_lineage_path = docs_dir / "table-lineage.md"
    safe_column_name = column.replace(".", "-")
    default_column = "customer_summary.total_revenue"
    column_lineage_filename = "customer-summary-lineage.md" if column == default_column else f"{safe_column_name}-lineage.md"
    column_lineage_path = docs_dir / column_lineage_filename
    visual_doc_path = docs_dir / "VISUAL_DOCUMENTATION.md"

    workflow_dir = _default_workflow_output_dir(selected_profile)
    graph = build_graph(config, workflow_dir=workflow_dir)
    workflows = build_domain_workflows(config, graph, sql_mode=True, sql_root=_default_pure_sql_output(selected_profile))
    pipeline_markdown = _render_mermaid_markdown([("Pipeline Workflow", render_workflows_mermaid(workflows))])
    table_lineage_markdown = _render_mermaid_markdown([
        ("Current Table Relationships", render_current_table_relationships_mermaid(graph))
    ])

    ddl_path = _resolve_ddl(DDL_DEFAULT)
    dbt_dir = ddl_path.parent if ddl_path.is_file() else ddl_path.parent
    tree = walk_column_lineage(graph, model_name, column_name, dbt_dir, forge_config=config)
    if "error" in tree:
        typer.echo(f"❌ {tree['error']}")
        raise typer.Exit(1)
    column_lineage_markdown = _render_mermaid_markdown([
        (f"Column Lineage: {column}", render_provenance_tree(tree, mermaid=True))
    ])

    pipeline_path.write_text(pipeline_markdown)
    table_lineage_path.write_text(table_lineage_markdown)
    column_lineage_path.write_text(column_lineage_markdown)
    visual_doc_path.write_text(
        _build_visual_documentation_markdown(
            pipeline_markdown,
            table_lineage_markdown,
            column_lineage_markdown,
            model_dot_column=column,
        )
    )

    typer.echo(f"✅ Pipeline workflow → {pipeline_path}")
    typer.echo(f"✅ Table lineage → {table_lineage_path}")
    typer.echo(f"✅ Column lineage → {column_lineage_path}")
    typer.echo(f"✅ Visual documentation → {visual_doc_path}")


# =============================================
# PYTHON-TASK – scaffold + manage Python tasks
# =============================================
@app.command("python-task")
def python_task(
    name: str = typer.Argument(..., help="Name for the Python task (e.g. enrich_customers)"),
    stage: str = typer.Option("enrich", "--stage", "-s", help="Pipeline stage: ingest/stage/clean/enrich/serve"),
    output: str = typer.Option(PROJECT_PYTHON_DIR.as_posix(), "--output", "-o", help="Output directory for task file"),
    description: str = typer.Option("", "--desc", help="Short description of the task"),
    template: str = typer.Option("default", "--template", "-t", help="Task template: default, ingest"),
):
    """forge python-task <name> → scaffolds a Python task file + registers in forge.yml"""
    from forge.python_task import scaffold_python_task

    out = scaffold_python_task(
        name=name,
        output_dir=Path(output),
        stage=stage,
        description=description,
        template=template,
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
# COMPILE – dbt/ddl → SQL (no coding needed)
# =============================================
@app.command()
def compile(
    ddl: str = typer.Option(DDL_DEFAULT, "--ddl", help="Path to DDL file or directory"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output directory for generated artifacts"),
    pure_sql: bool = typer.Option(False, "--pure-sql", help="Emit standalone SQL (no dbt/Jinja). Runs directly on SQL warehouse."),
    profile: Optional[str] = typer.Option(None, "--profile", "-p", help="Forge profile (from forge.yml profiles:)"),
):
    """forge compile → turns dbt/ddl into SQL + schema.yml (no SQL needed)"""
    ddl_path = _resolve_ddl(ddl)
    if not ddl_path.exists():
        typer.echo(f"❌ {ddl} not found. Create the canonical dbt/ddl/ tree first.")
        raise typer.Exit(1)

    if not CONFIG_FILE.exists():
        typer.echo("❌ No forge.yml found. Run 'forge setup' first!")
        raise typer.Exit(1)

    config = yaml.safe_load(CONFIG_FILE.read_text())
    selected_profile = _require_profile_name(config, command_name="forge compile", profile=profile)
    config, prof = _materialize_profile_config(config, selected_profile)

    if pure_sql:
        # Pure-SQL mode: no Jinja, no dbt, runs on any SQL warehouse
        catalog = prof.get("catalog", "main")
        schema = prof.get("schema", "silver")
        compute_type = prof.get("compute", {}).get("type", "serverless") if isinstance(prof.get("compute"), dict) else "serverless"
        platform = prof.get("platform", "databricks")

        sql_dir = Path(output) if output else _default_pure_sql_output(selected_profile)
        results = compile_all_pure_sql(
            ddl_path, sql_dir,
            catalog=catalog, schema=schema,
            compute_type=compute_type, platform=platform,
            profile=prof, forge_config=config,
        )

        # Teardown SQL (drops all tables, views, UDFs)
        from forge.simple_ddl import compile_teardown_sql, compile_backup_sql
        compile_backup_sql(ddl_path, sql_dir, profile=prof, forge_config=config)
        compile_teardown_sql(ddl_path, sql_dir, profile=prof, forge_config=config)

        for name, path in results.items():
            if name == "_udfs":
                typer.echo(f"  🔧 {path.relative_to(sql_dir.parent)} → UDF definitions + lineage UDFs")
            elif name == "_lineage_graph":
                typer.echo(f"  🔗 {path.relative_to(sql_dir.parent)} → lineage graph + log tables")
            elif name.startswith("_setup_"):
                typer.echo(f"  🏗️  {path.relative_to(sql_dir.parent)} → setup table definition")
            elif name.endswith("_quarantine"):
                typer.echo(f"  🔶 {path.relative_to(sql_dir.parent)} → quarantine sidecar")
            else:
                typer.echo(f"  ✅ {path.relative_to(sql_dir.parent)}")

        model_count = len([k for k in results if not k.startswith("_") and not k.endswith("_quarantine")])
        typer.echo(f"")
        typer.echo(f"🎉 Compiled {model_count} models → {sql_dir}")
        typer.echo("   SQL is grouped by phase: setup/, process/, teardown/.")
        return

    # Standard dbt mode
    output_dir = Path(output) if output else _default_compile_output(selected_profile)
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
        extras.append(f"{udf_count} UDFs → {output_dir.parent / 'functions'}")
    if "_sources" in results:
        extras.append(f"sources → {output_dir.parent / 'sources'}")
    extra_note = f" + {', '.join(extras)}" if extras else ""
    typer.echo(f"🎉 Compiled {model_count} models{extra_note} from {ddl_path} → {output_dir}.")


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
    """forge migrate → applies migration YAMLs to dbt/ddl + recompiles"""
    ddl_path = _resolve_ddl(ddl)
    mig_dir = Path(migrations)

    if not ddl_path.exists():
        typer.echo(f"❌ {ddl} not found. Create the canonical dbt/ddl/ tree first.")
        raise typer.Exit(1)

    if not mig_dir.exists():
        typer.echo(f"📁 No migrations directory at {migrations}. Nothing to apply.")
        raise typer.Exit(0)

    if dry_run:
        config = yaml.safe_load(CONFIG_FILE.read_text()) if CONFIG_FILE.exists() else {}
        results = apply_all_migrations_dry_run(ddl_path, mig_dir, forge_config=config)
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

    config = yaml.safe_load(CONFIG_FILE.read_text()) if CONFIG_FILE.exists() else {}
    results = apply_all_migrations(ddl_path, mig_dir, forge_config=config)

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
@app.command("inspect")
def inspect_generated(
    profile: Optional[str] = typer.Option(None, "--profile", "-p", help="Forge profile (from forge.yml profiles:)"),
    ddl: str = typer.Option(DDL_DEFAULT, "--ddl", help="Path to DDL file or directory"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Write summary to file (default: artifacts/targets/<profile>/ddl_inspect.md)"),
    json_flag: bool = typer.Option(False, "--json", help="Emit JSON instead of markdown"),
):
    """forge inspect → show what dbt/ddl authors and what Forge generates from it"""
    if not CONFIG_FILE.exists():
        typer.echo("❌ No forge.yml found. Run 'forge setup' first!")
        raise typer.Exit(1)

    ddl_path = _resolve_ddl(ddl)
    if not ddl_path.exists():
        typer.echo(f"❌ {ddl} not found. Create the canonical dbt/ddl/ tree first.")
        raise typer.Exit(1)

    config = yaml.safe_load(CONFIG_FILE.read_text())
    selected_profile = _require_profile_name(config, command_name="forge inspect", profile=profile)
    config, _ = _materialize_profile_config(config, selected_profile)
    project_spec = load_project_spec(ddl_path, forge_config=config)
    summary = _expected_generated_paths(project_spec, config, selected_profile)

    if json_flag:
        result = json.dumps(
            {
                "project": config.get("name", "project"),
                "profile": selected_profile,
                "target_root": str(_target_artifact_root(selected_profile).relative_to(_PROJECT_ROOT)),
                **summary,
            },
            indent=2,
        )
        default_output = _target_artifact_root(selected_profile) / "ddl_inspect.json"
    else:
        result = _render_inspect_markdown(config=config, profile_name=selected_profile, summary=summary)
        default_output = _target_artifact_root(selected_profile) / "ddl_inspect.md"

    output_path = Path(output) if output else default_output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(result)
    typer.echo(f"✅ DDL inspection summary → {output_path}")
    typer.echo("   Use this to map authored dbt/ddl assets to generated dbt, SQL, workflow, and graph artifacts.")


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
# UDFS – manage SQL/Python UDFs from dbt/ddl
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

    config = yaml.safe_load(CONFIG_FILE.read_text()) if CONFIG_FILE.exists() else {}

    udf_defs = load_udfs(ddl_path, forge_config=config)
    if not udf_defs:
        typer.echo("📋 No UDFs defined. Add a udfs: block to your DDL to get started.")
        return

    compiled = compile_all_udfs(ddl_path, forge_config=config)

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
# VALIDATE – run checks defined in dbt/ddl
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

    config = yaml.safe_load(CONFIG_FILE.read_text()) if CONFIG_FILE.exists() else {}
    results = compile_checks_sql(ddl_path, model_filter=model, forge_config=config)

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

    target_root, _, _, _ = _compile_target_dbt_project("dev", config)
    typer.echo(f"  ✅ Compiled staged dev project → {target_root / 'dbt' / 'models'}")

    # Run dbt with schema override
    dbt_cmd = [
        "dbt", "run",
        "--project-dir", ".",
        "--profiles-dir", ".",
        "--target", "dev",
        "--vars", yaml.dump({"dev_schema": dev_schema}),
    ]

    with _pushd(target_root):
        if seed:
            typer.echo("  🌱 Seeding sample data...")
            try:
                subprocess.run(
                    ["dbt", "seed", "--project-dir", ".", "--profiles-dir", ".", "--target", "dev"],
                    check=True, capture_output=True,
                )
                typer.echo("  ✅ Seeds loaded")
            except (FileNotFoundError, subprocess.CalledProcessError) as e:
                typer.echo(f"  ⚠️  Seed failed (non-fatal): {e}")

        typer.echo("  ▶ Running dbt in the staged dev project...")
        try:
            subprocess.run(dbt_cmd, check=True)
            typer.echo("  ✅ Dev models materialized")
        except (FileNotFoundError, subprocess.CalledProcessError) as e:
            typer.echo(f"  ⚠️  dbt run failed (non-fatal): {e}")

    typer.echo(f"🎉 Dev environment ready! Schema: {dev_schema}")
    typer.echo(f"   Run 'forge dev' to start watch mode.")
    typer.echo(f"   Run 'forge dev-down' to tear down.")


@app.command(name="dev")
def dev(
    ddl: str = typer.Option(DDL_DEFAULT, "--ddl", help="Path to DDL file or directory"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output directory for generated SQL files"),
    poll_interval: float = typer.Option(1.0, "--poll", help="Seconds between file change polls"),
):
    """forge dev → watches DDL files, auto-compiles on save"""
    ddl_path = _resolve_ddl(ddl)
    if not ddl_path.exists():
        typer.echo(f"❌ {ddl} not found. Create the canonical dbt/ddl/ tree first.")
        raise typer.Exit(1)

    output_dir = Path(output) if output else (_default_compile_output("dev") if CONFIG_FILE.exists() else Path("dbt/models"))
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
        if not output and config and "dev" in config.get("profiles", {}):
            _, _, _, results = _compile_target_dbt_project("dev", config)
        else:
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
                    if not output and config and "dev" in config.get("profiles", {}):
                        _, _, _, results = _compile_target_dbt_project("dev", config)
                    else:
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
    default_profile = next(iter(forge_profiles), "(none)")

    if not forge_profiles:
        typer.echo("📋 No profiles defined in forge.yml (using legacy flat config).")
        typer.echo("   Add a profiles: block to enable multi-environment support.")
        return

    # Show ~/.databrickscfg profiles for context
    dbr_profiles = list_databrickscfg_profiles()
    if dbr_profiles:
        typer.echo(f"🔗 ~/.databrickscfg profiles: {', '.join(dbr_profiles)}")
        typer.echo("")

    typer.echo(f"📋 Forge profiles (default resolver order starts with: {default_profile}):\n")

    for name, prof in forge_profiles.items():
        platform = prof.get("platform", "databricks")
        dbr_ref = prof.get("databricks_profile", "")
        env_tag = prof.get("env", "")
        expanded = _expand_env_prefix(prof, config)
        catalog = expanded.get("catalog", config.get("catalog", "main"))
        schema = expanded.get("schema", config.get("schema", "silver"))
        compute = prof.get("compute", {})
        compute_type = compute.get("type", "serverless") if isinstance(compute, dict) else compute

        typer.echo(f"  · {name}")
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
# • All heavy lifting (compute, graph, macros) stays in the local Forge runtime and project macros
# • SQL preferred, Python UDFs allowed via macros
# • Portability baked in (target_platform switch works later)
# • Every single line has a comment → THE CODE IS THE DOCUMENTATION