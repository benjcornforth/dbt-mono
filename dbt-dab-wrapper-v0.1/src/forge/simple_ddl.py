# =============================================
# src/forge/simple_ddl.py
# =============================================
# THE CODE IS THE DOCUMENTATION
#
# A zero-SQL, zero-Python DDL for users who have
# never written code before.
#
# Users write ONE YAML file: dbt/models.yml
# The compiler generates all SQL models, schema.yml,
# and lineage configuration automatically.
#
# Example models.yml:
#
#   models:
#     stg_customers:
#       description: "Staged customers"
#       source: raw_customers
#       columns:
#         customer_id:  { type: int, required: true, unique: true }
#         email:        { type: string }
#         signup_date:  { type: date, cast: true }
#         revenue:      { type: decimal(10,2), cast: true }
#
#     customer_clean:
#       description: "Clean customers"
#       source: stg_customers
#       stage: clean
#       materialized: table
#       version: v2
#       quarantine: "email IS NULL OR revenue < 0"
#       columns:
#         customer_id:  { type: int, required: true, unique: true }
#         email:        { type: string, required: true }
#         revenue:      { type: decimal(10,2) }
#
#     customer_orders:
#       description: "Customers joined with orders"
#       sources:
#         c: customer_clean
#         o: stg_orders
#       join: "c.customer_id = o.customer_id"
#       materialized: table
#       columns:
#         customer_id:  { from: c }
#         email:        { from: c }
#         order_id:     { from: o, required: true, unique: true }
#         product:      { from: o }
#         line_total:   { from: o }
#
#     customer_summary:
#       description: "Customer metrics"
#       source: customer_orders
#       materialized: table
#       group_by: [customer_id, first_name, last_name, email, country]
#       columns:
#         customer_id:  { type: int, required: true, unique: true }
#         total_orders: { expr: "count(order_id)", type: int }
#         total_revenue:{ expr: "sum(line_total)", type: decimal(10,2) }
#         first_order:  { expr: "min(order_date)", type: date }
#         last_order:   { expr: "max(order_date)", type: date }
#
# Then:  forge compile   → generates all .sql + schema.yml
#        forge deploy    → runs everything
#
# No SQL. No Python. Just YAML.

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml


# =============================================
# DDL YAML PARSER
# =============================================

def load_ddl(ddl_path: Path) -> dict[str, dict]:
    """Load models.yml and return the models dict."""
    raw = yaml.safe_load(ddl_path.read_text())
    return raw.get("models", {})


def load_udfs(ddl_path: Path) -> dict[str, dict]:
    """Load models.yml and return the udfs dict (if any)."""
    raw = yaml.safe_load(ddl_path.read_text())
    return raw.get("udfs", {})


# =============================================
# SQL COMPILER
# =============================================

def _compile_column_select(col_name: str, col_def: dict, alias: str | None = None) -> str:
    """
    Compile a column definition into a SQL SELECT expression.

    Rules:
      expr  → use as-is (aggregation / calculation)
      cast  → wrap in CAST(col AS type)
      from  → prefix with alias (c.col_name)
      else  → plain column name
    """
    # UDF call: { udf: "loyalty_tier(total_revenue)" }
    if "udf" in col_def:
        udf_call = col_def["udf"]
        return f"    {udf_call} as {col_name}"

    # Explicit expression (aggregation, calculation)
    if "expr" in col_def:
        expr = col_def["expr"]
        return f"    {expr} as {col_name}"

    # Column from a specific join alias
    source_alias = col_def.get("from", alias)
    raw_col = col_def.get("source_column", col_name)

    if source_alias:
        ref = f"{source_alias}.{raw_col}"
    else:
        ref = raw_col

    # Type casting
    if col_def.get("cast") and "type" in col_def:
        sql_type = col_def["type"]
        return f"    cast({ref} as {sql_type}) as {col_name}"

    # Rename (source column differs from output name)
    if raw_col != col_name:
        return f"    {ref} as {col_name}"

    return f"    {ref}"


def compile_model(model_name: str, model_def: dict) -> str:
    """Compile a single model definition into a dbt SQL file."""
    lines: list[str] = []

    # Config block
    materialized = model_def.get("materialized", "view")
    version = model_def.get("version")
    quarantine = model_def.get("quarantine")

    config_parts = [f"materialized='{materialized}'"]
    if version:
        config_parts.append(f"meta={{'version': '{version}'}}")
    if quarantine:
        config_parts.append(
            f'post_hook="{{{{ dbt_dab_tools.quarantine(\'{quarantine}\') }}}}"'
        )

    lines.append("{{ config(")
    lines.append(f"    {', '.join(config_parts)}")
    lines.append(") }}")
    lines.append("")

    # SELECT columns
    columns = model_def.get("columns", {})
    is_join = "sources" in model_def or "join" in model_def
    is_agg = "group_by" in model_def

    # Determine default alias for single-source
    default_alias = None
    if not is_join:
        default_alias = None

    col_lines = []
    for col_name, col_def in columns.items():
        if isinstance(col_def, str):
            # Shorthand: "customer_id: int"
            col_def = {"type": col_def}
        elif col_def is None:
            col_def = {}
        col_lines.append(_compile_column_select(col_name, col_def, default_alias))

    # Add lineage columns
    # Build verbose lineage entries for non-passthrough columns
    verbose_cols = []
    for col_name, col_def in columns.items():
        if isinstance(col_def, str):
            col_def = {"type": col_def}
        elif col_def is None:
            col_def = {}

        entry: dict[str, Any] = {"name": col_name}

        if "udf" in col_def:
            entry["expr"] = col_def["udf"]
            entry["op"] = "UDF"
        elif "expr" in col_def:
            entry["expr"] = col_def["expr"]
            # Detect op type from expression
            expr_lower = col_def["expr"].lower().strip()
            if any(expr_lower.startswith(fn) for fn in ["count(", "sum(", "avg(", "min(", "max("]):
                entry["op"] = "AGGREGATION"
            elif expr_lower.startswith("case "):
                entry["op"] = "CASE"
            else:
                entry["op"] = "EXPRESSION"
        elif col_def.get("cast"):
            entry["expr"] = f"cast({col_name} as {col_def.get('type', 'string')})"
            entry["op"] = "CAST"
            entry["inputs"] = [col_name]

        if entry.get("op"):
            verbose_cols.append(entry)

    if verbose_cols:
        lineage_arg = f"columns={verbose_cols}"
        col_lines.append(f"    {{{{ dbt_dab_tools.lineage_columns({lineage_arg}) }}}}")
    else:
        col_lines.append("    {{ dbt_dab_tools.lineage_columns() }}")

    lines.append("select")
    lines.append(",\n".join(col_lines))

    # FROM clause
    if is_join:
        sources = model_def.get("sources", {})
        join_condition = model_def.get("join", "")
        join_type = model_def.get("join_type", "inner join")

        source_items = list(sources.items())
        first_alias, first_source = source_items[0]
        lines.append(f"from {{{{ ref('{first_source}') }}}} {first_alias}")

        for alias, source in source_items[1:]:
            lines.append(f"{join_type} {{{{ ref('{source}') }}}} {alias}")
            lines.append(f"    on {join_condition}")
    else:
        source = model_def.get("source", "")
        if source:
            lines.append(f"from {{{{ ref('{source}') }}}}")

    # GROUP BY
    if is_agg:
        group_cols = model_def.get("group_by", [])
        if group_cols:
            lines.append(f"group by")
            lines.append(f"    {', '.join(group_cols)}")

    return "\n".join(lines) + "\n"


def compile_schema_yml(models: dict[str, dict]) -> str:
    """Compile the DDL into a dbt schema.yml."""
    schema: dict[str, Any] = {"version": 2, "models": []}

    for model_name, model_def in models.items():
        model_entry: dict[str, Any] = {"name": model_name}

        if "description" in model_def:
            model_entry["description"] = model_def["description"]

        cols_list = []
        for col_name, col_def in model_def.get("columns", {}).items():
            if isinstance(col_def, str):
                col_def = {"type": col_def}
            elif col_def is None:
                col_def = {}

            col_entry: dict[str, Any] = {"name": col_name}

            if "description" in col_def:
                col_entry["description"] = col_def["description"]

            if "type" in col_def:
                col_entry["data_type"] = col_def["type"]

            tests = []
            if col_def.get("required"):
                tests.append("not_null")
            if col_def.get("unique"):
                tests.append("unique")
            if col_def.get("accepted_values"):
                tests.append({"accepted_values": {"values": col_def["accepted_values"]}})
            if tests:
                col_entry["data_tests"] = tests

            cols_list.append(col_entry)

        if cols_list:
            model_entry["columns"] = cols_list

        schema["models"].append(model_entry)

    return yaml.dump(schema, sort_keys=False, default_flow_style=False)


# =============================================
# UDF COMPILER: udfs: block → CREATE FUNCTION SQL
# =============================================
# UDFs are defined in the same models.yml:
#
#   udfs:
#     loyalty_tier:
#       language: sql
#       returns: string
#       params: [{ name: revenue, type: "decimal(18,2)" }]
#       body: |
#         CASE
#           WHEN revenue >= 1000 THEN 'GOLD'
#           WHEN revenue >=  500 THEN 'SILVER'
#           ELSE 'BRONZE'
#         END
#
# Then reference in a column:
#   tier: { udf: "loyalty_tier(total_revenue)", description: "..." }
#
# forge compile → also writes _udfs.sql
# forge udfs    → shows defined UDFs
# Graph:          → adds purple UDF nodes

def compile_udf_sql(udf_name: str, udf_def: dict, schema: str = "{{ target.schema }}") -> str:
    """
    Compile a single UDF definition into a CREATE FUNCTION SQL statement.

    Supports both SQL and Python UDFs on Databricks.
    """
    language = udf_def.get("language", "sql").upper()
    returns = udf_def.get("returns", "STRING")
    params = udf_def.get("params", [])
    body = udf_def.get("body", "NULL").strip()
    comment = udf_def.get("description", "")

    # Build parameter list
    param_parts = []
    for p in params:
        if isinstance(p, dict):
            param_parts.append(f"{p['name']} {p['type']}")
        elif isinstance(p, str):
            # Shorthand "name: type"
            if ":" in p:
                pname, ptype = p.split(":", 1)
                param_parts.append(f"{pname.strip()} {ptype.strip()}")
            else:
                param_parts.append(f"{p} STRING")
    param_str = ", ".join(param_parts)

    lines: list[str] = []
    lines.append(f"-- UDF: {udf_name}")
    if comment:
        lines.append(f"-- {comment}")
    lines.append(f"CREATE OR REPLACE FUNCTION {schema}.{udf_name}({param_str})")
    lines.append(f"RETURNS {returns}")

    if language == "SQL":
        lines.append(f"RETURN ({body});")
    elif language == "PYTHON":
        lines.append(f"LANGUAGE PYTHON")
        lines.append(f"AS $$")
        lines.append(body)
        lines.append(f"$$;")
    else:
        lines.append(f"LANGUAGE {language}")
        lines.append(f"AS $$")
        lines.append(body)
        lines.append(f"$$;")

    return "\n".join(lines)


def compile_all_udfs(ddl_path: Path) -> dict[str, str]:
    """
    Compile all UDFs from models.yml.

    Returns: {udf_name: sql_statement}
    """
    udfs = load_udfs(ddl_path)
    results: dict[str, str] = {}
    for udf_name, udf_def in udfs.items():
        results[udf_name] = compile_udf_sql(udf_name, udf_def)
    return results


# =============================================
# FULL COMPILE: models.yml → .sql + schema.yml
# =============================================

def compile_all(
    ddl_path: Path,
    output_dir: Path,
    schema_output: Path | None = None,
) -> dict[str, Path]:
    """
    Compile models.yml into individual .sql files + schema.yml + UDFs.

    Returns a dict of {model_name: output_path} for all generated files.
    """
    models = load_ddl(ddl_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    results: dict[str, Path] = {}

    for model_name, model_def in models.items():
        sql = compile_model(model_name, model_def)
        out_path = output_dir / f"{model_name}.sql"
        out_path.write_text(sql)
        results[model_name] = out_path

    # Generate schema.yml
    schema_yml = compile_schema_yml(models)
    schema_path = schema_output or (output_dir / "schema.yml")
    schema_path.write_text(schema_yml)
    results["_schema"] = schema_path

    # Generate UDFs SQL (if any defined)
    udfs = compile_all_udfs(ddl_path)
    if udfs:
        udf_lines = []
        for udf_name, udf_sql in udfs.items():
            udf_lines.append(udf_sql)
            udf_lines.append("")
        udf_path = output_dir / "_udfs.sql"
        udf_path.write_text("\n".join(udf_lines))
        results["_udfs"] = udf_path

    return results


# =============================================
# MIGRATION ENGINE
# =============================================
# Migrations are also YAML — no SQL required.
#
# File: dbt/migrations/001_add_loyalty_tier.yml
#
#   migration: "001_add_loyalty_tier"
#   description: "Add loyalty tier column to customer_summary"
#   changes:
#     - model: customer_summary
#       add_columns:
#         loyalty_tier:
#           type: string
#           expr: "case when total_revenue > 1000 then 'gold' when total_revenue > 500 then 'silver' else 'bronze' end"
#           description: "Customer loyalty tier based on revenue"
#
#     - model: customer_clean
#       rename_columns:
#         revenue: total_revenue
#
#     - model: stg_customers
#       change_type:
#         customer_id: { from: string, to: int, cast: true }
#
#     - model: customer_orders
#       remove_columns: [legacy_field]
#
# Then:  forge migrate   → applies changes to models.yml + recompiles

def apply_migration(
    ddl_path: Path,
    migration_path: Path,
) -> dict[str, Any]:
    """
    Apply a migration YAML to the models.yml DDL.

    Returns a summary of changes applied.
    """
    models = load_ddl(ddl_path)
    migration = yaml.safe_load(migration_path.read_text())
    summary: dict[str, list[str]] = {}

    for change in migration.get("changes", []):
        model_name = change["model"]
        if model_name not in models:
            raise ValueError(f"Model '{model_name}' not found in {ddl_path}")

        model = models[model_name]
        columns = model.setdefault("columns", {})
        changes_applied: list[str] = []

        # ADD COLUMNS
        for col_name, col_def in change.get("add_columns", {}).items():
            columns[col_name] = col_def
            changes_applied.append(f"added column '{col_name}'")

        # REMOVE COLUMNS
        for col_name in change.get("remove_columns", []):
            if col_name in columns:
                del columns[col_name]
                changes_applied.append(f"removed column '{col_name}'")

        # RENAME COLUMNS
        for old_name, new_name in change.get("rename_columns", {}).items():
            if old_name in columns:
                columns[new_name] = columns.pop(old_name)
                changes_applied.append(f"renamed '{old_name}' → '{new_name}'")

        # CHANGE TYPE
        for col_name, type_change in change.get("change_type", {}).items():
            if col_name in columns:
                if isinstance(columns[col_name], dict):
                    columns[col_name]["type"] = type_change.get("to", columns[col_name].get("type"))
                    if type_change.get("cast"):
                        columns[col_name]["cast"] = True
                else:
                    columns[col_name] = {"type": type_change.get("to", "string")}
                changes_applied.append(
                    f"changed type of '{col_name}': {type_change.get('from', '?')} → {type_change.get('to', '?')}"
                )

        # CHANGE QUARANTINE
        if "quarantine" in change:
            model["quarantine"] = change["quarantine"]
            changes_applied.append(f"updated quarantine rule")

        # CHANGE VERSION
        if "version" in change:
            model["version"] = change["version"]
            changes_applied.append(f"bumped version to {change['version']}")

        # CHANGE MATERIALIZATION
        if "materialized" in change:
            model["materialized"] = change["materialized"]
            changes_applied.append(f"changed materialization to {change['materialized']}")

        summary[model_name] = changes_applied

    # Write back
    full = {"models": models}
    ddl_path.write_text(yaml.dump(full, sort_keys=False, default_flow_style=False))

    return {
        "migration": migration.get("migration", "unnamed"),
        "description": migration.get("description", ""),
        "changes": summary,
    }


def apply_all_migrations(
    ddl_path: Path,
    migrations_dir: Path,
    applied_log: Path | None = None,
) -> list[dict]:
    """
    Apply all unapplied migrations in order.

    Migrations are numbered: 001_xxx.yml, 002_xxx.yml, ...
    Already-applied migrations are tracked in .migrations_applied.
    """
    if not migrations_dir.exists():
        return []

    # Load applied log
    log_path = applied_log or (migrations_dir / ".migrations_applied")
    applied: set[str] = set()
    if log_path.exists():
        applied = set(log_path.read_text().strip().splitlines())

    # Find and sort migrations
    migration_files = sorted(migrations_dir.glob("*.yml"))
    migration_files = [
        f for f in migration_files
        if f.name != ".migrations_applied" and f.stem not in applied
    ]

    results = []
    for mig_file in migration_files:
        result = apply_migration(ddl_path, mig_file)
        results.append(result)
        applied.add(mig_file.stem)

    # Update log
    log_path.write_text("\n".join(sorted(applied)) + "\n")

    return results


def apply_all_migrations_dry_run(
    ddl_path: Path,
    migrations_dir: Path,
    applied_log: Path | None = None,
) -> list[dict]:
    """
    Preview all unapplied migrations WITHOUT modifying any files.

    Same logic as apply_all_migrations but operates on an in-memory
    copy of models.yml and never writes back.
    """
    if not migrations_dir.exists():
        return []

    log_path = applied_log or (migrations_dir / ".migrations_applied")
    applied: set[str] = set()
    if log_path.exists():
        applied = set(log_path.read_text().strip().splitlines())

    migration_files = sorted(migrations_dir.glob("*.yml"))
    migration_files = [
        f for f in migration_files
        if f.name != ".migrations_applied" and f.stem not in applied
    ]

    if not migration_files:
        return []

    # Work on in-memory copy
    models = load_ddl(ddl_path)

    results = []
    for mig_file in migration_files:
        migration = yaml.safe_load(mig_file.read_text())
        summary: dict[str, list[str]] = {}

        for change in migration.get("changes", []):
            model_name = change["model"]
            if model_name not in models:
                summary[model_name] = [f"⚠ model '{model_name}' not found — would fail"]
                continue

            model = models[model_name]
            columns = model.get("columns", {})
            changes_desc: list[str] = []

            for col_name in change.get("add_columns", {}):
                changes_desc.append(f"add column '{col_name}'")
            for col_name in change.get("remove_columns", []):
                changes_desc.append(f"remove column '{col_name}'")
            for old_n, new_n in change.get("rename_columns", {}).items():
                changes_desc.append(f"rename '{old_n}' → '{new_n}'")
            for col_name, tc in change.get("change_type", {}).items():
                changes_desc.append(f"retype '{col_name}': {tc.get('from','?')} → {tc.get('to','?')}")
            if "quarantine" in change:
                changes_desc.append("update quarantine rule")
            if "version" in change:
                changes_desc.append(f"bump version to {change['version']}")
            if "materialized" in change:
                changes_desc.append(f"change materialization to {change['materialized']}")

            summary[model_name] = changes_desc

        results.append({
            "migration": migration.get("migration", mig_file.stem),
            "description": migration.get("description", ""),
            "changes": summary,
        })

    return results


# =============================================
# DATA CHECKS ENGINE
# =============================================
# Checks are defined inline in models.yml under
# each model's `checks:` block.
#
# Supported check types:
#
#   range  — column value must be within [min, max]
#     checks:
#       - name: revenue_non_negative
#         type: range
#         column: total_revenue
#         min: 0
#         severity: error
#
#   recency — table must have rows within a time window
#     checks:
#       - name: data_is_fresh
#         type: recency
#         column: updated_at
#         max_age: "24 hours"
#         severity: warn
#
#   row_count — table must have at least N rows
#     checks:
#       - name: not_empty
#         type: row_count
#         min: 1
#         severity: error
#
#   regex — column must match a pattern
#     checks:
#       - name: valid_email
#         type: regex
#         column: email
#         pattern: "^[^@]+@[^@]+\\.[^@]+$"
#         severity: warn
#
#   custom_sql — arbitrary SQL condition
#     checks:
#       - name: orders_match
#         type: custom_sql
#         sql: "select count(*) from {{ ref('model') }} where condition"
#         severity: error

# Scope classification for check types
_CHECK_SCOPE = {
    "range": "cell",
    "regex": "cell",
    "recency": "column",
    "row_count": "table",
    "custom_sql": "table",
}


def _compile_single_check(
    model_name: str,
    check: dict,
) -> dict[str, Any]:
    """
    Compile a single check definition into executable SQL + metadata.

    Returns: {name, type, scope, severity, column, sql}
    """
    check_type = check["type"]
    name = check.get("name", f"{model_name}_{check_type}")
    severity = check.get("severity", "warn")
    column = check.get("column")
    scope = _CHECK_SCOPE.get(check_type, "table")

    ref = f"{{{{ ref('{model_name}') }}}}"

    if check_type == "range":
        conditions = []
        if "min" in check:
            conditions.append(f"{column} < {check['min']}")
        if "max" in check:
            conditions.append(f"{column} > {check['max']}")
        where = " or ".join(conditions) if conditions else "1=0"
        sql = f"select count(*) as failures from {ref} where {where}"

    elif check_type == "recency":
        max_age = check.get("max_age", "24 hours")
        # Parse "N unit" format (e.g. "24 hours", "7 days")
        sql = (
            f"select case when max({column}) < current_timestamp() - interval '{max_age}' "
            f"then 1 else 0 end as failures from {ref}"
        )

    elif check_type == "row_count":
        min_rows = check.get("min", 1)
        sql = (
            f"select case when count(*) < {int(min_rows)} "
            f"then 1 else 0 end as failures from {ref}"
        )

    elif check_type == "regex":
        pattern = check.get("pattern", ".*")
        sql = (
            f"select count(*) as failures from {ref} "
            f"where {column} is not null and not regexp({column}, '{pattern}')"
        )

    elif check_type == "custom_sql":
        sql = check.get("sql", f"select 0 as failures")

    else:
        sql = f"-- unknown check type: {check_type}"

    return {
        "name": name,
        "type": check_type,
        "scope": scope,
        "severity": severity,
        "column": column,
        "sql": sql,
    }


def compile_checks_sql(
    ddl_path: Path,
    model_filter: str | None = None,
) -> dict[str, list[dict]]:
    """
    Compile all checks from models.yml into SQL.

    Returns: {model_name: [{name, type, scope, severity, sql}, ...]}
    """
    models = load_ddl(ddl_path)
    results: dict[str, list[dict]] = {}

    for model_name, model_def in models.items():
        if model_filter and model_name != model_filter:
            continue

        checks = model_def.get("checks", [])
        if not checks:
            continue

        compiled = []
        for check in checks:
            compiled.append(_compile_single_check(model_name, check))
        results[model_name] = compiled

    return results


# =============================================
# AGENT GUIDE GENERATOR
# =============================================
# Auto-generates a project-specific .instructions.md
# that teaches an AI agent (or a new user) how to
# use this exact project.

def generate_agent_guide(
    forge_config: dict,
    ddl_path: Path | None = None,
    output_path: Path | None = None,
) -> str:
    """
    Generate a project-specific agent guide / onboarding document.

    Reads forge.yml + models.yml to produce contextual instructions
    that an LLM agent or new team member can follow.
    """
    project_name = forge_config.get("name", "unnamed")
    environment = forge_config.get("environment", "dev")
    catalog = forge_config.get("catalog", "main")
    schema = forge_config.get("schema", "default")
    compute_type = forge_config.get("compute", {}).get("type", "serverless")
    features = forge_config.get("features", {})
    schedule = forge_config.get("schedule")

    # Load models if DDL exists
    models: dict[str, dict] = {}
    if ddl_path and ddl_path.exists():
        models = load_ddl(ddl_path)

    # Build the guide
    lines: list[str] = []

    lines.append(f"# {project_name} — Project Guide")
    lines.append("")
    lines.append("This guide was auto-generated by `forge guide`.")
    lines.append("It teaches you everything about this specific project.")
    lines.append("")

    # ── Quick Start ──────────────────────────────
    lines.append("## Quick Start")
    lines.append("")
    lines.append("```bash")
    lines.append("# 1. Set up the project")
    lines.append("forge setup")
    lines.append("")
    lines.append("# 2. Edit your models (no SQL needed!)")
    lines.append("# Open: dbt/models.yml")
    lines.append("")
    lines.append("# 3. Compile YAML into SQL")
    lines.append("forge compile")
    lines.append("")
    lines.append("# 4. Deploy to Databricks")
    lines.append("forge deploy")
    lines.append("")
    lines.append("# 5. See what changed")
    lines.append("forge diff")
    lines.append("```")
    lines.append("")

    # ── Project Config ───────────────────────────
    lines.append("## Project Configuration")
    lines.append("")
    lines.append(f"- **Project**: {project_name}")
    lines.append(f"- **Environment**: {environment}")
    lines.append(f"- **Catalog**: {catalog}")
    lines.append(f"- **Schema**: {schema}")
    lines.append(f"- **Compute**: {compute_type}")
    if schedule:
        lines.append(f"- **Schedule**: {schedule}")
    lines.append("")

    # ── Features ─────────────────────────────────
    lines.append("## Enabled Features")
    lines.append("")
    for feat, enabled in features.items():
        status = "ON" if enabled else "OFF"
        lines.append(f"- **{feat}**: {status}")
    lines.append("")

    # ── Available Commands ───────────────────────
    lines.append("## Commands You Can Run")
    lines.append("")
    lines.append("| Command | What it does |")
    lines.append("|---------|-------------|")
    lines.append("| `forge setup` | Creates project structure + forge.yml |")
    lines.append("| `forge compile` | Turns models.yml into SQL (no coding needed) |")
    lines.append("| `forge deploy` | Builds + deploys to Databricks |")
    lines.append("| `forge diff` | Shows what changed since last deploy |")
    lines.append("| `forge diff --mermaid` | Visual diagram of changes (color-coded) |")
    lines.append("| `forge explain model.col` | Full provenance tree for any column |")
    lines.append("| `forge explain model.col --mermaid` | Provenance as Mermaid diagram |")
    lines.append("| `forge explain model.col --json` | Provenance as JSON (for scripting) |")
    lines.append("| `forge workflow` | Shows the pipeline DAG |")
    lines.append("| `forge workflow --mermaid` | Visual diagram of the pipeline |")
    lines.append("| `forge migrate` | Applies migration YAML files |")
    lines.append("| `forge udfs` | Shows defined UDFs |")
    lines.append("| `forge validate` | Runs data quality checks |")
    lines.append("| `forge codegen` | Generates type-safe Python SDK |")
    lines.append("| `forge guide` | Regenerates this guide |")
    lines.append("| `forge teardown` | Safely destroys everything |")
    lines.append("")

    # ── How to Define Models ─────────────────────
    lines.append("## How to Define Models (No SQL Required)")
    lines.append("")
    lines.append("Edit `dbt/models.yml` using this format:")
    lines.append("")
    lines.append("### Simple staging model (reads from a source)")
    lines.append("")
    lines.append("```yaml")
    lines.append("models:")
    lines.append("  stg_customers:")
    lines.append('    description: "Staged customer data"')
    lines.append("    source: raw_customers")
    lines.append("    columns:")
    lines.append("      customer_id: { type: int, required: true, unique: true }")
    lines.append("      email:       { type: string }")
    lines.append("      signup_date: { type: date, cast: true }")
    lines.append("```")
    lines.append("")
    lines.append("### Model with data cleaning (quarantine bad rows)")
    lines.append("")
    lines.append("```yaml")
    lines.append("  customer_clean:")
    lines.append('    description: "Clean customers"')
    lines.append("    source: stg_customers")
    lines.append("    materialized: table")
    lines.append('    quarantine: "email IS NULL OR revenue < 0"')
    lines.append("    columns:")
    lines.append("      customer_id: { type: int, required: true }")
    lines.append("      email:       { type: string, required: true }")
    lines.append("```")
    lines.append("")
    lines.append("### Join two sources")
    lines.append("")
    lines.append("```yaml")
    lines.append("  customer_orders:")
    lines.append('    description: "Customers with their orders"')
    lines.append("    sources:")
    lines.append("      c: customer_clean")
    lines.append("      o: stg_orders")
    lines.append('    join: "c.customer_id = o.customer_id"')
    lines.append("    materialized: table")
    lines.append("    columns:")
    lines.append("      customer_id: { from: c }")
    lines.append("      order_id:    { from: o, required: true, unique: true }")
    lines.append("      product:     { from: o }")
    lines.append("```")
    lines.append("")
    lines.append("### Aggregation (summary / gold layer)")
    lines.append("")
    lines.append("```yaml")
    lines.append("  customer_summary:")
    lines.append('    description: "Customer metrics"')
    lines.append("    source: customer_orders")
    lines.append("    materialized: table")
    lines.append("    group_by: [customer_id, email]")
    lines.append("    columns:")
    lines.append("      customer_id:   { type: int, required: true, unique: true }")
    lines.append('      total_orders:  { expr: "count(order_id)", type: int }')
    lines.append('      total_revenue: { expr: "sum(line_total)", type: decimal(10,2) }')
    lines.append("```")
    lines.append("")

    # ── Column Options ───────────────────────────
    lines.append("## Column Options Reference")
    lines.append("")
    lines.append("| Option | Example | What it does |")
    lines.append("|--------|---------|-------------|")
    lines.append("| `type` | `int`, `string`, `date`, `decimal(10,2)` | Column data type |")
    lines.append("| `required` | `true` | Column cannot be NULL (adds not_null test) |")
    lines.append("| `unique` | `true` | Column must be unique (adds unique test) |")
    lines.append("| `cast` | `true` | Wraps in CAST() to convert type |")
    lines.append("| `expr` | `\"count(order_id)\"` | Custom SQL expression |")
    lines.append("| `from` | `c` | Which join alias this column comes from |")
    lines.append("| `description` | `\"Customer email\"` | Human-readable description |")
    lines.append("| `accepted_values` | `[\"gold\", \"silver\"]` | Only allow these values |")
    lines.append("| `udf` | `\"loyalty_tier(total_revenue)\"` | Call a UDF defined in udfs: block |")
    lines.append("")

    # ── How to Explain Provenance ───────────────
    lines.append("## How to Explain Provenance (Column-Level Lineage)")
    lines.append("")
    lines.append("Trace any value back to its source with one command:")
    lines.append("")
    lines.append("```bash")
    lines.append("# See full provenance tree in terminal")
    lines.append("forge explain customer_summary.total_revenue")
    lines.append("")
    lines.append("# As Mermaid diagram")
    lines.append("forge explain customer_summary.total_revenue --mermaid --output docs/explain.mmd")
    lines.append("")
    lines.append("# As JSON (for scripting)")
    lines.append("forge explain customer_summary.total_revenue --json")
    lines.append("")
    lines.append("# Full detail (includes upstream checks)")
    lines.append("forge explain customer_summary.total_revenue --full")
    lines.append("```")
    lines.append("")
    lines.append("The explain tree shows: expression, UDF calls, checks,")
    lines.append("git commit, version, and full upstream lineage.")
    lines.append("")

    # ── How to Define UDFs ───────────────────────
    lines.append("## How to Define UDFs (Reusable Functions)")
    lines.append("")
    lines.append("Add a `udfs:` block in `dbt/models.yml`:")
    lines.append("")
    lines.append("### SQL UDF")
    lines.append("")
    lines.append("```yaml")
    lines.append("udfs:")
    lines.append("  loyalty_tier:")
    lines.append("    language: sql")
    lines.append("    returns: string")
    lines.append("    params:")
    lines.append("      - { name: revenue, type: \\\"decimal(18,2)\\\" }")
    lines.append("    body: |")
    lines.append("      CASE")
    lines.append("        WHEN revenue >= 1000 THEN 'GOLD'")
    lines.append("        WHEN revenue >=  500 THEN 'SILVER'")
    lines.append("        ELSE 'BRONZE'")
    lines.append("      END")
    lines.append("```")
    lines.append("")
    lines.append("### Use in a column")
    lines.append("")
    lines.append("```yaml")
    lines.append("models:")
    lines.append("  customer_summary:")
    lines.append("    columns:")
    lines.append('      tier: { udf: "loyalty_tier(total_revenue)", description: "Loyalty tier" }')
    lines.append("```")
    lines.append("")
    lines.append("### Manage UDFs")
    lines.append("")
    lines.append("| Command | What it does |")
    lines.append("|---------|-------------|")
    lines.append("| `forge udfs` | Shows all defined UDFs |")
    lines.append("| `forge udfs --output udfs.sql` | Writes CREATE FUNCTION SQL |")
    lines.append("| `forge compile` | Auto-generates `_udfs.sql` alongside models |")
    lines.append("")

    # ── UDF inventory ────────────────────────────
    if ddl_path and ddl_path.exists():
        udfs_defined = load_udfs(ddl_path)
        if udfs_defined:
            lines.append("### Your Current UDFs")
            lines.append("")
            lines.append("| UDF | Language | Returns | Parameters |")
            lines.append("|-----|----------|---------|------------|")
            for uname, udef in udfs_defined.items():
                lang = udef.get("language", "sql").upper()
                ret = udef.get("returns", "STRING")
                params = udef.get("params", [])
                param_str = ", ".join(
                    f"{p['name']}: {p['type']}" if isinstance(p, dict) else str(p)
                    for p in params
                ) or "(none)"
                lines.append(f"| `{uname}` | {lang} | {ret} | {param_str} |")
            lines.append("")

    # ── How Migrations Work ──────────────────────
    lines.append("## How to Make Changes (Migrations)")
    lines.append("")
    lines.append("Create a YAML file in `dbt/migrations/`:")
    lines.append("")
    lines.append("```yaml")
    lines.append("# dbt/migrations/001_add_loyalty_tier.yml")
    lines.append('migration: "001_add_loyalty_tier"')
    lines.append('description: "Add loyalty tier to customer summary"')
    lines.append("changes:")
    lines.append("  - model: customer_summary")
    lines.append("    add_columns:")
    lines.append("      loyalty_tier:")
    lines.append("        type: string")
    lines.append("        expr: \"case when total_revenue > 1000 then 'gold' else 'silver' end\"")
    lines.append("```")
    lines.append("")
    lines.append("Then run: `forge migrate`")
    lines.append("")
    lines.append("### Migration actions you can use:")
    lines.append("")
    lines.append("| Action | Example |")
    lines.append("|--------|---------|")
    lines.append("| `add_columns` | Add new columns with type/expression |")
    lines.append("| `remove_columns` | Remove columns by name |")
    lines.append("| `rename_columns` | Rename: `old_name: new_name` |")
    lines.append("| `change_type` | Change type: `col: { from: string, to: int }` |")
    lines.append("| `quarantine` | Update the quarantine rule |")
    lines.append("| `version` | Bump the model version |")
    lines.append("| `materialized` | Change to view/table |")
    lines.append("")

    # ── Current Models ───────────────────────────
    if models:
        lines.append("## Your Current Models")
        lines.append("")
        lines.append("| Model | Description | Stage | Columns |")
        lines.append("|-------|------------|-------|---------|")
        for name, mdef in models.items():
            desc = mdef.get("description", "")
            stage = mdef.get("stage", _guess_stage(name, mdef))
            cols = list(mdef.get("columns", {}).keys())
            col_str = ", ".join(cols[:5])
            if len(cols) > 5:
                col_str += f" +{len(cols)-5}"
            lines.append(f"| `{name}` | {desc} | {stage} | {col_str} |")
        lines.append("")

        # Pipeline visualization
        lines.append("### Pipeline Flow")
        lines.append("")
        lines.append("```")
        _render_text_pipeline(models, lines)
        lines.append("```")
        lines.append("")

    # ── Troubleshooting ──────────────────────────
    lines.append("## Troubleshooting")
    lines.append("")
    lines.append("| Problem | Solution |")
    lines.append("|---------|----------|")
    lines.append("| `forge.yml not found` | Run `forge setup` first |")
    lines.append("| Models not updating | Run `forge compile` after editing models.yml |")
    lines.append("| Bad data in table | Add a `quarantine` rule to the model |")
    lines.append("| Need to rename a column | Create a migration YAML file |")
    lines.append("| Want to see the pipeline | Run `forge workflow --mermaid` |")
    lines.append("| Python type errors | Run `forge codegen` to regenerate SDK |")
    lines.append("")

    guide_text = "\n".join(lines)

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(guide_text)

    return guide_text


def _guess_stage(name: str, model_def: dict) -> str:
    """Quick stage guess for the guide table."""
    if name.startswith("raw_") or name.startswith("seed_"):
        return "ingest"
    if name.startswith("stg_"):
        return "stage"
    if "quarantine" in str(model_def.get("quarantine", "")):
        return "clean"
    if model_def.get("quarantine"):
        return "clean"
    if "group_by" in model_def:
        return "serve"
    if "join" in model_def or "sources" in model_def:
        return "enrich"
    return "enrich"


def _render_text_pipeline(models: dict, lines: list[str]) -> None:
    """Render a simple ASCII pipeline diagram."""
    # Build dependency graph
    deps: dict[str, list[str]] = {}
    for name, mdef in models.items():
        sources = []
        if "source" in mdef:
            sources.append(mdef["source"])
        if "sources" in mdef:
            sources.extend(mdef["sources"].values())
        deps[name] = sources

    # Find roots (no source in our models dict)
    model_names = set(models.keys())
    roots = [n for n in models if not any(s in model_names for s in deps.get(n, []))]

    # BFS render
    visited: set[str] = set()
    queue = [(r, 0) for r in roots]
    while queue:
        name, depth = queue.pop(0)
        if name in visited:
            continue
        visited.add(name)
        indent = "  " * depth
        arrow = "→ " if depth > 0 else ""
        lines.append(f"{indent}{arrow}{name}")
        for child_name, child_deps in deps.items():
            if name in child_deps and child_name not in visited:
                queue.append((child_name, depth + 1))
