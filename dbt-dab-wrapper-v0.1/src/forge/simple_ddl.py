# =============================================
# src/forge/simple_ddl.py
# =============================================
# THE CODE IS THE DOCUMENTATION
#
# A zero-SQL, zero-Python DDL for users who have
# never written code before.
#
# Users write YAML in one of two layouts:
#   1. Single file:  dbt/models.yml        (simple projects)
#   2. Directory:    dbt/ddl/*.yml          (split by layer)
#
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

import getpass
import re
from pathlib import Path
from typing import Any

import yaml


# =============================================
# DDL YAML PARSER
# =============================================

def load_raw_ddl(ddl_path: Path, forge_config: dict | None = None) -> dict:
    """Load DDL from a single file or a directory tree of YAML files.

    Supports three layouts:
      1. Single file:  dbt/models.yml
      2. Flat dir:     dbt/ddl/*.yml  (all files at root level)
      3. Layer tree:   dbt/ddl/{layer}/**/*.yml  (folder = layer)

    In layout 3, top-level subdirectories (bronze/, silver/, gold/) are
    treated as layer names.  Each layer name is validated against the
    ``schemas`` list in *forge_config* (from forge.yml).  The layer is
    injected into every model definition as ``layer: <folder>``.

    Duplicate model, UDF, or seed names across files raise ValueError.
    """
    if ddl_path.is_file():
        return yaml.safe_load(ddl_path.read_text()) or {}

    if ddl_path.is_dir():
        merged: dict[str, Any] = {"models": {}, "udfs": {}, "seeds": {}, "volumes": {}}

        # Collect allowed layer names from forge.yml
        allowed_layers: set[str] | None = None
        if forge_config:
            # Union of schemas + catalogs = valid folder names
            allowed_layers = set(forge_config.get("schemas", []))
            allowed_layers |= set(forge_config.get("catalogs", []))

        # Detect layout: are there layer subdirectories?
        subdirs = [d for d in sorted(ddl_path.iterdir()) if d.is_dir()]
        root_ymls = sorted(ddl_path.glob("*.yml"))

        if subdirs:
            # Layout 3: layer tree — validate folder names
            if allowed_layers:
                for d in subdirs:
                    if d.name not in allowed_layers:
                        raise ValueError(
                            f"Folder '{d.name}' in {ddl_path} is not a valid layer. "
                            f"Allowed layers (from forge.yml schemas + catalogs): "
                            f"{sorted(allowed_layers)}"
                        )

            # Walk all YAML files recursively; infer layer from top-level folder
            for yml_file in sorted(ddl_path.rglob("*.yml")):
                rel = yml_file.relative_to(ddl_path)
                parts = rel.parts  # e.g. ('silver', 'staging', 'stg_customers.yml')
                layer = parts[0] if len(parts) > 1 else None

                raw = yaml.safe_load(yml_file.read_text()) or {}
                _merge_section(merged, "models", raw, yml_file, inject_layer=layer)
                _merge_section(merged, "udfs", raw, yml_file)
                _merge_section(merged, "seeds", raw, yml_file)
                _merge_section(merged, "volumes", raw, yml_file)
        else:
            # Layout 2: flat directory — root-level *.yml only
            for yml_file in root_ymls:
                raw = yaml.safe_load(yml_file.read_text()) or {}
                _merge_section(merged, "models", raw, yml_file)
                _merge_section(merged, "udfs", raw, yml_file)
                _merge_section(merged, "seeds", raw, yml_file)
                _merge_section(merged, "volumes", raw, yml_file)

        return merged

    raise FileNotFoundError(f"DDL path not found: {ddl_path}")


def _merge_section(
    merged: dict,
    section: str,
    raw: dict,
    yml_file: Path,
    inject_layer: str | None = None,
) -> None:
    """Merge a section (models/udfs/seeds) from *raw* into *merged*.

    Raises on duplicates.  For models, injects ``layer`` when provided.
    """
    for name, defn in raw.get(section, {}).items():
        if name in merged[section]:
            raise ValueError(
                f"Duplicate {section.rstrip('s')} '{name}' in {yml_file.name} "
                f"(already defined in an earlier file)"
            )
        if inject_layer and section in ("models", "udfs") and isinstance(defn, dict):
            defn.setdefault("layer", inject_layer)
        merged[section][name] = defn


def load_ddl(ddl_path: Path, forge_config: dict | None = None) -> dict[str, dict]:
    """Load models from a file or directory and return the models dict."""
    return load_raw_ddl(ddl_path, forge_config=forge_config).get("models", {})


def load_udfs(ddl_path: Path, forge_config: dict | None = None) -> dict[str, dict]:
    """Load UDFs from a file or directory and return the udfs dict."""
    return load_raw_ddl(ddl_path, forge_config=forge_config).get("udfs", {})


def load_seeds(ddl_path: Path, forge_config: dict | None = None) -> dict[str, dict]:
    """Load seed definitions from a file or directory."""
    return load_raw_ddl(ddl_path, forge_config=forge_config).get("seeds", {})


def load_volumes(ddl_path: Path, forge_config: dict | None = None) -> dict[str, dict]:
    """Load volume definitions from a file or directory."""
    return load_raw_ddl(ddl_path, forge_config=forge_config).get("volumes", {})


# =============================================
# SQL COMPILER
# =============================================

def _compile_column_select(
    col_name: str,
    col_def: dict,
    alias: str | None = None,
    udf_qualifiers: dict[str, str] | None = None,
    all_columns: dict | None = None,
) -> str:
    """
    Compile a column definition into a SQL SELECT expression.

    Rules:
      expr  → use as-is (aggregation / calculation)
      cast  → wrap in CAST(col AS type)
      from  → prefix with alias (c.col_name)
      else  → plain column name

    udf_qualifiers maps bare UDF names to fully-qualified names,
    e.g. {"loyalty_tier": "`dev_fd_silver`.`ben_sales`.loyalty_tier"}.

    all_columns is the full sibling columns dict — used to resolve UDF
    arguments that reference aliases with underlying expressions (avoids
    lateral column alias errors in Databricks).
    """
    # UDF call: { udf: "loyalty_tier(total_revenue)" }
    if "udf" in col_def:
        udf_call = col_def["udf"]
        # Resolve arguments that are aliases for expressions
        if all_columns:
            fn_name = udf_call.split("(")[0].strip()
            args_start = udf_call.index("(")
            args_str = udf_call[args_start + 1 : udf_call.rindex(")")]
            args = [a.strip() for a in args_str.split(",")]
            resolved_args = []
            for arg in args:
                sibling = all_columns.get(arg)
                if isinstance(sibling, dict) and "expr" in sibling:
                    resolved_args.append(sibling["expr"])
                else:
                    resolved_args.append(arg)
            udf_call = f"{fn_name}({', '.join(resolved_args)})"
        if udf_qualifiers:
            fn_name = udf_call.split("(")[0].strip()
            if fn_name in udf_qualifiers:
                udf_call = udf_qualifiers[fn_name] + udf_call[len(fn_name):]
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


def _resolve_base_schema(forge_config: dict, layer: str) -> str:
    """Resolve the concrete schema name for a layer (e.g. 'ben_demo').

    Used by domain expansion to compute domain schemas like 'ben_demo_eu'.
    """
    profile_name = forge_config.get("active_profile", "dev")
    profiles = forge_config.get("profiles", {})
    profile = profiles.get(profile_name, {})

    # If profile has explicit schemas dict, use it
    if isinstance(profile.get("schemas"), dict):
        return profile["schemas"].get(layer, profile.get("schema", layer))

    # Otherwise expand via pattern
    schema_pattern = forge_config.get("schema_pattern", "{user}_{id}")
    env = profile.get("env", "dev")
    project_id = forge_config.get("id", forge_config.get("name", "project")).replace("-", "_")
    scope = forge_config.get("scope", "").replace("-", "_")
    raw_user = getpass.getuser()
    user = re.sub(r"[^a-z0-9]+", "_", raw_user.lower()).strip("_")
    skip_envs = set(forge_config.get("skip_env_prefix", ["prd", "prod"]))

    result = schema_pattern
    if env in skip_envs:
        result = result.replace("{env}_", "").replace("{env}", "")
    else:
        result = result.replace("{env}", env)
    result = result.replace("{id}", project_id)
    result = result.replace("{scope}", scope)
    result = result.replace("{user}", user)
    result = result.replace("{schema}", layer)
    while "__" in result:
        result = result.replace("__", "_")
    return result.strip("_")


def _resolve_catalog(forge_config: dict, layer: str) -> str:
    """Resolve the concrete catalog name for a layer (e.g. 'dev_fd_bronze').

    Applies catalog_pattern from forge.yml to generate fully-qualified catalog names.
    """
    profile_name = forge_config.get("active_profile", "dev")
    profiles = forge_config.get("profiles", {})
    profile = profiles.get(profile_name, {})

    # If profile has explicit catalogs dict, use it
    if isinstance(profile.get("catalogs"), dict):
        return profile["catalogs"].get(layer, layer)

    # Otherwise expand via pattern
    catalog_pattern = forge_config.get("catalog_pattern", "{env}_{scope}_{catalog}")
    env = profile.get("env", "dev")
    project_id = forge_config.get("id", forge_config.get("name", "project")).replace("-", "_")
    scope = forge_config.get("scope", "").replace("-", "_")
    raw_user = getpass.getuser()
    user = re.sub(r"[^a-z0-9]+", "_", raw_user.lower()).strip("_")

    result = catalog_pattern
    result = result.replace("{env}", env)
    result = result.replace("{id}", project_id)
    result = result.replace("{scope}", scope)
    result = result.replace("{user}", user)
    result = result.replace("{catalog}", layer)
    while "__" in result:
        result = result.replace("__", "_")
    return result.strip("_")


def _compile_managed_table(
    model_name: str,
    model_def: dict,
    forge_config: dict | None = None,
) -> str:
    """Compile a managed_by: python model to CREATE TABLE IF NOT EXISTS.

    These tables are populated by an external process (Python task, API, etc.)
    rather than a dbt SELECT.  Forge still generates schema.yml tests
    and a graph node.
    """
    columns = model_def.get("columns", {})

    # Schema/catalog routing
    layer = model_def.get("layer")
    model_schema = model_def.get("schema")
    model_catalog = model_def.get("catalog")

    config_parts = ["materialized='table'"]
    if model_schema:
        config_parts.append(f"schema='{model_schema}'")
    elif layer:
        config_parts.append(f'schema=var("schema_{layer}")')
    if model_catalog:
        config_parts.append(f"database='{model_catalog}'")
    elif layer:
        config_parts.append(f'database=var("catalog_{layer}")')

    lines: list[str] = []
    lines.append("{{{{ config({}) }}}}".format(", ".join(config_parts)))
    lines.append("")
    lines.append(f"-- managed_by: {model_def.get('managed_by', 'python')}")
    lines.append("-- This table is populated by an external process.")
    lines.append("-- Forge generates CREATE TABLE to keep it in the graph with tests.")
    lines.append("")

    # Build column definitions
    col_defs: list[str] = []
    for col_name, col_def in columns.items():
        if isinstance(col_def, str):
            col_def = {"type": col_def}
        elif col_def is None:
            col_def = {}
        col_type = col_def.get("type", "STRING").upper()
        not_null = " NOT NULL" if col_def.get("required") else ""
        col_defs.append(f"    {col_name} {col_type}{not_null}")

    # Emit a SELECT that creates the schema but returns zero rows
    lines.append("SELECT")
    cast_cols = []
    for col_name, col_def in columns.items():
        if isinstance(col_def, str):
            col_def = {"type": col_def}
        elif col_def is None:
            col_def = {}
        col_type = col_def.get("type", "STRING")
        cast_cols.append(f"    CAST(NULL AS {col_type}) AS {col_name}")
    lines.append(",\n".join(cast_cols))
    lines.append("WHERE 1 = 0")

    return "\n".join(lines) + "\n"


def compile_model(model_name: str, model_def: dict, forge_config: dict | None = None, seed_names: set[str] | None = None, udf_languages: dict[str, str] | None = None, lineage_mode: str = "full", origin_tracking: bool = True) -> str:
    """Compile a single model definition into a dbt SQL file.

    udf_languages: optional {udf_name: language} lookup for rich lineage ops.
    lineage_mode: 'full' (capture runtime values) or 'lightweight' (names only).
    origin_tracking: whether to include origin metadata in lineage struct.
    """
    # ── managed_by: python → CREATE TABLE (no SELECT) ─────
    managed_by = model_def.get("managed_by")
    if managed_by:
        return _compile_managed_table(model_name, model_def, forge_config)

    lines: list[str] = []

    # Config block
    materialized = model_def.get("materialized", "view")
    version = model_def.get("version")
    quarantine = model_def.get("quarantine")

    config_parts = [f"materialized='{materialized}'"]

    # Schema routing: layer-based (from DDL folder) or explicit model-level overrides
    model_schema = model_def.get("schema")
    model_catalog = model_def.get("catalog")
    layer = model_def.get("layer")

    if model_schema:
        config_parts.append(f"schema='{model_schema}'")
    elif layer:
        config_parts.append(f'schema=var("schema_{layer}")')

    if model_catalog:
        config_parts.append(f"database='{model_catalog}'")
    elif layer:
        config_parts.append(f'database=var("catalog_{layer}")')

    # Domain tag (set by domain expansion in compile_all)
    domain = model_def.get("_domain")
    if domain:
        config_parts.append(f"tags=['{domain}']")

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
        col_lines.append(_compile_column_select(col_name, col_def, default_alias, all_columns=columns))

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
            udf_call = col_def["udf"]
            fn_name = udf_call.split("(")[0].strip()
            entry["expr"] = udf_call
            entry["udf_name"] = fn_name
            # Determine specific UDF op type
            _udf_langs = udf_languages or {}
            raw_lang = _udf_langs.get(fn_name, "").upper()
            if raw_lang == "PANDAS":
                entry["op"] = "PANDAS_UDF"
            elif raw_lang == "PYTHON":
                entry["op"] = "PYTHON_UDF"
            elif raw_lang:
                entry["op"] = "SQL_UDF"
            else:
                entry["op"] = "EXTERNAL_UDF"
            # Extract input column names from call args
            paren_start = udf_call.find("(")
            paren_end = udf_call.rfind(")")
            if paren_start != -1 and paren_end != -1:
                args_str = udf_call[paren_start + 1 : paren_end]
                inputs = [a.strip() for a in args_str.split(",") if a.strip()]
                if inputs:
                    entry["inputs"] = inputs
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

    # Build lineage macro arguments
    lineage_args: list[str] = []
    if verbose_cols:
        lineage_args.append(f"columns={verbose_cols}")
    # Origin tracking — from model def or seed origin
    origin = model_def.get("origin")
    if origin and origin_tracking:
        lineage_args.append(f"origin={origin}")
    # Lineage mode
    if lineage_mode != "full":
        lineage_args.append(f"lineage_mode='{lineage_mode}'")

    if lineage_args:
        col_lines.append(f"    {{{{ dbt_dab_tools.lineage_columns({', '.join(lineage_args)}) }}}}")
    else:
        col_lines.append("    {{ dbt_dab_tools.lineage_columns() }}")

    # Broadcast hint (Databricks join optimisation)
    broadcast = model_def.get("broadcast")
    if broadcast and is_join:
        # broadcast can be a single alias or a list
        aliases = [broadcast] if isinstance(broadcast, str) else list(broadcast)
        hint_list = ", ".join(aliases)
        lines.append(f"select /*+ BROADCAST({hint_list}) */")
    else:
        lines.append("select")
    lines.append(",\n".join(col_lines))

    # FROM clause
    _seeds = seed_names or set()
    # Models that also have domain instances (for ref rewriting)
    _domain_models = model_def.get("_domain_models", set())

    def _ref_or_source(name: str) -> str:
        if name in _seeds:
            return f"{{{{ source('seed', '{name}') }}}}"
        # Domain instances ref domain-specific upstream models
        # but only if the upstream model also has domain instances
        if domain and name in _domain_models:
            domain_ref = f"{name}_{domain}"
            return f"{{{{ ref('{domain_ref}') }}}}"
        return f"{{{{ ref('{name}') }}}}"

    if is_join:
        sources = model_def.get("sources", {})
        join_condition = model_def.get("join", "")
        join_type = model_def.get("join_type", "inner join")

        source_items = list(sources.items())
        first_alias, first_source = source_items[0]
        lines.append(f"from {_ref_or_source(first_source)} {first_alias}")

        for alias, source in source_items[1:]:
            lines.append(f"{join_type} {_ref_or_source(source)} {alias}")
            lines.append(f"    on {join_condition}")
    else:
        source = model_def.get("source", "")
        if source:
            lines.append(f"from {_ref_or_source(source)}")

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


def _compile_table_stub(table_name: str, columns: dict, catalog: str, schema: str) -> str:
    """Generate a CREATE TABLE IF NOT EXISTS stub from a model's column definitions.

    Used to ensure tables exist before UDFs that reference them.
    The stub is idempotent — safe even if the real table is created later.
    """
    fq_name = f"`{catalog}`.`{schema}`.`{table_name}`"
    col_defs = []
    for col_name, col_def in columns.items():
        if isinstance(col_def, str):
            col_def = {"type": col_def}
        elif col_def is None:
            col_def = {}
        col_type = col_def.get("type", "STRING").upper()
        type_map = {"INT": "INT", "STRING": "STRING", "DATE": "DATE",
                    "BOOLEAN": "BOOLEAN", "DOUBLE": "DOUBLE", "FLOAT": "FLOAT",
                    "BIGINT": "BIGINT", "SMALLINT": "SMALLINT", "TIMESTAMP": "TIMESTAMP"}
        resolved_type = type_map.get(col_type, col_type)
        constraints = ""
        if col_def.get("required"):
            constraints += " NOT NULL"
        default = col_def.get("default")
        if default is not None:
            constraints += f" DEFAULT {default}"
        col_defs.append(f"    {col_name} {resolved_type}{constraints}")

    has_defaults = any(
        (col if isinstance(col, dict) else {}).get("default") is not None
        for col in columns.values()
    )
    lines = [
        f"CREATE TABLE IF NOT EXISTS {fq_name} (",
        ",\n".join(col_defs),
        ")",
        "USING DELTA",
    ]
    if has_defaults:
        lines.append("TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');")
    else:
        # Close the statement
        lines[-1] += ";"
    return "\n".join(lines)


# ── Internal table schemas for forge-managed tables ──
# These are used by compile_trace_lineage_udf's depends_on mechanism.

_LINEAGE_GRAPH_COLUMNS: dict = {
    "target_model": {"type": "STRING", "required": True},
    "source_model": {"type": "STRING", "required": True},
    "target_column": "STRING",
    "source_column": "STRING",
    "join_key": "STRING",
    "source_key": "STRING",
    "transform_type": {"type": "STRING", "required": True},
    "expression": "STRING",
    "target_catalog": {"type": "STRING", "required": True},
    "target_schema": {"type": "STRING", "required": True},
    "source_catalog": {"type": "STRING", "required": True},
    "source_schema": {"type": "STRING", "required": True},
    "git_commit": {"type": "STRING", "default": "'unknown'"},
    "updated_at": {"type": "TIMESTAMP", "default": "current_timestamp()"},
}

_LINEAGE_LOG_COLUMNS: dict = {
    "run_id": {"type": "STRING", "required": True},
    "model": {"type": "STRING", "required": True},
    "materialized": "STRING",
    "rows_created": "BIGINT",
    "catalog": "STRING",
    "schema": "STRING",
    "sources": "STRING",
    "git_commit": "STRING",
    "completed_at": {"type": "TIMESTAMP", "default": "current_timestamp()"},
}


def compile_udf_sql(udf_name: str, udf_def: dict, schema: str = "{{ target.catalog }}.{{ target.schema }}", compute_type: str = "serverless") -> str:
    """
    Compile a single UDF definition into a CREATE FUNCTION SQL statement.

    Supports both SQL and Python UDFs on Databricks.
    Uses fully-qualified catalog.schema naming so UDFs are created in the
    same catalog where models will search for them.
    """
    raw_language = udf_def.get("language", "sql").upper()
    # Treat PANDAS as a Python UDF variant — auto-include pandas package
    is_pandas = raw_language == "PANDAS"
    language = "PYTHON" if is_pandas else raw_language

    returns = udf_def.get("returns", "STRING")
    params = udf_def.get("params", [])
    body = udf_def.get("body", "NULL").strip()
    comment = udf_def.get("description", "")
    handler = udf_def.get("handler")
    runtime_version = udf_def.get("runtime_version")
    packages = list(udf_def.get("packages", []))

    # Pandas UDFs always need the pandas package
    if is_pandas and "pandas" not in packages:
        packages.insert(0, "pandas")

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
    if is_pandas:
        lines.append(f"-- Pandas vectorized UDF")
    if comment:
        lines.append(f"-- {comment}")
    # DROP + CREATE is more portable than CREATE OR REPLACE across
    # Databricks warehouse types and Unity Catalog versions.
    fq_name = f"{schema}.{udf_name}"
    lines.append(f"DROP FUNCTION IF EXISTS {fq_name};")
    lines.append(f"CREATE FUNCTION {fq_name}({param_str})")
    lines.append(f"RETURNS {returns}")

    if language == "SQL":
        lines.append(f"RETURN ({body});")
    elif language == "PYTHON":
        lines.append("LANGUAGE PYTHON")
        if compute_type == "serverless":
            # Serverless SQL warehouses don't support RUNTIME_VERSION,
            # PACKAGES, or HANDLER.  The Python function name must match
            # the UDF name.
            adjusted_body = body
            if handler and handler != udf_name:
                adjusted_body = body.replace(f"def {handler}(", f"def {udf_name}(")
            lines.append("AS $$")
            lines.append(adjusted_body)
            lines.append("$$;")
        else:
            if runtime_version:
                lines.append(f"RUNTIME_VERSION = '{runtime_version}'")
            if packages:
                pkg_str = ", ".join(f"'{p}'" for p in packages)
                lines.append(f"PACKAGES ({pkg_str})")
            if handler:
                lines.append(f"HANDLER = '{handler}'")
            lines.append("AS $$")
            lines.append(body)
            lines.append("$$;")
    else:
        lines.append(f"LANGUAGE {language}")
        lines.append("AS $$")
        lines.append(body)
        lines.append("$$;")

    return "\n".join(lines)


def compile_pure_sql_seed(
    seed_name: str,
    seed_def: dict,
    catalog: str,
    schema: str,
    csv_path: Path,
) -> str:
    """Compile a seed into pure SQL: CREATE TABLE IF NOT EXISTS + TRUNCATE + INSERT.

    Reads the CSV file and emits INSERT VALUES rows so the seed data
    lives entirely in SQL — no dbt seed required.
    """
    import csv as csv_mod

    columns = seed_def.get("columns", {})
    fq_table = f"`{catalog}`.`{schema}`.`{seed_name}`"

    lines: list[str] = [
        f"-- Seed: {seed_name}",
        f"-- Source: {csv_path}",
        "-- Generated by: forge compile --pure-sql",
        "",
        f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`;",
        "",
    ]

    # CREATE TABLE IF NOT EXISTS
    col_defs = []
    for col_name, col_def in columns.items():
        if isinstance(col_def, str):
            col_def = {"type": col_def}
        elif col_def is None:
            col_def = {}
        col_type = col_def.get("type", "STRING").upper()
        type_map = {"INT": "INT", "STRING": "STRING", "DATE": "DATE",
                    "BOOLEAN": "BOOLEAN", "DOUBLE": "DOUBLE", "FLOAT": "FLOAT",
                    "BIGINT": "BIGINT", "SMALLINT": "SMALLINT", "TIMESTAMP": "TIMESTAMP"}
        resolved_type = type_map.get(col_type, col_type)
        constraints = ""
        if col_def.get("required"):
            constraints += " NOT NULL"
        col_defs.append(f"    {col_name} {resolved_type}{constraints}")
    lines.append(f"CREATE TABLE IF NOT EXISTS {fq_table} (")
    lines.append(",\n".join(col_defs))
    lines.append(") USING DELTA;")
    lines.append("")

    # TRUNCATE + INSERT from CSV
    lines.append(f"TRUNCATE TABLE {fq_table};")
    lines.append("")

    if not csv_path.exists():
        lines.append(f"-- WARNING: CSV file not found: {csv_path}")
        return "\n".join(lines) + "\n"

    # Read CSV and generate INSERT VALUES
    col_names = list(columns.keys())
    col_types = {}
    for col_name, col_def in columns.items():
        if isinstance(col_def, str):
            col_types[col_name] = col_def.upper()
        elif isinstance(col_def, dict):
            col_types[col_name] = col_def.get("type", "STRING").upper()
        else:
            col_types[col_name] = "STRING"

    with open(csv_path, newline="") as f:
        reader = csv_mod.DictReader(f)
        value_rows: list[str] = []
        for row in reader:
            vals: list[str] = []
            for col_name in col_names:
                raw = row.get(col_name, "").strip()
                ctype = col_types.get(col_name, "STRING")
                if ctype in ("INT", "BIGINT", "SMALLINT", "FLOAT", "DOUBLE"):
                    vals.append(raw if raw else "NULL")
                elif ctype == "BOOLEAN":
                    vals.append(raw.lower() if raw else "NULL")
                else:
                    escaped = raw.replace("'", "''")
                    vals.append(f"'{escaped}'")
            value_rows.append(f"  ({', '.join(vals)})")

    if value_rows:
        col_list = ", ".join(col_names)
        lines.append(f"INSERT INTO {fq_table} ({col_list}) VALUES")
        lines.append(",\n".join(value_rows) + ";")
        lines.append("")

    # Summary
    lines.append("-- Execution summary")
    lines.append("SELECT")
    lines.append(f"    '{seed_name}' AS seed,")
    lines.append(f"    (SELECT COUNT(*) FROM {fq_table}) AS rows_loaded,")
    lines.append("    current_timestamp() AS completed_at;")

    return "\n".join(lines) + "\n"


def compile_all_udfs(ddl_path: Path, forge_config: dict | None = None) -> dict[str, str]:
    """
    Compile all UDFs from models.yml.

    Returns: {udf_name: sql_statement}
    """
    udfs = load_udfs(ddl_path, forge_config=forge_config)
    results: dict[str, str] = {}
    for udf_name, udf_def in udfs.items():
        results[udf_name] = compile_udf_sql(udf_name, udf_def)
    return results


def compile_udfs_to_dir(ddl_path: Path, output_dir: Path, forge_config: dict | None = None) -> dict[str, Path]:
    """Compile each UDF to its own .sql file in output_dir."""
    udfs = compile_all_udfs(ddl_path, forge_config=forge_config)
    if not udfs:
        return {}
    # Clean stale UDF files before writing
    if output_dir.exists():
        for old_sql in output_dir.glob("*.sql"):
            old_sql.unlink()
    output_dir.mkdir(parents=True, exist_ok=True)
    results: dict[str, Path] = {}
    for udf_name, udf_sql in udfs.items():
        out_path = output_dir / f"{udf_name}.sql"
        out_path.write_text(udf_sql + "\n")
        results[udf_name] = out_path
    return results


def compile_sources_yml(ddl_path: Path, source_name: str = "seed", forge_config: dict | None = None) -> str | None:
    """Generate dbt sources YAML from seed definitions in DDL.

    Seeds are declared as a dbt source so models can reference them
    via ``{{ source('seed', 'raw_customers') }}``.  The source
    schema defaults to ``{{ target.schema }}`` so it matches wherever
    ``dbt seed`` loads the CSV data.

    Seeds with ``catalog:`` or ``schema:`` overrides are placed in a
    separate source group so dbt routes them to the correct location.
    """
    seeds = load_seeds(ddl_path, forge_config=forge_config)
    if not seeds:
        return None

    # Group seeds: default vs overridden
    default_tables = []
    override_groups: dict[tuple[str, str], list[dict]] = {}

    for seed_name, seed_def in seeds.items():
        seed_def = seed_def or {}
        table: dict[str, Any] = {"name": seed_name}
        desc = seed_def.get("description")
        if desc:
            table["description"] = desc
        cols = seed_def.get("columns", {})
        if cols:
            col_list = []
            for col_name, col_def in cols.items():
                if isinstance(col_def, str):
                    col_def = {"type": col_def}
                elif col_def is None:
                    col_def = {}
                col_entry: dict[str, str] = {"name": col_name}
                col_desc = col_def.get("description")
                if col_desc:
                    col_entry["description"] = col_desc
                col_list.append(col_entry)
            table["columns"] = col_list

        seed_catalog = seed_def.get("catalog")
        seed_schema = seed_def.get("schema")
        if seed_catalog or seed_schema:
            key = (seed_catalog or "", seed_schema or "")
            override_groups.setdefault(key, []).append(table)
        else:
            default_tables.append(table)

    sources_list: list[dict] = []

    if default_tables:
        sources_list.append({
            "name": source_name,
            "description": "Seed data loaded by dbt seed",
            "schema": "{{ target.schema }}",
            "tables": default_tables,
        })

    # Each override group gets its own source entry
    for (cat, sch), tables in override_groups.items():
        group_name = f"{source_name}_{cat or 'default'}_{sch or 'default'}".replace("-", "_")
        entry: dict[str, Any] = {
            "name": group_name,
            "description": f"Seed data in {cat or 'default'}.{sch or 'default'}",
            "tables": tables,
        }
        if cat:
            # Use var() so the catalog resolves via forge's naming pattern
            # e.g. catalog: meta → var('catalog_meta', 'dev_fd_meta')
            var_name = f"catalog_{cat}"
            entry["database"] = "{{ var('" + var_name + "') }}"
        if sch:
            entry["schema"] = sch
        else:
            entry["schema"] = "{{ target.schema }}"
        sources_list.append(entry)

    if not sources_list:
        return None

    source_block = {
        "version": 2,
        "sources": sources_list,
    }
    return yaml.dump(source_block, default_flow_style=False, sort_keys=False)


_GENERATE_SCHEMA_NAME_SQL = """\
{% macro generate_schema_name(custom_schema_name, node) -%}
    {#  Forge-managed: use the custom schema directly.
        dbt's default concatenates default_schema + custom_schema,
        which doubles the name when both resolve to the same value.
        Since forge resolves full schema names via var("schema_*"),
        we use the custom schema as-is.  #}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}
"""


def _ensure_generate_schema_name_macro(forge_config: dict | None = None) -> None:
    """Write the generate_schema_name macro if it doesn't already exist
    or if it was previously generated by forge (contains the sentinel comment)."""
    macro_dir = Path("macros")
    macro_dir.mkdir(parents=True, exist_ok=True)
    macro_path = macro_dir / "generate_schema_name.sql"

    if macro_path.exists():
        content = macro_path.read_text()
        if "Forge-managed" not in content:
            return  # user has a custom override — don't touch it

    macro_path.write_text(_GENERATE_SCHEMA_NAME_SQL)


# =============================================
# VOLUME COMPILER
# =============================================

def compile_volume_sql(
    volume_name: str,
    volume_def: dict,
    catalog: str,
    schema: str,
) -> str:
    """Compile a volume definition to a CREATE VOLUME SQL statement.

    Example output:
      CREATE EXTERNAL VOLUME IF NOT EXISTS `catalog`.`schema`.`landing`
        LOCATION 's3://bucket/landing/';
    """
    vol_type = volume_def.get("type", "managed").upper()
    location = volume_def.get("location", "")

    lines = [f"-- Volume: {volume_name}"]
    lines.append(f"-- Generated by: forge compile")

    if vol_type == "EXTERNAL":
        lines.append(
            f"CREATE EXTERNAL VOLUME IF NOT EXISTS "
            f"`{catalog}`.`{schema}`.`{volume_name}`"
        )
        if location:
            lines.append(f"  LOCATION '{location}';")
        else:
            lines.append(";")
    else:
        lines.append(
            f"CREATE VOLUME IF NOT EXISTS "
            f"`{catalog}`.`{schema}`.`{volume_name}`;"
        )

    desc = volume_def.get("description", "")
    if desc:
        lines.append(
            f"COMMENT ON VOLUME `{catalog}`.`{schema}`.`{volume_name}` "
            f"IS '{desc}';"
        )

    return "\n".join(lines) + "\n"


def compile_volumes(
    ddl_path: Path,
    output_dir: Path,
    forge_config: dict | None = None,
) -> dict[str, Path]:
    """Compile all volume definitions to SQL files.

    Supports domain-aware volumes: when ``domain: true`` (or inherited
    from domain_layers), a separate CREATE VOLUME is emitted per domain.

    Output goes directly to ``output_dir``.
    """
    volumes = load_volumes(ddl_path, forge_config=forge_config)
    if not volumes:
        return {}

    output_dir.mkdir(parents=True, exist_ok=True)

    domains = (forge_config or {}).get("domains", {})
    domain_layers = set((forge_config or {}).get("domain_layers", []))

    results: dict[str, Path] = {}

    for vol_name, vol_def in volumes.items():
        vol_layer = vol_def.get("layer", "bronze")
        vol_domain_enabled = vol_def.get("domain", True) is not False
        is_bifurcated = domains and vol_layer in domain_layers and vol_domain_enabled

        # Resolve catalog and schema using patterns from forge_config
        resolved_catalog = _resolve_catalog(forge_config, vol_layer) if forge_config else vol_layer
        base_schema = _resolve_base_schema(forge_config, vol_layer) if forge_config else "default"

        if is_bifurcated:
            for domain_name, domain_cfg in domains.items():
                suffix = domain_cfg.get("schema_suffix", f"_{domain_name}")
                instance_name = f"{vol_name}_{domain_name}"
                d_schema = f"{base_schema}{suffix}"

                # Support per-domain location overrides
                d_def = {**vol_def}
                domain_locs = vol_def.get("domain_locations", {})
                if domain_name in domain_locs:
                    d_def["location"] = domain_locs[domain_name]
                elif vol_def.get("location"):
                    d_def["location"] = vol_def["location"].rstrip("/") + f"/{domain_name}/"

                sql = compile_volume_sql(instance_name, d_def, resolved_catalog, d_schema)
                out_path = output_dir / f"{instance_name}.sql"
                out_path.write_text(sql)
                results[instance_name] = out_path
        else:
            sql = compile_volume_sql(vol_name, vol_def, resolved_catalog, base_schema)
            out_path = output_dir / f"{vol_name}.sql"
            out_path.write_text(sql)
            results[vol_name] = out_path

    return results


# =============================================
# FULL COMPILE: models.yml → .sql + schema.yml
# =============================================

def compile_all(
    ddl_path: Path,
    output_dir: Path,
    schema_output: Path | None = None,
    forge_config: dict | None = None,
) -> dict[str, Path]:
    """
    Compile DDL into individual .sql files + schema.yml.

    Also generates:
      dbt/functions/{udf}.sql   — one file per UDF
      dbt/sources/_sources.yml  — dbt source config for seeds

    Output structure (when forge_config is available):
      {output_dir}/{origin}/{layer}/{model}.sql

    Where:
      origin = project id (from forge_config) or 'project'
      layer  = bronze / silver / gold (auto-detected from model name)

    Returns a dict of {model_name: output_path} for all generated files.
    """
    models = load_ddl(ddl_path, forge_config=forge_config)
    seeds = load_seeds(ddl_path, forge_config=forge_config)
    seed_names = set(seeds.keys())
    output_dir.mkdir(parents=True, exist_ok=True)
    results: dict[str, Path] = {}

    origin = (forge_config or {}).get("id", "project")

    # Clean stale files: remove flat .sql and previous origin subdirectory
    for old_sql in output_dir.glob("*.sql"):
        if old_sql.name != "_udfs.sql":
            old_sql.unlink()
    origin_dir = output_dir / origin
    if origin_dir.is_dir():
        import shutil
        shutil.rmtree(origin_dir)
    # Remove legacy _udfs.sql from models dir (UDFs now go to dbt/functions/)
    legacy_udfs = output_dir / "_udfs.sql"
    if legacy_udfs.exists():
        legacy_udfs.unlink()

    # Build UDF language lookup once for lineage enrichment
    udf_lang_map = {n: d.get("language", "sql") for n, d in load_udfs(ddl_path, forge_config=forge_config).items()}

    # Lineage settings from forge_config
    lineage_cfg = (forge_config or {}).get("lineage", {})
    lineage_mode = lineage_cfg.get("mode", "full")
    origin_tracking = lineage_cfg.get("origin_tracking", True)

    # Domain config
    domains = (forge_config or {}).get("domains", {})
    domain_layers = set((forge_config or {}).get("domain_layers", list((forge_config or {}).get("schemas", []))))

    # Track domain instances for schema.yml generation
    domain_models: dict[str, dict] = {}

    # Pre-compute which models are in domain layers (for ref rewriting)
    # Models with domain: false are excluded — they stay shared
    domain_model_names: set[str] = set()
    if domains:
        for name, mdef in models.items():
            if mdef.get("layer", "default") in domain_layers and mdef.get("domain", True) is not False:
                domain_model_names.add(name)

    for model_name, model_def in models.items():
        layer = model_def.get("layer", "default")
        sub_dir = output_dir / origin / layer
        sub_dir.mkdir(parents=True, exist_ok=True)
        model_domain_enabled = model_def.get("domain", True) is not False
        is_bifurcated = domains and layer in domain_layers and model_domain_enabled

        # Default instance: skip if this layer is fully bifurcated into domains
        if not is_bifurcated:
            sql = compile_model(model_name, model_def, forge_config=forge_config, seed_names=seed_names,
                                udf_languages=udf_lang_map, lineage_mode=lineage_mode,
                                origin_tracking=origin_tracking)
            out_path = sub_dir / f"{model_name}.sql"
            out_path.write_text(sql)
            results[model_name] = out_path

        # Domain instances: generate one .sql per (model x domain)
        if is_bifurcated:
            base_schema = _resolve_base_schema(forge_config, layer) if forge_config else None
            for domain_name, domain_cfg in domains.items():
                suffix = domain_cfg.get("schema_suffix", f"_{domain_name}")
                instance_name = f"{model_name}_{domain_name}"
                domain_def = {**model_def, "_domain": domain_name, "_domain_models": domain_model_names}
                if base_schema:
                    domain_def["schema"] = f"{base_schema}{suffix}"

                # Domain source routing: swap source per domain if domain_sources defined
                # domain_sources.{domain}: "source_name"        → replaces source: key
                # domain_sources.{domain}: {alias: source_name} → merges into sources: key (joins)
                ds = model_def.get("domain_sources", {})
                if domain_name in ds:
                    ds_val = ds[domain_name]
                    if isinstance(ds_val, str):
                        domain_def["source"] = ds_val
                    elif isinstance(ds_val, dict):
                        merged = {**model_def.get("sources", {}), **ds_val}
                        domain_def["sources"] = merged

                domain_sql = compile_model(
                    instance_name, domain_def, forge_config=forge_config,
                    seed_names=seed_names, udf_languages=udf_lang_map,
                    lineage_mode=lineage_mode, origin_tracking=origin_tracking,
                )
                domain_dir = sub_dir / domain_name
                domain_dir.mkdir(parents=True, exist_ok=True)
                out_path = domain_dir / f"{instance_name}.sql"
                out_path.write_text(domain_sql)
                results[instance_name] = out_path
                domain_models[instance_name] = domain_def

    # Merge domain instances into models for schema.yml generation
    # Exclude default instances of fully bifurcated layers (but keep domain: false models)
    base_models = {k: v for k, v in models.items()
                   if not (domains and v.get("layer", "default") in domain_layers
                           and v.get("domain", True) is not False)}
    all_models = {**base_models, **domain_models}

    # Generate schema.yml (includes domain instances)
    schema_yml = compile_schema_yml(all_models)
    schema_path = schema_output or (output_dir / "schema.yml")
    schema_path.write_text(schema_yml)
    results["_schema"] = schema_path

    # Generate UDFs → dbt/functions/ (one file per UDF)
    functions_dir = output_dir.parent / "functions"
    udf_results = compile_udfs_to_dir(ddl_path, functions_dir, forge_config=forge_config)
    for udf_name, udf_path in udf_results.items():
        results[f"_udf_{udf_name}"] = udf_path

    # Generate sources YAML → dbt/sources/ (seed declarations)
    sources_yml = compile_sources_yml(ddl_path, forge_config=forge_config)
    if sources_yml:
        sources_dir = output_dir.parent / "sources"
        # Clean stale source files
        if sources_dir.exists():
            for old_yml in sources_dir.glob("*.yml"):
                old_yml.unlink()
        sources_dir.mkdir(parents=True, exist_ok=True)
        sources_path = sources_dir / "_sources.yml"
        sources_path.write_text(sources_yml)
        results["_sources"] = sources_path

    # Generate Volume DDL → dbt/volumes/ (CREATE VOLUME SQL, deployed separately)
    # Volumes go to a separate directory, NOT inside models/, to avoid dbt treating them as models
    volumes_output_dir = output_dir.parent / "volumes"
    volume_results = compile_volumes(ddl_path, volumes_output_dir, forge_config=forge_config)
    for vol_name, vol_path in volume_results.items():
        results[f"_volume_{vol_name}"] = vol_path

    # Generate generate_schema_name macro so dbt uses the schema as-is
    # (dbt's default concatenates default_schema + custom_schema)
    _ensure_generate_schema_name_macro(forge_config)

    return results


# =============================================
# PURE-SQL COMPILER (no dbt / no Jinja at runtime)
# =============================================
# Emits standalone SQL files that run directly on a
# Databricks SQL warehouse — zero dbt installation needed.
#
#   forge compile --pure-sql
#
# Output (in sql/ folder, numbered for execution order):
#   sql/000_udfs.sql          ← CREATE FUNCTION statements
#   sql/001_stg_customers.sql ← CREATE VIEW AS SELECT ...
#   sql/002_stg_orders.sql
#   sql/003_customer_clean.sql
#   sql/003_customer_clean_quarantine.sql
#   sql/004_customer_orders.sql
#   sql/005_customer_summary.sql
#
# Every file is executable as-is with `databricks sql` or
# a SQL warehouse REST call. No Python. No cold starts.

def topo_sort_models(models: dict[str, dict]) -> list[str]:
    """
    Topologically sort models by their source dependencies.

    Returns model names in execution order (sources before dependents).
    """
    # Build adjacency: model → set of models it depends on
    deps: dict[str, set[str]] = {}
    model_names = set(models.keys())

    for name, mdef in models.items():
        d: set[str] = set()
        if "source" in mdef and mdef["source"] in model_names:
            d.add(mdef["source"])
        for src in mdef.get("sources", {}).values():
            if src in model_names:
                d.add(src)
        deps[name] = d

    # Kahn's algorithm
    in_degree = {n: len(deps[n]) for n in model_names}
    queue = [n for n in model_names if in_degree[n] == 0]
    ordered: list[str] = []

    while queue:
        queue.sort()  # deterministic ordering within same level
        node = queue.pop(0)
        ordered.append(node)
        for n in model_names:
            if node in deps[n]:
                in_degree[n] -= 1
                if in_degree[n] == 0:
                    queue.append(n)

    return ordered


def _compile_lineage_struct(
    model_name: str,
    model_def: dict,
    schema: str,
    git_commit: str,
    deploy_ts: str,
    compute_type: str,
    columns: dict[str, dict],
    platform: str = "databricks",
) -> str:
    """
    Compile static lineage metadata as a named_struct() / jsonb_build_object().

    No Jinja. No dbt context. Pure SQL.
    """
    version = model_def.get("version", "v1")
    # Collect source names
    source_names: list[str] = []
    if "source" in model_def:
        source_names.append(model_def["source"])
    for src in model_def.get("sources", {}).values():
        source_names.append(src)

    contract_id = f"{schema}.{model_name}"
    sources_str = ", ".join(source_names)

    # Build per-column lineage entries
    col_entries: list[dict[str, Any]] = []
    for col_name, col_def in columns.items():
        if isinstance(col_def, str):
            col_def = {"type": col_def}
        elif col_def is None:
            col_def = {}

        entry: dict[str, str] = {"name": col_name}
        if "udf" in col_def:
            entry["expr"] = col_def["udf"]
            entry["op"] = "UDF"
        elif "expr" in col_def:
            entry["expr"] = col_def["expr"]
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
        else:
            entry["op"] = "PASSTHROUGH"

        col_entries.append(entry)

    if platform in ("databricks", "spark"):
        # Build column array
        if col_entries:
            col_parts = []
            for e in col_entries:
                inputs_key = e.get("name", "")
                col_parts.append(
                    f"        named_struct(\n"
                    f"            'name', '{e['name']}',\n"
                    f"            'expression', '{e.get('expr', e['name'])}',\n"
                    f"            'op', '{e['op']}'\n"
                    f"        )"
                )
            cols_sql = "array(\n" + ",\n".join(col_parts) + "\n    )"
        else:
            cols_sql = "cast(null as array<struct<name:string, expression:string, op:string>>)"

        return (
            f"    named_struct(\n"
            f"        'schema_version', '3',\n"
            f"        'model', '{model_name}',\n"
            f"        'sources', '{sources_str}',\n"
            f"        'git_commit', '{git_commit}',\n"
            f"        'deployed_at', '{deploy_ts}',\n"
            f"        'compute_type', '{compute_type}',\n"
            f"        'contract_id', '{contract_id}',\n"
            f"        'version', '{version}',\n"
            f"        'columns', {cols_sql}\n"
            f"    ) AS _lineage"
        )
    else:
        # Postgres / generic fallback
        import json
        meta = {
            "schema_version": "3",
            "model": model_name,
            "sources": sources_str,
            "git_commit": git_commit,
            "deployed_at": deploy_ts,
            "compute_type": compute_type,
            "contract_id": contract_id,
            "version": version,
            "columns": [{"name": e["name"], "expression": e.get("expr", e["name"]), "op": e["op"]} for e in col_entries],
        }
        json_str = json.dumps(meta).replace("'", "''")
        return f"    cast('{json_str}' as varchar(8000)) AS _lineage"


def compile_pure_sql_model(
    model_name: str,
    model_def: dict,
    catalog: str,
    schema: str,
    git_commit: str = "unknown",
    deploy_ts: str = "",
    compute_type: str = "serverless",
    platform: str = "databricks",
    source_locations: dict[str, tuple[str, str]] | None = None,
    udf_qualifiers: dict[str, str] | None = None,
) -> str:
    """
    Compile a model to standalone SQL — no Jinja, no dbt.

    Tables: CREATE TABLE IF NOT EXISTS + TRUNCATE + INSERT INTO.
    Views: CREATE OR REPLACE VIEW.
    source_locations maps model_name → (catalog, schema) for cross-schema refs.

    Models with managed_by: python emit CREATE TABLE IF NOT EXISTS (schema only)
    — the python task handles inserts.
    """
    if not deploy_ts:
        from datetime import datetime, timezone
        deploy_ts = datetime.now(timezone.utc).isoformat()

    managed_by = model_def.get("managed_by", "")
    columns = model_def.get("columns", {})

    # ── Python-managed tables: DDL only (no SELECT, no data) ──
    if managed_by:
        fq_name = f"`{catalog}`.`{schema}`.`{model_name}`"

        lines: list[str] = []
        lines.append(f"-- Model: {model_name}")
        lines.append(f"-- Managed by: {managed_by}")
        lines.append(f"-- Schema created by forge; data populated by {managed_by} task.")
        lines.append(f"-- Generated by: forge compile --pure-sql")
        lines.append("")

        # Each SQL file is an independent task — ensure schema exists
        lines.append(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`;")
        lines.append("")

        # Build typed column list
        col_defs = []
        for col_name, col_def in columns.items():
            if isinstance(col_def, str):
                col_def = {"type": col_def}
            elif col_def is None:
                col_def = {}
            col_type = col_def.get("type", "STRING").upper()
            # Map shorthand types
            type_map = {"INT": "INT", "STRING": "STRING", "DATE": "DATE",
                        "BOOLEAN": "BOOLEAN", "DOUBLE": "DOUBLE", "FLOAT": "FLOAT",
                        "BIGINT": "BIGINT", "SMALLINT": "SMALLINT", "TIMESTAMP": "TIMESTAMP"}
            resolved_type = type_map.get(col_type, col_type)
            constraints = ""
            if col_def.get("required"):
                constraints += " NOT NULL"
            col_defs.append(f"    {col_name} {resolved_type}{constraints}")

        lines.append(f"CREATE TABLE IF NOT EXISTS {fq_name} (")
        lines.append(",\n".join(col_defs))
        lines.append(")")
        lines.append("USING DELTA;")
        lines.append("")
        lines.append(f"-- Execution summary")
        lines.append(f"SELECT")
        lines.append(f"    '{model_name}' AS model,")
        lines.append(f"    'managed_by_{managed_by}' AS materialized,")
        lines.append(f"    (SELECT COUNT(*) FROM {fq_name}) AS rows_created,")
        lines.append(f"    current_timestamp() AS completed_at;")

        return "\n".join(lines) + "\n"

    materialized = model_def.get("materialized", "view")
    columns = model_def.get("columns", {})
    is_join = "sources" in model_def or "join" in model_def
    is_agg = "group_by" in model_def

    lines: list[str] = []
    lines.append(f"-- Model: {model_name}")
    lines.append(f"-- Generated by: forge compile --pure-sql")
    lines.append(f"-- No dbt required. Runs directly on SQL warehouse.")
    lines.append("")

    # Each SQL file is an independent task — ensure schema exists
    lines.append(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`;")
    lines.append("")

    # DDL wrapper
    fq_name = f"`{catalog}`.`{schema}`.`{model_name}`"
    if materialized == "view":
        lines.append(f"CREATE OR REPLACE VIEW {fq_name} AS")
    elif materialized == "incremental":
        # Incremental: INSERT INTO (append new rows only)
        # The table must already exist (SETUP creates it).
        lines.append(f"INSERT INTO {fq_name}")
    else:
        # Table: CREATE IF NOT EXISTS + TRUNCATE + INSERT.
        # On first run, creates the table with correct schema.
        # On subsequent runs, CREATE is a no-op.
        # TRUNCATE clears data while preserving schema/metadata/grants.
        # INSERT INTO repopulates.

        # Build column definitions for CREATE TABLE
        col_defs: list[str] = []
        for col_name_raw, col_def_raw in columns.items():
            cd = col_def_raw
            if isinstance(cd, str):
                cd = {"type": cd}
            elif cd is None:
                cd = {}
            col_type = cd.get("type", "STRING").upper()
            constraints = ""
            if cd.get("required"):
                constraints += " NOT NULL"
            col_defs.append(f"    {col_name_raw} {col_type}{constraints}")

        # Add _lineage metadata column
        if platform in ("databricks", "spark"):
            lineage_type = (
                "STRUCT<schema_version: STRING, model: STRING, sources: STRING, "
                "git_commit: STRING, deployed_at: STRING, compute_type: STRING, "
                "contract_id: STRING, version: STRING, "
                "columns: ARRAY<STRUCT<name: STRING, expression: STRING, op: STRING>>>"
            )
        else:
            lineage_type = "VARCHAR(8000)"
        col_defs.append(f"    _lineage {lineage_type}")

        lines.append(f"CREATE TABLE IF NOT EXISTS {fq_name} (")
        lines.append(",\n".join(col_defs))
        if platform in ("databricks", "spark"):
            lines.append(") USING DELTA;")
        else:
            lines.append(");")
        lines.append("")
        lines.append(f"TRUNCATE TABLE {fq_name};")
        lines.append("")
        lines.append(f"INSERT INTO {fq_name}")

    # SELECT columns
    # Detect if any UDF references an aggregated sibling — if so, use CTE
    has_udf_on_aggregate = False
    if is_agg:
        for col_name, col_def in columns.items():
            cd = col_def if isinstance(col_def, dict) else {}
            if "udf" not in cd:
                continue
            # Check if any argument references a sibling with expr
            udf_call = cd["udf"]
            args_start = udf_call.index("(")
            args_str = udf_call[args_start + 1 : udf_call.rindex(")")]
            for arg in (a.strip() for a in args_str.split(",")):
                sibling = columns.get(arg)
                if isinstance(sibling, dict) and "expr" in sibling:
                    has_udf_on_aggregate = True
                    break
            if has_udf_on_aggregate:
                break

    if has_udf_on_aggregate:
        # CTE approach: aggregate in inner query, apply UDFs in outer query
        # Inner CTE: all non-UDF columns with their expressions
        inner_col_lines = []
        for col_name, col_def in columns.items():
            cd = col_def
            if isinstance(cd, str):
                cd = {"type": cd}
            elif cd is None:
                cd = {}
            if "udf" in cd:
                continue  # skip UDF columns — computed in outer SELECT
            inner_col_lines.append(_compile_column_select(col_name, cd, None, None, all_columns=columns))

        # FROM clause
        def _fq(source_name: str) -> str:
            if source_locations and source_name in source_locations:
                s_cat, s_sch = source_locations[source_name]
                return f"`{s_cat}`.`{s_sch}`.`{source_name}`"
            return f"`{catalog}`.`{schema}`.`{source_name}`"

        from_lines: list[str] = []
        if is_join:
            sources = model_def.get("sources", {})
            join_condition = model_def.get("join", "")
            join_type = model_def.get("join_type", "INNER JOIN")
            source_items = list(sources.items())
            first_alias, first_source = source_items[0]
            from_lines.append(f"    FROM {_fq(first_source)} {first_alias}")
            for alias, source in source_items[1:]:
                from_lines.append(f"    {join_type} {_fq(source)} {alias}")
                from_lines.append(f"        ON {join_condition}")
        else:
            source = model_def.get("source", "")
            if source:
                from_lines.append(f"    FROM {_fq(source)}")

        group_cols = model_def.get("group_by", [])

        lines.append(f"WITH _agg AS (")
        lines.append("    SELECT")
        lines.append(",\n".join(f"    {cl}" for cl in inner_col_lines))
        lines.extend(from_lines)
        if group_cols:
            lines.append(f"    GROUP BY")
            lines.append(f"        {', '.join(group_cols)}")
        lines.append(")")

        # Outer SELECT: passthrough all CTE columns + apply UDFs + lineage
        outer_col_lines = []
        for col_name, col_def in columns.items():
            cd = col_def
            if isinstance(cd, str):
                cd = {"type": cd}
            elif cd is None:
                cd = {}
            if "udf" in cd:
                # UDF on CTE column — arguments are now plain column refs
                outer_col_lines.append(_compile_column_select(col_name, cd, None, udf_qualifiers))
            else:
                outer_col_lines.append(f"    {col_name}")

        lineage_sql = _compile_lineage_struct(
            model_name, model_def, schema, git_commit, deploy_ts, compute_type, columns, platform,
        )
        outer_col_lines.append(lineage_sql)

        lines.append("SELECT")
        lines.append(",\n".join(outer_col_lines))
        lines.append("FROM _agg")
        lines.append(";")
    else:
        # Standard path: no CTE needed
        col_lines = []
        for col_name, col_def in columns.items():
            if isinstance(col_def, str):
                col_def = {"type": col_def}
            elif col_def is None:
                col_def = {}
            col_lines.append(_compile_column_select(col_name, col_def, None, udf_qualifiers, all_columns=columns))

        # Lineage struct (static, no Jinja)
        lineage_sql = _compile_lineage_struct(
            model_name, model_def, schema, git_commit, deploy_ts, compute_type, columns, platform,
        )
        col_lines.append(lineage_sql)

        lines.append("SELECT")
        lines.append(",\n".join(col_lines))

        # FROM clause — fully qualified table names instead of {{ ref() }}
        # Use source_locations to resolve each source to its correct schema
        def _fq(source_name: str) -> str:
            if source_locations and source_name in source_locations:
                s_cat, s_sch = source_locations[source_name]
                return f"`{s_cat}`.`{s_sch}`.`{source_name}`"
            return f"`{catalog}`.`{schema}`.`{source_name}`"

        if is_join:
            sources = model_def.get("sources", {})
            join_condition = model_def.get("join", "")
            join_type = model_def.get("join_type", "INNER JOIN")

            source_items = list(sources.items())
            first_alias, first_source = source_items[0]
            lines.append(f"FROM {_fq(first_source)} {first_alias}")

            for alias, source in source_items[1:]:
                lines.append(f"{join_type} {_fq(source)} {alias}")
                lines.append(f"    ON {join_condition}")
        else:
            source = model_def.get("source", "")
            if source:
                lines.append(f"FROM {_fq(source)}")

        # GROUP BY
        if is_agg:
            group_cols = model_def.get("group_by", [])
            if group_cols:
                lines.append("GROUP BY")
                lines.append(f"    {', '.join(group_cols)}")

        lines.append(";")

    # ── Execution summary (shown as sql_task output) ──
    fq_name = f"`{catalog}`.`{schema}`.`{model_name}`"
    lines.append("")
    lines.append(f"-- Execution summary")
    lines.append(f"SELECT")
    lines.append(f"    '{model_name}' AS model,")
    lines.append(f"    '{materialized}' AS materialized,")
    lines.append(f"    (SELECT COUNT(*) FROM {fq_name}) AS rows_created,")
    lines.append(f"    current_timestamp() AS completed_at;")

    return "\n".join(lines) + "\n"


def compile_pure_sql_quarantine(
    model_name: str,
    condition: str,
    catalog: str,
    schema: str,
    git_commit: str = "unknown",
    deploy_ts: str = "",
) -> str:
    """
    Compile quarantine as a standalone SQL statement.

    Creates a sidecar table with rows matching the failing condition.
    """
    if not deploy_ts:
        from datetime import datetime, timezone
        deploy_ts = datetime.now(timezone.utc).isoformat()

    fq_model = f"`{catalog}`.`{schema}`.`{model_name}`"
    fq_quarantine = f"`{catalog}`.`{schema}`.`{model_name}_quarantine`"

    return (
        f"-- Quarantine: {model_name}\n"
        f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`;\n"
        f"\n"
        f"CREATE OR REPLACE TABLE {fq_quarantine}\n"
        f"USING DELTA\n"
        f"AS\n"
        f"SELECT *,\n"
        f"    '{model_name}' AS _quarantine_source,\n"
        f"    '{git_commit}' AS _quarantine_git_commit,\n"
        f"    '{deploy_ts}' AS _quarantine_detected_at\n"
        f"FROM {fq_model}\n"
        f"WHERE {condition};\n"
        f"\n"
        f"-- Execution summary\n"
        f"SELECT\n"
        f"    '{model_name}_quarantine' AS model,\n"
        f"    'quarantine' AS materialized,\n"
        f"    (SELECT COUNT(*) FROM {fq_quarantine}) AS rows_quarantined,\n"
        f"    (SELECT COUNT(*) FROM {fq_model}) AS rows_passed,\n"
        f"    current_timestamp() AS completed_at;\n"
    )


def compile_lineage_graph_sql(
    models: dict,
    model_locations: dict[str, tuple[str, str]],
    meta_catalog: str,
    meta_schema: str,
    git_commit: str = "unknown",
    deploy_ts: str = "",
) -> str:
    """
    Compile the lineage_graph + lineage_log tables and seed INSERT statements.

    lineage_graph stores the static DAG edges (source→target, join keys,
    column mappings, transform types, and the SQL expression for aggregations).
    lineage_log stores per-run execution records with job_run_id.

    Both live in the meta catalog so they don't pollute data catalogs.
    """
    fq_graph = f"`{meta_catalog}`.`{meta_schema}`.lineage_graph"
    fq_log = f"`{meta_catalog}`.`{meta_schema}`.lineage_log"

    lines: list[str] = []
    lines.append("-- ============================================")
    lines.append("-- Lineage Graph + Log (auto-generated by forge)")
    lines.append("-- ============================================")
    lines.append("")

    # Each SQL file runs as an independent task — must be self-sufficient
    lines.append(f"CREATE SCHEMA IF NOT EXISTS `{meta_catalog}`.`{meta_schema}`;")
    lines.append("")

    # ── lineage_graph table ──
    lines.append(f"CREATE TABLE IF NOT EXISTS {fq_graph} (")
    lines.append("    target_model    STRING     NOT NULL,")
    lines.append("    source_model    STRING     NOT NULL,")
    lines.append("    target_column   STRING,")
    lines.append("    source_column   STRING,")
    lines.append("    join_key        STRING,")
    lines.append("    source_key      STRING,")
    lines.append("    transform_type  STRING     NOT NULL,")
    lines.append("    expression      STRING,")
    lines.append("    target_catalog  STRING     NOT NULL,")
    lines.append("    target_schema   STRING     NOT NULL,")
    lines.append("    source_catalog  STRING     NOT NULL,")
    lines.append("    source_schema   STRING     NOT NULL,")
    lines.append(f"    git_commit      STRING     DEFAULT '{git_commit}',")
    lines.append(f"    updated_at      TIMESTAMP  DEFAULT current_timestamp()")
    lines.append(")")
    lines.append("USING DELTA")
    lines.append("TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');")
    lines.append("")

    # ── lineage_log table ──
    lines.append(f"CREATE TABLE IF NOT EXISTS {fq_log} (")
    lines.append("    run_id          STRING     NOT NULL,")
    lines.append("    model           STRING     NOT NULL,")
    lines.append("    materialized    STRING,")
    lines.append("    rows_created    BIGINT,")
    lines.append("    catalog         STRING,")
    lines.append("    schema          STRING,")
    lines.append("    sources         STRING,")
    lines.append("    git_commit      STRING,")
    lines.append("    completed_at    TIMESTAMP  DEFAULT current_timestamp()")
    lines.append(")")
    lines.append("USING DELTA")
    lines.append("TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');")
    lines.append("")

    # ── Truncate + reload lineage_graph ──
    # Full reload each deploy so the graph reflects the current DDL
    lines.append(f"TRUNCATE TABLE {fq_graph};")
    lines.append("")

    # ── Collect all edge rows, then emit a single INSERT ──
    value_rows: list[str] = []

    for model_name, model_def in models.items():
        if isinstance(model_def, str):
            model_def = {"source": model_def}
        elif model_def is None:
            continue

        t_cat, t_sch = model_locations.get(model_name, (meta_catalog, meta_schema))
        columns = model_def.get("columns", {})
        is_join = "sources" in model_def
        is_agg = "group_by" in model_def

        # Determine sources
        if is_join:
            source_aliases = model_def.get("sources", {})
            join_cond = model_def.get("join", "")
        else:
            single_source = model_def.get("source", "")
            source_aliases = {"_": single_source} if single_source else {}
            join_cond = ""

        for alias, source_name in source_aliases.items():
            if not source_name:
                continue
            s_cat, s_sch = model_locations.get(source_name, (meta_catalog, meta_schema))

            # Extract join keys from condition (e.g. "c.customer_id = o.customer_id")
            join_key = ""
            source_key = ""
            if join_cond and alias != "_":
                # Parse simple equi-joins
                for part in join_cond.split(" AND "):
                    part = part.strip()
                    if "=" in part:
                        left, right = [s.strip() for s in part.split("=", 1)]
                        # Check if this alias is on either side
                        if left.startswith(f"{alias}."):
                            source_key = left.split(".", 1)[1]
                            # Find the other alias's key
                            for other_alias in source_aliases:
                                if other_alias != alias and right.startswith(f"{other_alias}."):
                                    join_key = right.split(".", 1)[1]
                        elif right.startswith(f"{alias}."):
                            source_key = right.split(".", 1)[1]
                            for other_alias in source_aliases:
                                if other_alias != alias and left.startswith(f"{other_alias}."):
                                    join_key = left.split(".", 1)[1]

            # Column-level edges
            has_column_edges = False
            for col_name, col_def in columns.items():
                if isinstance(col_def, str):
                    col_def = {"type": col_def}
                elif col_def is None:
                    col_def = {}

                # Determine if this column comes from this source
                col_from = col_def.get("from", "")
                if is_join and col_from and col_from != alias:
                    continue  # Column comes from a different alias
                if is_join and not col_from:
                    continue  # Ambiguous — skip

                expr = col_def.get("expr", "")
                udf_call = col_def.get("udf", "")
                has_cast = col_def.get("cast", False)

                # Determine transform type
                if udf_call:
                    transform = "UDF"
                    expr = udf_call
                elif expr:
                    # Check for aggregate functions
                    expr_upper = expr.upper()
                    if any(fn in expr_upper for fn in ("COUNT(", "SUM(", "MIN(", "MAX(", "AVG(")):
                        transform = "AGGREGATION"
                    else:
                        transform = "EXPRESSION"
                elif has_cast:
                    transform = "CAST"
                else:
                    transform = "PASSTHROUGH"

                source_col = col_name  # Same name unless expr references different cols

                _e = (expr or "").replace("'", "''")
                value_rows.append(
                    f"  ('{model_name}', '{source_name}', '{col_name}', '{source_col}', "
                    f"'{join_key}', '{source_key}', '{transform}', '{_e}', "
                    f"'{t_cat}', '{t_sch}', '{s_cat}', '{s_sch}')"
                )
                has_column_edges = True

            # If no column-level edges, add a model-level edge
            if not has_column_edges:
                transform = "JOIN" if is_join else "SOURCE"
                value_rows.append(
                    f"  ('{model_name}', '{source_name}', NULL, NULL, "
                    f"'{join_key}', '{source_key}', '{transform}', NULL, "
                    f"'{t_cat}', '{t_sch}', '{s_cat}', '{s_sch}')"
                )

    if value_rows:
        lines.append(
            f"INSERT INTO {fq_graph} "
            f"(target_model, source_model, target_column, source_column, "
            f"join_key, source_key, transform_type, expression, "
            f"target_catalog, target_schema, source_catalog, source_schema) VALUES"
        )
        lines.append(",\n".join(value_rows) + ";")

    lines.append("")
    return "\n".join(lines) + "\n"


def compile_lineage_log_insert(
    model_name: str,
    model_def: dict,
    catalog: str,
    schema: str,
    meta_catalog: str,
    meta_schema: str,
    git_commit: str = "unknown",
) -> str:
    """
    Compile an INSERT INTO lineage_log for a single model execution.

    Appended to each model's SQL file so run_id is captured per task.
    Uses {{job.run_id}} which Databricks resolves at runtime.
    """
    fq_log = f"`{meta_catalog}`.`{meta_schema}`.lineage_log"
    fq_model = f"`{catalog}`.`{schema}`.`{model_name}`"
    materialized = model_def.get("materialized", "view") if isinstance(model_def, dict) else "view"
    managed_by = model_def.get("managed_by", "") if isinstance(model_def, dict) else ""
    if managed_by:
        materialized = f"managed_by_{managed_by}"

    # Collect source names
    if isinstance(model_def, dict):
        if "sources" in model_def:
            sources = ",".join(model_def["sources"].values())
        elif "source" in model_def:
            sources = model_def["source"]
        else:
            sources = ""
    else:
        sources = ""

    return (
        f"\n-- Lineage log (run_id captured at runtime)\n"
        f"INSERT INTO {fq_log}\n"
        f"    (run_id, model, materialized, rows_created, catalog, schema, sources, git_commit, completed_at)\n"
        f"SELECT\n"
        f"    '{{{{job.run_id}}}}',\n"
        f"    '{model_name}',\n"
        f"    '{materialized}',\n"
        f"    COUNT(*),\n"
        f"    '{catalog}',\n"
        f"    '{schema}',\n"
        f"    '{sources}',\n"
        f"    '{git_commit}',\n"
        f"    current_timestamp()\n"
        f"FROM {fq_model};\n"
    )


def _compile_lineage_union_sql(fq_graph: str, max_depth: int = 10) -> str:
    """Generate unrolled UNION ALL for lineage traversal.

    Databricks serverless doesn't support recursive CTEs in SQL UDFs
    (function params are treated as outer references which are disallowed
    in recursive CTE anchors).  We unroll to fixed-depth self-joins.
    """
    branches: list[str] = []

    # Depth 0: the starting model itself (no table access)
    branches.append(
        "      SELECT 0 AS depth,\n"
        "             start_model AS model,\n"
        "             key_col AS key_column,\n"
        "             key_val AS key_value,\n"
        "             start_model AS path,\n"
        "             CAST(NULL AS STRING) AS transform_type,\n"
        "             CAST(NULL AS STRING) AS expression,\n"
        "             CAST(NULL AS STRING) AS source_catalog,\n"
        "             CAST(NULL AS STRING) AS source_schema"
    )

    for d in range(1, max_depth):
        # Key column: nested COALESCE tracking column renames through DAG
        kc = "key_col"
        for i in range(d):
            kc = f"COALESCE(g{i}.source_key, g{i}.join_key, {kc})"

        # Path: concat(start_model, ' -> ', g0.source_model, ' -> ', ...)
        path_parts = ["start_model"]
        for i in range(d):
            path_parts.extend([f"' -> '", f"g{i}.source_model"])
        path_expr = f"concat({', '.join(path_parts)})"

        last = f"g{d - 1}"

        # FROM / JOIN chain
        from_lines = [f"      FROM {fq_graph} g0"]
        for i in range(1, d):
            from_lines.append(
                f"      JOIN {fq_graph} g{i} ON g{i - 1}.source_model = g{i}.target_model"
            )

        branch = (
            f"      SELECT {d} AS depth,\n"
            f"             {last}.source_model AS model,\n"
            f"             {kc} AS key_column,\n"
            f"             key_val AS key_value,\n"
            f"             {path_expr} AS path,\n"
            f"             {last}.transform_type,\n"
            f"             {last}.expression,\n"
            f"             {last}.source_catalog,\n"
            f"             {last}.source_schema\n"
            + "\n".join(from_lines) + "\n"
            f"      WHERE g0.target_model = start_model"
        )
        branches.append(branch)

    return "\n      UNION ALL\n".join(branches)


def compile_trace_lineage_udf(meta_catalog: str, meta_schema: str, compute_type: str = "serverless") -> str:
    """
    Compile the trace_lineage, trace_lineage_json, and last_run_id SQL UDFs.

    Uses unrolled self-joins (max depth 10) instead of recursive CTEs
    because Databricks serverless doesn't allow function parameters as
    outer references inside recursive CTE anchors.
    """
    fq_schema = f"`{meta_catalog}`.`{meta_schema}`"
    fq_graph = f"{fq_schema}.lineage_graph"
    fq_log = f"{fq_schema}.lineage_log"

    max_depth = 10
    union_sql = _compile_lineage_union_sql(fq_graph, max_depth)

    return (
        f"-- UDF: trace_lineage\n"
        f"-- Trace any value back through the full DAG (max depth {max_depth})\n"
        f"DROP FUNCTION IF EXISTS {fq_schema}.trace_lineage;\n"
        f"CREATE FUNCTION {fq_schema}.trace_lineage(\n"
        f"    start_model STRING, key_col STRING, key_val STRING\n"
        f")\n"
        f"RETURNS STRING\n"
        f"RETURN (\n"
        f"  SELECT concat_ws('\\n', collect_list(line))\n"
        f"  FROM (\n"
        f"    SELECT\n"
        f"      concat(\n"
        f"        '[', CAST(depth AS STRING), '] ',\n"
        f"        model, '.', key_column, ' = ', key_value,\n"
        f"        CASE WHEN transform_type IS NOT NULL\n"
        f"             THEN concat(' (', transform_type,\n"
        f"                  CASE WHEN expression IS NOT NULL AND expression != ''\n"
        f"                       THEN concat(': ', expression) ELSE '' END, ')')\n"
        f"             ELSE '' END\n"
        f"      ) AS line\n"
        f"    FROM (\n"
        f"{union_sql}\n"
        f"    )\n"
        f"    ORDER BY depth\n"
        f"  )\n"
        f");\n"
        f"\n"
        f"-- UDF: trace_lineage_json\n"
        f"-- Same as trace_lineage but returns structured JSON (max depth {max_depth})\n"
        f"DROP FUNCTION IF EXISTS {fq_schema}.trace_lineage_json;\n"
        f"CREATE FUNCTION {fq_schema}.trace_lineage_json(\n"
        f"    start_model STRING, key_col STRING, key_val STRING\n"
        f")\n"
        f"RETURNS STRING\n"
        f"RETURN (\n"
        f"  SELECT to_json(collect_list(node))\n"
        f"  FROM (\n"
        f"    SELECT named_struct(\n"
        f"      'depth', depth,\n"
        f"      'model', model,\n"
        f"      'key_column', key_column,\n"
        f"      'key_value', key_value,\n"
        f"      'transform_type', transform_type,\n"
        f"      'expression', expression,\n"
        f"      'catalog', source_catalog,\n"
        f"      'schema', source_schema\n"
        f"    ) AS node\n"
        f"    FROM (\n"
        f"{union_sql}\n"
        f"    )\n"
        f"    ORDER BY depth\n"
        f"  )\n"
        f");\n"
        f"\n"
        f"-- UDF: last_run_id\n"
        f"-- Returns the most recent run_id for a model\n"
        f"DROP FUNCTION IF EXISTS {fq_schema}.last_run_id;\n"
        f"CREATE FUNCTION {fq_schema}.last_run_id(model_name STRING)\n"
        f"RETURNS STRING\n"
        f"RETURN (\n"
        f"  SELECT run_id FROM {fq_log}\n"
        f"  WHERE model = model_name\n"
        f"  ORDER BY completed_at DESC\n"
        f"  LIMIT 1\n"
        f");\n"
    )


def compile_all_pure_sql(
    ddl_path: Path,
    output_dir: Path,
    catalog: str = "main",
    schema: str = "silver",
    git_commit: str = "unknown",
    compute_type: str = "serverless",
    platform: str = "databricks",
    profile: dict | None = None,
    forge_config: dict | None = None,
) -> dict[str, Path]:
    """
    Compile models.yml into numbered, standalone SQL files.

    No Jinja. No dbt. Runs directly on a SQL warehouse.
    Files are numbered for execution order (topological sort).

    If a profile with schemas: mapping is provided, each model
    gets its correct schema based on naming convention (stg_ → silver,
    raw_ → bronze, customer_summary → gold, etc.).
    """
    from datetime import datetime, timezone
    from forge.compute_resolver import resolve_model_schema

    deploy_ts = datetime.now(timezone.utc).isoformat()
    prof = profile or {"catalog": catalog, "schema": schema}

    models = load_ddl(ddl_path)
    udfs = load_udfs(ddl_path)

    # Clean stale files — source of truth is always dbt/ddl/*
    if output_dir.exists():
        for old_sql in output_dir.glob("*.sql"):
            old_sql.unlink()

    output_dir.mkdir(parents=True, exist_ok=True)
    results: dict[str, Path] = {}

    # Build a lookup of resolved (catalog, schema) per model
    model_locations: dict[str, tuple[str, str]] = {}
    for model_name, model_def in models.items():
        model_locations[model_name] = resolve_model_schema(
            model_name, model_def, prof, forge_config=forge_config,
        )

    # ── Validate shared assets aren't in domain-suffixed schemas ──
    domains = (forge_config or {}).get("domains", {})
    if domains:
        domain_suffixes = {
            d_cfg.get("schema_suffix", f"_{d_name}")
            for d_name, d_cfg in domains.items()
        }
        errors: list[str] = []
        for model_name, model_def in models.items():
            if isinstance(model_def, str) or model_def is None:
                continue
            if model_def.get("domain") is False:
                _, m_schema = model_locations[model_name]
                for suffix in domain_suffixes:
                    if m_schema.endswith(suffix):
                        errors.append(
                            f"  {model_name}: shared asset (domain: false) is in "
                            f"domain-suffixed schema '{m_schema}' — a domain teardown "
                            f"with DROP SCHEMA CASCADE would destroy it"
                        )
        if errors:
            raise RuntimeError(
                "Compile error: shared assets placed in domain-specific schemas.\n"
                + "\n".join(errors)
                + "\n\nMove shared assets to the base schema or set domain: true."
            )

    seq = 0

    # Resolve meta catalog/schema for lineage tables
    meta_cat, meta_sch = resolve_model_schema(
        "_lineage", {"layer": "meta"}, prof, forge_config=forge_config,
    )

    # ── Collect ALL distinct catalog.schema pairs used anywhere ──
    all_schemas: set[tuple[str, str]] = set()
    all_schemas.add((meta_cat, meta_sch))
    for cat, sch in model_locations.values():
        all_schemas.add((cat, sch))

    # Step 0: UDFs (must exist before any model references them)
    udf_qualifiers: dict[str, str] = {}
    if udfs:
        # Pre-resolve UDF schemas so they're included in all_schemas
        udf_schemas: list[tuple[str, str, str]] = []  # (udf_name, catalog, schema)
        for udf_name, udf_def in udfs.items():
            udf_catalog = udf_def.get("catalog")
            udf_schema = udf_def.get("schema")
            if not udf_catalog or not udf_schema:
                udf_layer = udf_def.get("layer", "silver")
                resolved_cat, resolved_sch = resolve_model_schema(
                    udf_name, {"layer": udf_layer}, prof, forge_config=forge_config,
                )
                udf_catalog = udf_catalog or resolved_cat
                udf_schema = udf_schema or resolved_sch
            all_schemas.add((udf_catalog, udf_schema))
            udf_schemas.append((udf_name, udf_catalog, udf_schema))

        # Build UDF qualifier map: bare name → fully-qualified name
        udf_qualifiers: dict[str, str] = {
            name: f"`{cat}`.`{sch}`.{name}"
            for name, cat, sch in udf_schemas
        }

        udf_lines = []

        # Ensure all schemas exist before any DDL runs
        for cat, sch in sorted(all_schemas):
            udf_lines.append(f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{sch}`;")
        udf_lines.append("")

        # ── Collect and emit table stubs for UDF depends_on ──
        # User UDFs can declare depends_on: [model_name, ...] to reference
        # tables in their body. Since the UDF file runs before model files,
        # we emit CREATE TABLE IF NOT EXISTS stubs here.
        emitted_stubs: set[str] = set()
        dep_errors: list[str] = []

        for udf_name, udf_def in udfs.items():
            for dep in udf_def.get("depends_on", []):
                if dep in emitted_stubs:
                    continue
                if dep not in models:
                    dep_errors.append(
                        f"  UDF '{udf_name}': depends_on '{dep}' but no model "
                        f"with that name exists in models.yml"
                    )
                    continue
                dep_def = models[dep]
                if isinstance(dep_def, str) or dep_def is None:
                    dep_errors.append(
                        f"  UDF '{udf_name}': depends_on '{dep}' but that model "
                        f"has no column definitions (needed for table stub)"
                    )
                    continue
                dep_cols = dep_def.get("columns", {})
                if not dep_cols:
                    dep_errors.append(
                        f"  UDF '{udf_name}': depends_on '{dep}' but that model "
                        f"has no columns defined (needed for table stub)"
                    )
                    continue
                dep_cat, dep_sch = model_locations[dep]
                udf_lines.append(f"-- Table stub for UDF dependency: {dep}")
                udf_lines.append(_compile_table_stub(dep, dep_cols, dep_cat, dep_sch))
                udf_lines.append("")
                emitted_stubs.add(dep)

        if dep_errors:
            raise RuntimeError(
                "Compile error: UDF depends_on references could not be resolved.\n"
                + "\n".join(dep_errors)
            )

        # ── Lineage table stubs (internal depends_on for trace_lineage UDFs) ──
        udf_lines.append("-- Table stubs for lineage UDFs (forge-internal)")
        udf_lines.append(_compile_table_stub("lineage_graph", _LINEAGE_GRAPH_COLUMNS, meta_cat, meta_sch))
        udf_lines.append("")
        udf_lines.append(_compile_table_stub("lineage_log", _LINEAGE_LOG_COLUMNS, meta_cat, meta_sch))
        udf_lines.append("")

        for udf_name, udf_catalog, udf_schema in udf_schemas:
            fq_schema = f"`{udf_catalog}`.`{udf_schema}`"
            udf_lines.append(compile_udf_sql(udf_name, udfs[udf_name], schema=fq_schema, compute_type=compute_type))
            udf_lines.append("")

        # Lineage UDFs (trace_lineage, trace_lineage_json, last_run_id)
        udf_lines.append(compile_trace_lineage_udf(meta_cat, meta_sch, compute_type=compute_type))

        udf_path = output_dir / f"{seq:03d}_udfs.sql"
        udf_path.write_text("\n".join(udf_lines))
        results["_udfs"] = udf_path
        seq += 1
    else:
        # Even without user UDFs, ensure schemas + generate lineage UDFs
        schema_lines = []
        for cat, sch in sorted(all_schemas):
            schema_lines.append(f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{sch}`;")
        schema_lines.append("")
        # Lineage table stubs for trace_lineage UDFs
        schema_lines.append("-- Table stubs for lineage UDFs (forge-internal)")
        schema_lines.append(_compile_table_stub("lineage_graph", _LINEAGE_GRAPH_COLUMNS, meta_cat, meta_sch))
        schema_lines.append("")
        schema_lines.append(_compile_table_stub("lineage_log", _LINEAGE_LOG_COLUMNS, meta_cat, meta_sch))
        schema_lines.append("")
        lineage_udf_sql = "\n".join(schema_lines) + compile_trace_lineage_udf(meta_cat, meta_sch, compute_type=compute_type)
        udf_path = output_dir / f"{seq:03d}_udfs.sql"
        udf_path.write_text(lineage_udf_sql)
        results["_udfs"] = udf_path
        seq += 1

    # Step 1: Lineage graph DDL + seed data
    lineage_sql = compile_lineage_graph_sql(
        models, model_locations, meta_cat, meta_sch,
        git_commit=git_commit, deploy_ts=deploy_ts,
    )
    lineage_path = output_dir / f"{seq:03d}_lineage_graph.sql"
    lineage_path.write_text(lineage_sql)
    results["_lineage_graph"] = lineage_path
    seq += 1

    # Step 1a: Volumes (CREATE VOLUME IF NOT EXISTS)
    volumes = load_volumes(ddl_path, forge_config=forge_config)
    if volumes:
        vol_lines: list[str] = [
            "-- Volumes",
            "-- Generated by: forge compile --pure-sql",
            "",
        ]
        for vol_name, vol_def in volumes.items():
            vol_layer = vol_def.get("layer", "bronze")
            v_cat, v_sch = resolve_model_schema(
                vol_name, {"layer": vol_layer}, prof, forge_config=forge_config,
            )
            vol_lines.append(compile_volume_sql(vol_name, vol_def, v_cat, v_sch))
        vol_path = output_dir / f"{seq:03d}_volumes.sql"
        vol_path.write_text("\n".join(vol_lines))
        results["_volumes"] = vol_path
        seq += 1

    # Step 1b: Seeds with catalog/schema overrides (not handled by dbt seed)
    seeds = load_seeds(ddl_path, forge_config=forge_config)
    for seed_name, seed_def in seeds.items():
        if not isinstance(seed_def, dict):
            continue
        seed_catalog = seed_def.get("catalog")
        if not seed_catalog:
            continue  # default-catalog seeds are handled by dbt seed
        # Resolve catalog + schema through profile mapping
        s_cat, s_sch = resolve_model_schema(
            seed_name, {"layer": seed_catalog}, prof, forge_config=forge_config,
        )
        # Locate the CSV file
        origin = seed_def.get("origin", {})
        csv_rel = origin.get("path", f"dbt/seeds/{seed_name}.csv")
        csv_path = Path(csv_rel)
        if not csv_path.is_absolute():
            csv_path = ddl_path.parent.parent / csv_rel  # dbt/ddl/../../dbt/seeds/
            if not csv_path.exists():
                csv_path = Path(csv_rel)  # try relative to cwd

        seed_sql = compile_pure_sql_seed(seed_name, seed_def, s_cat, s_sch, csv_path)
        seed_path = output_dir / f"{seq:03d}_{seed_name}.sql"
        seed_path.write_text(seed_sql)
        results[seed_name] = seed_path
        seq += 1

    # Step 2..N: Models in dependency order
    ordered = topo_sort_models(models)
    for model_name in ordered:
        model_def = models[model_name]
        m_catalog, m_schema = model_locations[model_name]

        # Model SQL — resolve source refs to their correct schema too
        sql = compile_pure_sql_model(
            model_name, model_def, m_catalog, m_schema,
            git_commit=git_commit, deploy_ts=deploy_ts,
            compute_type=compute_type, platform=platform,
            source_locations=model_locations,
            udf_qualifiers=udf_qualifiers or None,
        )

        # Append lineage_log INSERT (captures run_id at runtime)
        sql += compile_lineage_log_insert(
            model_name, model_def, m_catalog, m_schema,
            meta_cat, meta_sch, git_commit=git_commit,
        )

        out_path = output_dir / f"{seq:03d}_{model_name}.sql"
        out_path.write_text(sql)
        results[model_name] = out_path

        # Quarantine (runs immediately after the model)
        quarantine = model_def.get("quarantine")
        if quarantine:
            q_sql = compile_pure_sql_quarantine(
                model_name, quarantine, m_catalog, m_schema,
                git_commit=git_commit, deploy_ts=deploy_ts,
            )
            q_path = output_dir / f"{seq:03d}_{model_name}_quarantine.sql"
            q_path.write_text(q_sql)
            results[f"{model_name}_quarantine"] = q_path

        seq += 1

    # ── Post-compile validation: verify schema ordering ──
    _validate_sql_schema_ordering(output_dir)

    return results


def compile_teardown_sql(
    ddl_path: Path,
    output_dir: Path,
    profile: dict | None = None,
    forge_config: dict | None = None,
) -> Path:
    """Compile a teardown SQL file that drops all tables, views, and UDFs.

    Drops are emitted in reverse dependency order so child tables are
    dropped before parents.  UDFs are dropped first (they may reference
    tables).  Lineage tables are dropped last.
    """
    from forge.compute_resolver import resolve_model_schema

    prof = profile or {}
    models = load_ddl(ddl_path, forge_config=forge_config)
    udfs = load_udfs(ddl_path, forge_config=forge_config)

    model_locations: dict[str, tuple[str, str]] = {}
    for model_name, model_def in models.items():
        model_locations[model_name] = resolve_model_schema(
            model_name, model_def, prof, forge_config=forge_config,
        )

    meta_cat, meta_sch = resolve_model_schema(
        "_lineage", {"layer": "meta"}, prof, forge_config=forge_config,
    )

    lines: list[str] = [
        "-- ============================================",
        "-- TEARDOWN: Drop all forge-managed objects",
        "-- Generated by: forge compile --pure-sql",
        "-- ============================================",
        "",
    ]

    # Drop UDFs first (they may reference tables)
    for udf_name, udf_def in udfs.items():
        udf_catalog = udf_def.get("catalog")
        udf_schema = udf_def.get("schema")
        if not udf_catalog or not udf_schema:
            udf_layer = udf_def.get("layer", "silver")
            udf_catalog, udf_schema = resolve_model_schema(
                udf_name, {"layer": udf_layer}, prof, forge_config=forge_config,
            )
        fq = f"`{udf_catalog}`.`{udf_schema}`.{udf_name}"
        lines.append(f"DROP FUNCTION IF EXISTS {fq};")

    # Drop lineage UDFs
    fq_meta = f"`{meta_cat}`.`{meta_sch}`"
    for fn in ("trace_lineage", "trace_lineage_json", "last_run_id"):
        lines.append(f"DROP FUNCTION IF EXISTS {fq_meta}.{fn};")
    lines.append("")

    # Drop models in reverse topo order (children before parents)
    ordered = list(reversed(topo_sort_models(models)))
    for model_name in ordered:
        model_def = models[model_name]
        m_cat, m_sch = model_locations[model_name]
        fq = f"`{m_cat}`.`{m_sch}`.`{model_name}`"
        materialized = model_def.get("materialized", "view") if isinstance(model_def, dict) else "view"
        # Quarantine sibling
        quarantine = model_def.get("quarantine") if isinstance(model_def, dict) else None
        if quarantine:
            lines.append(f"DROP TABLE IF EXISTS `{m_cat}`.`{m_sch}`.`{model_name}_quarantine`;")
        if materialized in ("table", "incremental") or model_def.get("managed_by"):
            lines.append(f"DROP TABLE IF EXISTS {fq};")
        else:
            lines.append(f"DROP VIEW IF EXISTS {fq};")
    lines.append("")

    # Drop lineage tables
    lines.append(f"DROP TABLE IF EXISTS {fq_meta}.lineage_graph;")
    lines.append(f"DROP TABLE IF EXISTS {fq_meta}.lineage_log;")
    lines.append("")

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "teardown.sql"
    out_path.write_text("\n".join(lines) + "\n")
    return out_path


def compile_backup_sql(
    ddl_path: Path,
    output_dir: Path,
    profile: dict | None = None,
    forge_config: dict | None = None,
) -> Path:
    """Compile a backup SQL file that archives all table data before teardown.

    Inserts each table's data (serialised as JSON via ``collect_list``)
    into the ``_backup_archive`` table in the operations / backups schema.
    This matches the schema used by ``forge backup --data``.

    Runs as a SQL task before teardown in the TEARDOWN workflow.
    """
    import subprocess
    from forge.compute_resolver import resolve_model_schema

    prof = profile or {}
    fc = forge_config or {}
    models = load_ddl(ddl_path, forge_config=forge_config)

    model_locations: dict[str, tuple[str, str]] = {}
    for model_name, model_def in models.items():
        model_locations[model_name] = resolve_model_schema(
            model_name, model_def, prof, forge_config=forge_config,
        )

    meta_cat, meta_sch = resolve_model_schema(
        "_lineage", {"layer": "meta"}, prof, forge_config=forge_config,
    )

    # Resolve backup target: operations catalog + backups schema
    backup_cat = _resolve_catalog(fc, "operations")
    user = re.sub(r"[^a-z0-9]+", "_", getpass.getuser().lower()).strip("_")
    schema_pattern = fc.get("schema_pattern", "{user}_{id}")
    project_id = fc.get("id", fc.get("name", "project")).replace("-", "_")
    project_name = fc.get("name", "unnamed")
    scope = fc.get("scope", "").replace("-", "_")
    env = prof.get("env", "dev")
    backup_sch = schema_pattern.replace("{user}", user).replace("{id}", "backups")
    backup_sch = backup_sch.replace("{scope}", scope).replace("{env}", env)
    while "__" in backup_sch:
        backup_sch = backup_sch.replace("__", "_")
    backup_sch = backup_sch.strip("_")

    archive_table = f"`{backup_cat}`.`{backup_sch}`.`_backup_archive`"

    # Git context (baked in at compile time)
    git_sha = "unknown"
    git_branch = "unknown"
    try:
        git_sha = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
        ).decode().strip()
        git_branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        pass

    dab_name = f"PROCESS_{project_id}"

    lines: list[str] = [
        "-- ============================================",
        "-- BACKUP: Archive all table data before teardown",
        "-- Generated by: forge compile --pure-sql",
        "-- ============================================",
        "",
        f"CREATE SCHEMA IF NOT EXISTS `{backup_cat}`.`{backup_sch}`;",
        "",
        f"CREATE TABLE IF NOT EXISTS {archive_table} (",
        "  asset_name STRING,",
        "  asset_type STRING,",
        "  project STRING,",
        "  environment STRING,",
        "  archived_at TIMESTAMP,",
        "  performed_by STRING,",
        "  git_sha STRING,",
        "  git_branch STRING,",
        "  dab_name STRING,",
        "  row_count BIGINT,",
        "  snapshot_data STRING",
        ") USING DELTA;",
        "",
    ]

    # Metadata columns shared by every INSERT
    meta_cols = (
        f"'{project_name}' AS project, "
        f"'{env}' AS environment, "
        f"current_timestamp() AS archived_at, "
        f"current_user() AS performed_by, "
        f"'{git_sha}' AS git_sha, "
        f"'{git_branch}' AS git_branch, "
        f"'{dab_name}' AS dab_name"
    )

    # Archive models that are tables (skip views — no data to backup)
    ordered = topo_sort_models(models)
    for model_name in ordered:
        model_def = models[model_name]
        if isinstance(model_def, str) or model_def is None:
            continue
        materialized = model_def.get("materialized", "view")
        is_table = materialized in ("table", "incremental") or model_def.get("managed_by")
        if not is_table:
            continue

        m_cat, m_sch = model_locations[model_name]
        src_fq = f"`{m_cat}`.`{m_sch}`.`{model_name}`"
        lines.append(
            f"INSERT INTO {archive_table} "
            f"SELECT '{model_name}' AS asset_name, "
            f"'table' AS asset_type, "
            f"{meta_cols}, "
            f"COUNT(*) AS row_count, "
            f"to_json(collect_list(to_json(struct(*)))) AS snapshot_data "
            f"FROM {src_fq};"
        )

        # Quarantine sibling
        quarantine = model_def.get("quarantine")
        if quarantine:
            q_fq = f"`{m_cat}`.`{m_sch}`.`{model_name}_quarantine`"
            lines.append(
                f"INSERT INTO {archive_table} "
                f"SELECT '{model_name}_quarantine' AS asset_name, "
                f"'table' AS asset_type, "
                f"{meta_cols}, "
                f"COUNT(*) AS row_count, "
                f"to_json(collect_list(to_json(struct(*)))) AS snapshot_data "
                f"FROM {q_fq};"
            )

    lines.append("")

    # Archive lineage tables
    lg_fq = f"`{meta_cat}`.`{meta_sch}`.`lineage_graph`"
    ll_fq = f"`{meta_cat}`.`{meta_sch}`.`lineage_log`"
    lines.append(
        f"INSERT INTO {archive_table} "
        f"SELECT 'lineage_graph' AS asset_name, 'table' AS asset_type, "
        f"{meta_cols}, COUNT(*) AS row_count, "
        f"to_json(collect_list(to_json(struct(*)))) AS snapshot_data "
        f"FROM {lg_fq};"
    )
    lines.append(
        f"INSERT INTO {archive_table} "
        f"SELECT 'lineage_log' AS asset_name, 'table' AS asset_type, "
        f"{meta_cols}, COUNT(*) AS row_count, "
        f"to_json(collect_list(to_json(struct(*)))) AS snapshot_data "
        f"FROM {ll_fq};"
    )
    lines.append("")

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "backup.sql"
    out_path.write_text("\n".join(lines) + "\n")
    return out_path


def _validate_sql_schema_ordering(output_dir: Path) -> None:
    """
    Validate that every catalog.schema referenced in a SQL file has a
    CREATE SCHEMA IF NOT EXISTS in the same file.

    Each SQL file runs as an independent Databricks sql_task, so cross-file
    ordering is not sufficient — every file must be self-sufficient for the
    schemas it creates objects in.

    References in INSERT/SELECT (reading existing tables) are exempt when the
    target schema is guaranteed by a prior SETUP task (dependency chain).
    Only DDL statements (CREATE TABLE/VIEW/FUNCTION, TRUNCATE) require
    the schema to be created in the same file.
    """
    import re

    ref_pattern = re.compile(r"`([^`]+)`\.`([^`]+)`\.")
    create_pattern = re.compile(
        r"CREATE\s+SCHEMA\s+IF\s+NOT\s+EXISTS\s+`([^`]+)`\.`([^`]+)`",
        re.IGNORECASE,
    )
    # DDL context: lines that create/modify objects (require schema to exist)
    ddl_line_pattern = re.compile(
        r"^\s*(CREATE\s+(TABLE|VIEW|FUNCTION)|TRUNCATE\s+TABLE|DROP\s+(TABLE|VIEW|FUNCTION))",
        re.IGNORECASE,
    )

    errors: list[str] = []

    for sql_file in sorted(output_dir.glob("*.sql")):
        content = sql_file.read_text()

        # Schemas created in THIS file
        file_schemas: set[tuple[str, str]] = set()
        for m in create_pattern.finditer(content):
            file_schemas.add((m.group(1), m.group(2)))

        # Check DDL lines for schema references not created in this file
        for line in content.split("\n"):
            if not ddl_line_pattern.match(line):
                continue
            for m in ref_pattern.finditer(line):
                ref = (m.group(1), m.group(2))
                if ref not in file_schemas:
                    errors.append(
                        f"  {sql_file.name}: DDL references `{ref[0]}`.`{ref[1]}` "
                        f"but this file has no CREATE SCHEMA IF NOT EXISTS for it"
                    )

    if errors:
        # Deduplicate
        errors = sorted(set(errors))
        raise RuntimeError(
            "Compile error: SQL files contain DDL for schemas they don't create.\n"
            "Each SQL file runs as an independent task and must be self-sufficient.\n"
            + "\n".join(errors)
            + "\n\nAdd CREATE SCHEMA IF NOT EXISTS to each file that needs it."
        )


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
    lines.append("| `forge explain model.col --full` | Include UDF runtime details |")
    lines.append("| `forge explain model.col --light` | Compact: expressions + columns only |")
    lines.append("| `forge explain model.col --origin` | Show data origin / source provenance |")
    lines.append("| `forge explain model.col --version v2` | Filter to specific version |")
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

    # ── Lineage Modes & Origin Tracking ──────────
    lineage_cfg = forge_config.get("lineage", {})
    lineage_mode = lineage_cfg.get("mode", "full")
    origin_tracking = lineage_cfg.get("origin_tracking", True)

    lines.append("## Lineage & Origin Tracking")
    lines.append("")
    lines.append(f"**Mode:** `{lineage_mode}`  ")
    lines.append(f"**Origin tracking:** `{'enabled' if origin_tracking else 'disabled'}`")
    lines.append("")
    lines.append("| Mode | What it captures | When to use |")
    lines.append("|------|-----------------|-------------|")
    lines.append("| `full` | Runtime input values + expressions + origin | Production audits, compliance |")
    lines.append("| `lightweight` | Column names + expressions only (no runtime values) | Dev/test, cost savings |")
    lines.append("")
    lines.append("### Origin types")
    lines.append("")
    lines.append("Add `origin:` to any seed or model to track where data comes from:")
    lines.append("")
    lines.append("```yaml")
    lines.append("seeds:")
    lines.append("  raw_customers:")
    lines.append("    origin:")
    lines.append("      type: seed                        # seed, volume_file, api, webscraper, external_table, python_process")
    lines.append("      path: dbt/seeds/raw_customers.csv")
    lines.append("      format: csv")
    lines.append("```")
    lines.append("")
    lines.append("```yaml")
    lines.append("models:")
    lines.append("  stg_events:")
    lines.append("    origin:")
    lines.append("      type: volume_file")
    lines.append("      path: /Volumes/bronze/landing/events/")
    lines.append("      format: parquet")
    lines.append("```")
    lines.append("")
    lines.append("```yaml")
    lines.append("models:")
    lines.append("  stg_api_data:")
    lines.append("    origin:")
    lines.append("      type: api")
    lines.append("      endpoint: https://api.example.com/v2/records")
    lines.append("      format: json")
    lines.append("```")
    lines.append("")
    lines.append("Use `forge explain model.col --origin` to see the full origin chain.")
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
    lines.append("The explain tree shows: expression, UDF calls (SQL/Python/Pandas),")
    lines.append("checks, git commit, version, and full upstream lineage.")
    lines.append("")

    # ── How to Define UDFs ───────────────────────
    lines.append("## How to Define UDFs (Reusable Functions)")
    lines.append("")
    lines.append("Add a `udfs:` block in `dbt/ddl/00_udfs.yml`:")
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
    lines.append("### Python UDF")
    lines.append("")
    lines.append("```yaml")
    lines.append("udfs:")
    lines.append("  clean_email:")
    lines.append("    language: python")
    lines.append("    returns: string")
    lines.append("    runtime_version: '3.11'")
    lines.append("    handler: clean")
    lines.append("    params:")
    lines.append("      - { name: raw_email, type: string }")
    lines.append("    body: |")
    lines.append("      def clean(raw_email):")
    lines.append("          if raw_email is None:")
    lines.append("              return None")
    lines.append("          return raw_email.strip().lower()")
    lines.append("```")
    lines.append("")
    lines.append("### Pandas UDF (vectorized — fast on large batches)")
    lines.append("")
    lines.append("```yaml")
    lines.append("udfs:")
    lines.append("  average_score:")
    lines.append("    language: pandas")
    lines.append("    returns: double")
    lines.append("    runtime_version: '3.11'")
    lines.append("    handler: compute")
    lines.append("    packages: [pandas, numpy]")
    lines.append("    params:")
    lines.append("      - { name: score_a, type: double }")
    lines.append("      - { name: score_b, type: double }")
    lines.append("    body: |")
    lines.append("      import pandas as pd")
    lines.append("      def compute(score_a: pd.Series, score_b: pd.Series) -> pd.Series:")
    lines.append("          return (score_a + score_b) / 2.0")
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
    lines.append("Lineage automatically tracks UDF type (SQL_UDF / PYTHON_UDF / PANDAS_UDF)")
    lines.append("and captures the input column values at runtime in `_lineage.columns[].inputs`.")
    lines.append("")
    lines.append("### Manage UDFs")
    lines.append("")
    lines.append("| Command | What it does |")
    lines.append("|---------|-------------|")
    lines.append("| `forge udfs` | Shows all defined UDFs (🔵 SQL, 🟣 Python, 🟠 Pandas) |")
    lines.append("| `forge udfs --output udfs.sql` | Writes CREATE FUNCTION SQL |")
    lines.append("| `forge compile` | Auto-generates `dbt/functions/*.sql` per UDF |")
    lines.append("| `forge explain model.col` | Shows full provenance including UDF type + inputs |")
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
                raw_lang = udef.get("language", "sql").upper()
                icon = "🟠" if raw_lang == "PANDAS" else ("🟣" if raw_lang == "PYTHON" else "🔵")
                ret = udef.get("returns", "STRING")
                params = udef.get("params", [])
                param_str = ", ".join(
                    f"{p['name']}: {p['type']}" if isinstance(p, dict) else str(p)
                    for p in params
                ) or "(none)"
                lines.append(f"| {icon} `{uname}` | {raw_lang} | {ret} | {param_str} |")
            lines.append("")

    # ── Broadcast Hints ──────────────────────────
    lines.append("## Broadcast Hints (Join Optimisation)")
    lines.append("")
    lines.append("On Databricks, broadcasting a small table makes joins fly.")
    lines.append("Add one line to any join model:")
    lines.append("")
    lines.append("```yaml")
    lines.append("models:")
    lines.append("  customer_orders:")
    lines.append("    sources:")
    lines.append("      c: customer_clean    # small table")
    lines.append("      o: stg_orders        # big table")
    lines.append('    join: "c.customer_id = o.customer_id"')
    lines.append("    broadcast: c            # ← one line")
    lines.append("```")
    lines.append("")
    lines.append("Generates: `SELECT /*+ BROADCAST(c) */ ...`")
    lines.append("")
    lines.append("Multiple tables: `broadcast: [c, small_lookup]`")
    lines.append("")

    # ── Core dbt Concepts ────────────────────────
    lines.append("## Core dbt Concepts (that the wrapper makes invisible)")
    lines.append("")
    lines.append("You never need to know these to be successful — but here's what's happening behind the scenes:")
    lines.append("")
    lines.append("- **Jinja** = dbt's templating language (the wrapper writes it for you)")
    lines.append("- **Macros** = reusable SQL snippets (all live in external `../dbt-dab-tools`)")
    lines.append("- **Tests** = data quality checks (wrapper's `checks:` + `quarantine:` replace raw dbt tests)")
    lines.append("- **Incremental models & snapshots** = performance tricks (wrapper uses `materialized:` + `prior_version`)")
    lines.append("")
    lines.append("The wrapper hides the complex parts so you can focus on business logic in YAML.")
    lines.append("")

    # ── Domains (Multi-Region / Multi-Tenant) ────
    domains = forge_config.get("domains", {})
    domain_layers = forge_config.get("domain_layers", [])
    if domains:
        domain_names = list(domains.keys())
        lines.append("## Domains (Multi-Region / Multi-Tenant)")
        lines.append("")
        lines.append("This project deploys the **same models** into isolated, per-domain schemas.")
        lines.append("Each domain gets its own tables, quarantine sidecars, and lineage — zero code duplication.")
        lines.append("")
        lines.append("### How it works")
        lines.append("")
        lines.append("One model definition → N domain instances at compile time:")
        lines.append("")
        lines.append("```")
        lines.append("customer_clean.yml  ──forge compile──▶  customer_clean_eu.sql")
        lines.append("                                        customer_clean_us.sql")
        lines.append("                                        customer_clean_apac.sql")
        lines.append("```")
        lines.append("")
        lines.append("Each instance writes to its own schema (e.g. `ben_demo_eu`, `ben_demo_us`).")
        lines.append("Refs between domain instances are rewritten automatically —")
        lines.append("`customer_orders_eu` refs `customer_clean_eu`, not the base model.")
        lines.append("")

        # Current domains
        lines.append("### Your domains")
        lines.append("")
        lines.append("| Domain | Schema Suffix | Example Schema |")
        lines.append("|--------|--------------|----------------|")
        schema_pattern = forge_config.get("schema_pattern", "{id}")
        base_example = schema_pattern.replace("{user}", "ben").replace("{id}", forge_config.get("id", "demo"))
        for dname, dcfg in domains.items():
            suffix = dcfg.get("schema_suffix", f"_{dname}")
            lines.append(f"| `{dname}` | `{suffix}` | `{base_example}{suffix}` |")
        lines.append("")

        # Domain layers
        if domain_layers:
            non_domain = [l for l in forge_config.get("schemas", []) if l not in domain_layers]
            lines.append("### Which layers are split by domain?")
            lines.append("")
            lines.append(f"**Bifurcated (per-domain instances):** {', '.join(f'`{l}`' for l in domain_layers)}")
            lines.append("")
            if non_domain:
                lines.append(f"**Shared (single instance, all domains read from it):** {', '.join(f'`{l}`' for l in non_domain)}")
                lines.append("")
            lines.append("Models in shared layers are referenced by all domain instances.")
            lines.append("For example, if bronze is shared, `customer_clean_eu` and `customer_clean_us`")
            lines.append("both read from the same `stg_customers` table.")
            lines.append("")

        # Per-model opt-out
        lines.append("### Opting a model out of domains")
        lines.append("")
        lines.append("If a model is in a bifurcated layer but should stay shared (e.g. a reference table),")
        lines.append("add `domain: false` in its DDL YAML:")
        lines.append("")
        lines.append("```yaml")
        lines.append("models:")
        lines.append("  country_codes:")
        lines.append("    domain: false            # stays shared — no EU/US/APAC copies")
        lines.append("    source: raw_country_codes")
        lines.append("    columns:")
        lines.append("      code: { type: string, required: true }")
        lines.append("```")
        lines.append("")
        lines.append("Downstream domain models that reference an opted-out model will correctly")
        lines.append("ref the shared base table, not a non-existent domain instance.")
        lines.append("")

        # Domain source routing
        lines.append("### Domain-specific ingestion (different source per domain)")
        lines.append("")
        lines.append("Each domain can read from its own source file (e.g. region-specific volumes).")
        lines.append("Add `domain_sources:` to the model DDL:")
        lines.append("")
        lines.append("```yaml")
        lines.append("models:")
        lines.append("  stg_customers:")
        lines.append("    source: raw_customers                  # default / fallback")
        lines.append("    domain_sources:")
        lines.append("      eu: raw_customers_eu                 # EU reads from EU volume")
        lines.append("      us: raw_customers_us                 # US reads from US volume")
        lines.append("      apac: raw_customers_apac             # APAC reads from APAC volume")
        lines.append("    columns:")
        lines.append("      customer_id: { type: int, required: true }")
        lines.append("```")
        lines.append("")
        lines.append("Result: `stg_customers_eu.sql` reads from `raw_customers_eu`,")
        lines.append("`stg_customers_us.sql` reads from `raw_customers_us`, etc.")
        lines.append("")
        lines.append("For join models, use a dict to override specific join sources:")
        lines.append("")
        lines.append("```yaml")
        lines.append("  customer_orders:")
        lines.append("    sources: { c: customer_clean, o: stg_orders }")
        lines.append("    domain_sources:")
        lines.append("      eu: { o: stg_orders_eu_only }        # override just the orders source for EU")
        lines.append("```")
        lines.append("")

        # Per-domain workflows
        domain_wf_mode = forge_config.get("domain_workflows", "separate")
        lines.append("### Per-domain Databricks workflows")
        lines.append("")
        lines.append("Each domain gets its own Databricks workflow by default, so domains")
        lines.append("run in parallel on separate schedules if needed.")
        lines.append("")
        lines.append(f"**Current mode:** `{domain_wf_mode}`")
        lines.append("")
        lines.append("| Mode | Behavior |")
        lines.append("|------|----------|")
        lines.append("| `separate` (default) | One workflow per domain: `PROCESS_demo_eu`, `PROCESS_demo_us`, etc. |")
        lines.append("| `shared` | Single workflow containing all domain instances |")
        lines.append("")
        lines.append("To switch to a shared workflow, add to forge.yml:")
        lines.append("")
        lines.append("```yaml")
        lines.append("domain_workflows: shared")
        lines.append("```")
        lines.append("")

        # forge.yml config reference
        lines.append("### forge.yml domain config")
        lines.append("")
        lines.append("```yaml")
        lines.append("# In forge.yml:")
        lines.append("domains:")
        for dname, dcfg in domains.items():
            suffix = dcfg.get("schema_suffix", f"_{dname}")
            lines.append(f'  {dname}: {{ schema_suffix: "{suffix}" }}')
        lines.append(f"domain_layers: {domain_layers}")
        lines.append("```")
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

    # ── DAB Workflow ─────────────────────────────
    lines.append("## Databricks Asset Bundle Workflow")
    lines.append("")
    lines.append("`forge deploy` automatically generates a DAB workflow YAML after every successful run.")
    lines.append("")
    wf_name = forge_config.get("name", "unnamed")
    wf_path = f"resources/jobs/{wf_name}.yml"
    lines.append(f"- **Workflow YAML**: `{wf_path}`")
    lines.append(f"- Open this file to see the full pipeline: stages, tasks, compute, and schedule.")
    lines.append(f"- To regenerate manually: `forge workflow --dab`")
    lines.append(f"- To see the pipeline as a diagram: `forge workflow --mermaid`")
    lines.append("")
    lines.append("The graph (`forge diff`) includes a **DAB Workflow** node linked to every model,")
    lines.append("so you can see orchestration alongside data lineage.")
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
