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
        if inject_layer and section == "models" and isinstance(defn, dict):
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

def compile_udf_sql(udf_name: str, udf_def: dict, schema: str = "{{ target.catalog }}.{{ target.schema }}") -> str:
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
            entry["database"] = cat
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

    Output goes to ``{output_dir}/_volumes/``.
    """
    volumes = load_volumes(ddl_path, forge_config=forge_config)
    if not volumes:
        return {}

    out_dir = output_dir / "_volumes"
    out_dir.mkdir(parents=True, exist_ok=True)

    domains = (forge_config or {}).get("domains", {})
    domain_layers = set((forge_config or {}).get("domain_layers", []))

    # Resolve default catalog/schema from forge_config
    profile_name = (forge_config or {}).get("active_profile", "dev")
    profiles = (forge_config or {}).get("profiles", {})
    prof = profiles.get(profile_name, {})
    default_catalog = prof.get("catalog", (forge_config or {}).get("catalog", "main"))
    default_schema = prof.get("schema", (forge_config or {}).get("schema", "default"))

    results: dict[str, Path] = {}

    for vol_name, vol_def in volumes.items():
        vol_catalog = vol_def.get("catalog", default_catalog)
        vol_layer = vol_def.get("layer", "bronze")
        vol_domain_enabled = vol_def.get("domain", True) is not False
        is_bifurcated = domains and vol_layer in domain_layers and vol_domain_enabled

        if is_bifurcated:
            base_schema = _resolve_base_schema(forge_config, vol_layer) if forge_config else default_schema
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

                sql = compile_volume_sql(instance_name, d_def, vol_catalog, d_schema)
                out_path = out_dir / f"{instance_name}.sql"
                out_path.write_text(sql)
                results[instance_name] = out_path
        else:
            sql = compile_volume_sql(vol_name, vol_def, vol_catalog, default_schema)
            out_path = out_dir / f"{vol_name}.sql"
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
        sources_dir.mkdir(parents=True, exist_ok=True)
        sources_path = sources_dir / "_sources.yml"
        sources_path.write_text(sources_yml)
        results["_sources"] = sources_path

    # Generate Volume DDL → _volumes/ (CREATE VOLUME SQL)
    volume_results = compile_volumes(ddl_path, output_dir, forge_config=forge_config)
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
) -> str:
    """
    Compile a model to standalone SQL — no Jinja, no dbt.

    Emits CREATE OR REPLACE TABLE/VIEW with fully qualified table names.
    source_locations maps model_name → (catalog, schema) for cross-schema refs.
    """
    if not deploy_ts:
        from datetime import datetime, timezone
        deploy_ts = datetime.now(timezone.utc).isoformat()

    materialized = model_def.get("materialized", "view")
    columns = model_def.get("columns", {})
    is_join = "sources" in model_def or "join" in model_def
    is_agg = "group_by" in model_def

    lines: list[str] = []
    lines.append(f"-- Model: {model_name}")
    lines.append(f"-- Generated by: forge compile --pure-sql")
    lines.append(f"-- No dbt required. Runs directly on SQL warehouse.")
    lines.append("")

    # DDL wrapper
    fq_name = f"`{catalog}`.`{schema}`.`{model_name}`"
    if materialized == "view":
        lines.append(f"CREATE OR REPLACE VIEW {fq_name} AS")
    else:
        lines.append(f"CREATE OR REPLACE TABLE {fq_name}")
        lines.append("USING DELTA")
        lines.append("AS")

    # SELECT columns
    col_lines = []
    for col_name, col_def in columns.items():
        if isinstance(col_def, str):
            col_def = {"type": col_def}
        elif col_def is None:
            col_def = {}
        col_lines.append(_compile_column_select(col_name, col_def, None))

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
        f"CREATE OR REPLACE TABLE {fq_quarantine}\n"
        f"USING DELTA\n"
        f"AS\n"
        f"SELECT *,\n"
        f"    '{model_name}' AS _quarantine_source,\n"
        f"    '{git_commit}' AS _quarantine_git_commit,\n"
        f"    '{deploy_ts}' AS _quarantine_detected_at\n"
        f"FROM {fq_model}\n"
        f"WHERE {condition};\n"
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
    output_dir.mkdir(parents=True, exist_ok=True)
    results: dict[str, Path] = {}

    # Build a lookup of resolved (catalog, schema) per model
    model_locations: dict[str, tuple[str, str]] = {}
    for model_name, model_def in models.items():
        model_locations[model_name] = resolve_model_schema(
            model_name, model_def, prof, forge_config=forge_config,
        )

    seq = 0

    # Step 0: UDFs (must exist before any model references them)
    if udfs:
        udf_lines = []
        for udf_name, udf_def in udfs.items():
            fq_schema = f"`{catalog}`.`{schema}`"
            udf_lines.append(compile_udf_sql(udf_name, udf_def, schema=fq_schema))
            udf_lines.append("")
        udf_path = output_dir / f"{seq:03d}_udfs.sql"
        udf_path.write_text("\n".join(udf_lines))
        results["_udfs"] = udf_path
        seq += 1

    # Step 1..N: Models in dependency order
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
