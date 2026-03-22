# =============================================
# src/forge/compute_resolver.py
# =============================================
# THE CODE IS THE DOCUMENTATION
#
# Two responsibilities:
#   1. Resolve Databricks connection from ~/.databrickscfg profiles
#   2. Resolve compute type (serverless/dedicated) per profile
#
# ~/.databrickscfg is the standard Databricks CLI config file:
#   [DEFAULT]
#   host  = https://xxx.cloud.databricks.com
#   token = dapiXXX
#
#   [PROD]
#   host  = https://prod.cloud.databricks.com
#   token = dapiYYY
#
# forge.yml references a profile by name:
#   profiles:
#     dev:
#       databricks_profile: DEFAULT
#     prod:
#       databricks_profile: PROD

from __future__ import annotations

import configparser
import getpass
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


# =============================================
# CONNECTION DETAILS
# =============================================

@dataclass
class ConnectionInfo:
    """Resolved connection details for a forge profile."""
    platform: str = "databricks"
    host: str | None = None
    token: str | None = None
    http_path: str | None = None
    catalog: str = "main"
    schema: str = "silver"
    compute_type: str = "serverless"
    cluster_id: str | None = None
    # Postgres / generic
    port: int | None = None
    database: str | None = None
    user: str | None = None
    password: str | None = None

    @property
    def is_databricks(self) -> bool:
        return self.platform == "databricks"


# =============================================
# READ ~/.databrickscfg
# =============================================

_DATABRICKSCFG_PATH = Path.home() / ".databrickscfg"


def read_databrickscfg(
    profile_name: str = "DEFAULT",
    cfg_path: Path | None = None,
) -> dict[str, str]:
    """
    Read a named profile from ~/.databrickscfg.

    Returns dict with keys: host, token, (optionally) http_path, cluster_id.
    Raises FileNotFoundError if the config file is missing.
    Raises KeyError if the profile doesn't exist.
    """
    path = cfg_path or _DATABRICKSCFG_PATH

    if not path.exists():
        raise FileNotFoundError(
            f"Databricks config not found at {path}.\n"
            f"Create it with: databricks configure --profile {profile_name}\n"
            f"Or set env vars: DBT_DATABRICKS_HOST, DBT_DATABRICKS_TOKEN"
        )

    cfg = configparser.ConfigParser()
    cfg.read(path)

    if profile_name not in cfg:
        available = [s for s in cfg.sections() if s != "DEFAULT"] or ["(none)"]
        raise KeyError(
            f"Profile [{profile_name}] not found in {path}.\n"
            f"Available profiles: {', '.join(available)}"
        )

    section = cfg[profile_name]
    result = {}
    for key in ("host", "token", "http_path", "cluster_id", "account_id"):
        val = section.get(key)
        if val:
            result[key] = val.strip()

    # Normalise host — strip trailing slash, ensure https://
    if "host" in result:
        h = result["host"].rstrip("/")
        if not h.startswith("http"):
            h = f"https://{h}"
        result["host"] = h

    return result


def list_databrickscfg_profiles(cfg_path: Path | None = None) -> list[str]:
    """List available profile names from ~/.databrickscfg."""
    path = cfg_path or _DATABRICKSCFG_PATH
    if not path.exists():
        return []
    cfg = configparser.ConfigParser()
    cfg.read(path)
    return list(cfg.keys())


# =============================================
# RESOLVE A FORGE PROFILE
# =============================================

def resolve_profile(
    forge_config: dict,
    profile_name: str | None = None,
) -> dict[str, Any]:
    """
    Resolve the active profile from forge.yml.

    Profile resolution order:
      1. Explicit --profile flag
      2. forge.yml → active_profile key
      3. Legacy flat keys (backward compatible)

    Returns a merged dict with: platform, catalog, schema, compute, etc.
    """
    profiles = forge_config.get("profiles", {})

    # Determine which profile to use
    name = profile_name or forge_config.get("active_profile")

    if name and name in profiles:
        profile = dict(profiles[name])
    elif profiles:
        # Use first profile if none specified
        name = next(iter(profiles))
        profile = dict(profiles[name])
    else:
        # Legacy: flat keys (backward compatible)
        profile = {
            "platform": forge_config.get("target_platform", "databricks"),
            "catalog": forge_config.get("catalog", "main"),
            "schema": forge_config.get("schema", "silver"),
            "compute": forge_config.get("compute", {"type": "serverless"}),
            "features": forge_config.get("features", {}),
        }
        name = forge_config.get("environment", "dev")

    # Ensure computed defaults
    profile.setdefault("platform", forge_config.get("target_platform", "databricks"))
    profile.setdefault("catalog", forge_config.get("catalog", "main"))
    profile.setdefault("schema", forge_config.get("schema", "silver"))
    profile.setdefault("compute", forge_config.get("compute", {"type": "serverless"}))
    profile["_name"] = name

    return profile


# =============================================
# RESOLVE CONNECTION (databrickscfg + env vars)
# =============================================

def resolve_connection(
    profile: dict[str, Any],
    cfg_path: Path | None = None,
) -> ConnectionInfo:
    """
    Build a ConnectionInfo from a resolved profile.

    Priority:
      1. databricks_profile → reads ~/.databrickscfg
      2. connection: block in forge.yml profile
      3. Environment variables (DBT_DATABRICKS_*)
    """
    platform = profile.get("platform", "databricks")
    compute = profile.get("compute", {})
    compute_type = compute.get("type", "serverless") if isinstance(compute, dict) else compute

    info = ConnectionInfo(
        platform=platform,
        catalog=profile.get("catalog", "main"),
        schema=profile.get("schema", "silver"),
        compute_type=compute_type,
        cluster_id=compute.get("cluster_id") if isinstance(compute, dict) else None,
    )

    if platform == "databricks":
        # Try databrickscfg profile first
        dbr_profile = profile.get("databricks_profile")
        if dbr_profile:
            try:
                creds = read_databrickscfg(dbr_profile, cfg_path=cfg_path)
                info.host = creds.get("host")
                info.token = creds.get("token")
                info.http_path = creds.get("http_path")
                if creds.get("cluster_id"):
                    info.cluster_id = creds["cluster_id"]
                return info
            except (FileNotFoundError, KeyError):
                pass  # Fall through to env vars

        # Try inline connection block
        conn = profile.get("connection", {})
        if conn:
            info.host = conn.get("host")
            info.token = conn.get("token")
            info.http_path = conn.get("http_path")
            if info.host:
                return info

        # Fall back to env vars
        info.host = os.environ.get("DBT_DATABRICKS_HOST")
        info.token = os.environ.get("DBT_DATABRICKS_TOKEN")
        info.http_path = os.environ.get("DBT_DATABRICKS_HTTP_PATH")

    elif platform in ("postgres", "redshift"):
        conn = profile.get("connection", {})
        info.host = conn.get("host", "localhost")
        info.port = conn.get("port", 5432)
        info.database = conn.get("database", info.catalog)
        info.user = conn.get("user")
        info.password = conn.get("password")

    return info


# =============================================
# GENERATE profiles.yml FROM FORGE PROFILES
# =============================================

_NO_PREFIX_ENVS = {"prd", "prod", ""}
"""Environments where the {env}_ token is dropped (production = base names)."""

_DEFAULT_SCHEMAS = ("bronze", "silver", "gold")
_DEFAULT_CATALOGS = ("bronze", "silver", "meta", "operations")


def _apply_pattern(
    pattern: str,
    *,
    env: str,
    project_id: str,
    logical_name: str,
    skip_envs: set[str],
    scope: str = "",
    user: str = "",
) -> str:
    """
    Expand a naming pattern like ``{env}_{scope}_{catalog}`` into a concrete name.

    Tokens:
      {env}     — environment tag (dev, int, prd)
      {id}      — project id from forge.yml top-level ``id:``
      {scope}   — project scope from forge.yml (e.g. fd, crm)
      {user}    — current OS / Databricks user (sanitised for catalog names)
      {catalog} — the logical catalog name (e.g. bronze, meta)
      {schema}  — the logical schema name  (e.g. bronze, silver)

    When ``env`` is in ``skip_envs`` (e.g. prd/prod) **and** the caller passes
    that set, the ``{env}_`` prefix (including trailing underscore) is removed —
    so ``{env}_{id}`` becomes just ``{id}`` for production schemas.
    Catalogs always keep the env prefix (pass empty ``skip_envs`` for catalogs).
    """
    result = pattern
    if env in skip_envs:
        # Strip "{env}_" or "{env}" from the pattern before substitution
        result = result.replace("{env}_", "").replace("{env}", "")
    else:
        result = result.replace("{env}", env)

    result = result.replace("{id}", project_id)
    result = result.replace("{scope}", scope)
    result = result.replace("{user}", user)
    result = result.replace("{catalog}", logical_name)
    result = result.replace("{schema}", logical_name)
    # Clean up any doubled underscores from empty tokens
    while "__" in result:
        result = result.replace("__", "_")
    return result.strip("_")


def _expand_env_prefix(
    profile: dict[str, Any],
    forge_config: dict | None = None,
) -> dict[str, Any]:
    """
    Expand ``env`` key into concrete ``schemas`` and ``catalogs`` dicts
    using the naming patterns from forge.yml.

    Uses:
      - ``catalog_pattern`` (default ``{env}_{id}_{catalog}``)
      - ``schema_pattern``  (default ``{env}_{schema}``)
      - ``skip_env_prefix`` (default ``[prd, prod]``)
      - Top-level ``catalogs:`` list and ``schemas:`` list for logical names
      - Top-level ``id:`` for the ``{id}`` token

    If the profile already has explicit ``schemas:`` / ``catalogs:`` dicts
    (e.g. the ``local`` profile), they are left untouched.
    """
    forge_config = forge_config or {}
    env = profile.get("env", "").strip().lower()
    if not env:
        return profile

    profile = dict(profile)  # shallow copy

    project_id = forge_config.get("id", forge_config.get("name", "project")).replace("-", "_")
    scope = forge_config.get("scope", "").replace("-", "_")
    skip_envs = set(forge_config.get("skip_env_prefix", ["prd", "prod"]))
    # Resolve current user — sanitised for use in catalog/schema names
    raw_user = getpass.getuser()
    user = re.sub(r"[^a-z0-9]+", "_", raw_user.lower()).strip("_")

    catalog_pattern = forge_config.get("catalog_pattern", "{env}_{id}_{catalog}")
    schema_pattern = forge_config.get("schema_pattern", "{env}_{schema}")

    pattern_kwargs = dict(env=env, project_id=project_id, scope=scope, user=user)

    # --- schemas (skip env prefix for prod envs) ---
    if not profile.get("schemas"):
        logical_schemas = forge_config.get("schemas", list(_DEFAULT_SCHEMAS))
        if isinstance(logical_schemas, list):
            schemas: dict[str, str] = {}
            for s in logical_schemas:
                schemas[s] = _apply_pattern(
                    schema_pattern, logical_name=s, skip_envs=skip_envs,
                    **pattern_kwargs,
                )
            profile["schemas"] = schemas

    # --- catalogs (always keep env prefix — pass empty skip_envs) ---
    if not profile.get("catalogs"):
        logical_catalogs = forge_config.get("catalogs", list(_DEFAULT_CATALOGS))
        if isinstance(logical_catalogs, list):
            catalogs: dict[str, str] = {}
            for c in logical_catalogs:
                catalogs[c] = _apply_pattern(
                    catalog_pattern, logical_name=c, skip_envs=set(),
                    **pattern_kwargs,
                )
            profile["catalogs"] = catalogs

    # --- default catalog (expand the profile's base catalog through the pattern) ---
    base_catalog = profile.get("catalog", "bronze")
    if base_catalog and "catalogs" in profile:
        profile["catalog"] = profile["catalogs"].get(base_catalog, base_catalog)

    # --- default schema (always set from expanded schemas when env-expanding) ---
    if "schemas" in profile:
        profile["schema"] = profile["schemas"].get("silver", next(iter(profile["schemas"].values())))

    return profile


# Model name prefix → medallion layer
_LAYER_PREFIXES: dict[str, str] = {
    "raw_": "bronze",
    "src_": "bronze",
    "seed_": "bronze",
    "stg_": "silver",
    "staging_": "silver",
    "clean_": "silver",
    "int_": "silver",
    "fct_": "gold",
    "dim_": "gold",
    "agg_": "gold",
    "rpt_": "gold",
    "pub_": "gold",
}


def resolve_model_schema(
    model_name: str,
    model_def: dict,
    profile: dict[str, Any],
    forge_config: dict | None = None,
) -> tuple[str, str]:
    """
    Resolve catalog and schema for a specific model.

    Resolution order:
      1. Explicit model-level schema/catalog keys in models.yml
      2. schemas: mapping (name prefix → layer → schema name)
      3. catalogs: mapping (layer → catalog name)
      4. Profile-level defaults (catalog, schema)

    Automatically expands naming patterns via ``forge_config``.

    Returns (catalog, schema).
    """
    profile = _expand_env_prefix(profile, forge_config)
    schemas_map = profile.get("schemas", {})
    catalogs_map = profile.get("catalogs", {})
    default_catalog = profile.get("catalog", "main")
    default_schema = profile.get("schema", "silver")

    # 1. Explicit model-level override
    model_schema = model_def.get("schema")
    model_catalog = model_def.get("catalog")
    if model_schema and model_catalog:
        return model_catalog, model_schema
    if model_schema:
        return model_catalog or default_catalog, model_schema

    # 2. Determine layer from model name prefix
    layer = model_def.get("layer")  # explicit layer key
    if not layer:
        for prefix, lyr in _LAYER_PREFIXES.items():
            if model_name.startswith(prefix):
                layer = lyr
                break

    # 3. Resolve from schemas/catalogs maps
    if layer and schemas_map:
        resolved_schema = schemas_map.get(layer, default_schema)
        resolved_catalog = catalogs_map.get(layer, default_catalog) if catalogs_map else default_catalog
        return model_catalog or resolved_catalog, resolved_schema

    return model_catalog or default_catalog, default_schema


def get_schema_variables(
    profile: dict[str, Any],
    forge_config: dict | None = None,
) -> dict[str, str]:
    """
    Build schema_* and catalog_* dbt variables from a profile.

    Given:
      schemas: { bronze: dev_bronze, silver: dev_silver, gold: dev_gold }
      catalogs: { bronze: dev_myproject_bronze, ... }
    Or:
      env: dev   (auto-expands using catalog_pattern / schema_pattern)

    Returns:
      { "schema_bronze": "dev_bronze", "schema_silver": "dev_silver",
        "schema_gold": "dev_gold",
        "catalog_bronze": "dev_myproject_bronze", ... }
    """
    profile = _expand_env_prefix(profile, forge_config)
    variables: dict[str, str] = {}
    for layer, schema_name in profile.get("schemas", {}).items():
        variables[f"schema_{layer}"] = schema_name
    for layer, catalog_name in profile.get("catalogs", {}).items():
        variables[f"catalog_{layer}"] = catalog_name
    return variables

def generate_profiles_yml(
    forge_config: dict,
    output_path: Path | None = None,
) -> str:
    """
    Auto-generate dbt profiles.yml from forge.yml profiles.

    Each forge profile becomes a dbt output target.
    Databricks profiles reference env vars or databrickscfg values.
    Schema variables (schema_bronze, schema_silver, etc.) are injected.
    """
    import yaml

    project_name = forge_config.get("name", "dbt_forge").replace("-", "_")
    profiles = forge_config.get("profiles", {})
    active = forge_config.get("active_profile", "dev")

    outputs: dict[str, dict] = {}

    if not profiles:
        # Legacy flat config → single dev output
        outputs["dev"] = {
            "type": "databricks",
            "catalog": f"{{{{ env_var('DBT_DATABRICKS_CATALOG', '{forge_config.get('catalog', 'main')}') }}}}",
            "schema": forge_config.get("schema", "silver"),
            "host": "{{ env_var('DBT_DATABRICKS_HOST') }}",
            "http_path": "{{ env_var('DBT_DATABRICKS_HTTP_PATH') }}",
            "token": "{{ env_var('DBT_DATABRICKS_TOKEN') }}",
            "threads": 4,
        }
    else:
        for name, prof in profiles.items():
            expanded = _expand_env_prefix(prof, forge_config)
            platform = prof.get("platform", "databricks")
            catalog = expanded.get("catalog", forge_config.get("catalog", "main"))
            schema = expanded.get("schema", forge_config.get("schema", "silver"))
            compute = prof.get("compute", {})
            threads = 8 if name == "prod" else 4

            if platform == "databricks":
                entry: dict[str, Any] = {
                    "type": "databricks",
                    "catalog": f"{{{{ env_var('DBT_DATABRICKS_CATALOG', '{catalog}') }}}}",
                    "schema": schema,
                    "host": "{{ env_var('DBT_DATABRICKS_HOST') }}",
                    "http_path": "{{ env_var('DBT_DATABRICKS_HTTP_PATH') }}",
                    "token": "{{ env_var('DBT_DATABRICKS_TOKEN') }}",
                    "threads": threads,
                }
                # Inject schema_* and catalog_* as dbt vars
                schema_vars = get_schema_variables(prof, forge_config)
                if schema_vars:
                    entry["vars"] = schema_vars
                outputs[name] = entry

            elif platform in ("postgres", "redshift"):
                conn = prof.get("connection", {})
                entry = {
                    "type": platform,
                    "host": conn.get("host", "localhost"),
                    "port": conn.get("port", 5432),
                    "dbname": conn.get("database", catalog),
                    "schema": schema,
                    "user": f"{{{{ env_var('DBT_PG_USER', '{conn.get('user', 'postgres')}') }}}}",
                    "password": "{{ env_var('DBT_PG_PASSWORD') }}",
                    "threads": threads,
                }
                schema_vars = get_schema_variables(prof, forge_config)
                if schema_vars:
                    entry["vars"] = schema_vars
                outputs[name] = entry

    profile_yml = {
        project_name: {
            "target": active if active in outputs else next(iter(outputs)),
            "outputs": outputs,
        }
    }

    text = yaml.dump(profile_yml, sort_keys=False, default_flow_style=False)

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(text)

    return text


# =============================================
# LEGACY COMPAT – simple resolve_compute
# =============================================

def resolve_compute(env: str) -> str:
    """Legacy API — prefer resolve_profile + resolve_connection."""
    return "serverless"
