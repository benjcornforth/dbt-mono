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
import os
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

def generate_profiles_yml(
    forge_config: dict,
    output_path: Path | None = None,
) -> str:
    """
    Auto-generate dbt profiles.yml from forge.yml profiles.

    Each forge profile becomes a dbt output target.
    Databricks profiles reference env vars or databrickscfg values.
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
            platform = prof.get("platform", "databricks")
            catalog = prof.get("catalog", forge_config.get("catalog", "main"))
            schema = prof.get("schema", forge_config.get("schema", "silver"))
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
