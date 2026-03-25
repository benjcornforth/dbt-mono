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


_DBT_PROJECT_TEMPLATE = """# =============================================
# dbt_project.yml
# =============================================
# Forge-generated dbt project config.
# Source of truth lives in forge.yml + dbt/ddl/.

name: '{project_name}'
version: '0.2.0'
config-version: 2

profile: '{project_name}'

model-paths: ["dbt/models", "dbt/sources"]
seed-paths: ["dbt/seeds"]
test-paths: ["dbt/tests"]
analysis-paths: ["dbt/analysis"]
macro-paths: ["macros", "artifacts/runtime/macros"]
snapshot-paths: ["dbt/snapshots"]
asset-paths: ["dbt/assets"]

clean-targets:
    - "dbt/target"
    - "dbt_packages"

vars:
    git_commit: "local"
    compute_type: "serverless"
    methodology_version: "v1"
    catalog_bronze: "dev_fd_bronze"
    catalog_silver: "dev_fd_silver"
    catalog_meta: "dev_fd_meta"
    schema_bronze: "ben_sales"
    schema_silver: "ben_sales"
    schema_gold: "ben_sales"

seeds:
    {project_name}:
        ingestion_config:
            +database: "{{{{ var('catalog_meta', 'dev_fd_meta') }}}}"
            +schema: config

models:
    dbt_forge:
        +materialized: view
"""


def generate_dbt_project_yml(
        forge_config: dict[str, Any],
        output_path: Path | None = None,
        *,
        overwrite: bool = True,
) -> str:
        """Generate a dbt_project.yml from forge.yml.

        If overwrite is False and the file already exists, it is left untouched.
        """
        project_name = forge_config.get("name", "forge_project")
        text = _DBT_PROJECT_TEMPLATE.format(project_name=project_name)

        if output_path:
                if output_path.exists() and not overwrite:
                        return output_path.read_text()
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_text(text)

        return text


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


@dataclass
class AssetLocation:
    """Resolved placement information for an authored or system asset."""

    name: str
    kind: str
    class_name: str
    layer: str
    placement_family: str
    catalog: str
    schema: str
    domain: str | None = None
    namespace: str | None = None
    system: str | None = None
    tokens: dict[str, str] = field(default_factory=dict)


# =============================================
# READ ~/.databrickscfg
# =============================================

_DATABRICKSCFG_PATH = Path.home() / ".databrickscfg"


def resolve_databrickscfg_path(cfg_path: Path | None = None) -> Path:
    """Resolve the Databricks config path, honoring DATABRICKS_CONFIG_FILE if set."""
    if cfg_path is not None:
        return cfg_path
    override = os.environ.get("DATABRICKS_CONFIG_FILE")
    if override:
        return Path(override).expanduser()
    return _DATABRICKSCFG_PATH


def inspect_databrickscfg_profile(
    profile_name: str = "DEFAULT",
    cfg_path: Path | None = None,
    *,
    require_http_path: bool = False,
) -> dict[str, Any]:
    """Inspect one Databricks profile and report whether required fields are available."""
    path = resolve_databrickscfg_path(cfg_path)
    status: dict[str, Any] = {
        "path": str(path),
        "profile": profile_name,
        "exists": path.exists(),
        "found": False,
        "host": None,
        "has_token": False,
        "has_http_path": False,
        "has_cluster_id": False,
        "missing": [],
    }

    if not path.exists():
        status["missing"] = ["config_file"]
        return status

    cfg = configparser.ConfigParser()
    cfg.read(path)
    if profile_name not in cfg:
        status["missing"] = ["profile"]
        return status

    status["found"] = True
    creds = read_databrickscfg(profile_name, cfg_path=path)
    status["host"] = creds.get("host")
    status["has_token"] = bool(creds.get("token"))
    status["has_http_path"] = bool(creds.get("http_path"))
    status["has_cluster_id"] = bool(creds.get("cluster_id"))

    missing: list[str] = []
    if not creds.get("host"):
        missing.append("host")
    if not creds.get("token"):
        missing.append("token")
    if require_http_path and not creds.get("http_path"):
        missing.append("http_path")
    status["missing"] = missing
    return status


def require_databrickscfg_profile(
    profile: dict[str, Any],
    cfg_path: Path | None = None,
    *,
    require_http_path: bool = True,
) -> dict[str, str]:
    """Fail fast with actionable guidance if a Forge Databricks profile lacks required credentials."""
    if profile.get("platform", "databricks") != "databricks":
        return {}

    dbr_profile = profile.get("databricks_profile") or profile.get("_name", "DEFAULT")
    status = inspect_databrickscfg_profile(
        dbr_profile,
        cfg_path=cfg_path,
        require_http_path=require_http_path,
    )
    if not status["missing"]:
        return read_databrickscfg(dbr_profile, cfg_path=resolve_databrickscfg_path(cfg_path))

    path = status["path"]
    missing = ", ".join(status["missing"])
    raise RuntimeError(
        "Databricks credentials are not available for this Forge profile.\n"
        f"  Forge profile: {profile.get('_name', '(unknown)')}\n"
        f"  Databricks profile: {dbr_profile}\n"
        f"  Config path: {path}\n"
        f"  Missing: {missing}\n"
        f"  Fix: update ~/.databrickscfg profile [{dbr_profile}] with the missing fields\n"
        f"  Then validate with: forge auth --dbt --profile {profile.get('_name', dbr_profile)}"
    )


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
    path = resolve_databrickscfg_path(cfg_path)

    if not path.exists():
        raise FileNotFoundError(
            f"Databricks config not found at {path}.\n"
                f"Create or update profile [{profile_name}] in ~/.databrickscfg.\n"
                f"Then validate with: forge auth --dbt --profile {profile_name}"
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
    path = resolve_databrickscfg_path(cfg_path)
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
            2. First entry in forge.yml profiles:
      3. Legacy flat keys (backward compatible)

    Returns a merged dict with: platform, catalog, schema, compute, etc.
    """
    profiles = forge_config.get("profiles", {})

    # Determine which profile to use
    name = profile_name

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
        # Try databrickscfg profile first (fall back to forge profile name)
        dbr_profile = profile.get("databricks_profile") or profile.get("_name", "DEFAULT")
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

ASSET_CLASSES = {"domain", "shared", "system"}

ALLOWED_LAYERS_BY_CLASS = {
    "domain": {"bronze", "silver", "gold"},
    "shared": {"meta", "operations"},
    "system": {"meta", "operations"},
}

REQUIRED_IDENTITY_BY_CLASS = {
    "domain": "domain",
    "shared": "namespace",
    "system": "system",
}


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


def _sanitize_identity(value: str) -> str:
    """Normalize user-provided identity values so they are safe in object names."""
    return re.sub(r"[^a-z0-9]+", "_", str(value).lower()).strip("_")


def _current_user_token() -> str:
    raw_user = getpass.getuser()
    return _sanitize_identity(raw_user)


def _expand_named_pattern(
    pattern: str,
    *,
    tokens: dict[str, str],
    skip_envs: set[str] | None = None,
) -> str:
    """Expand a v1 placement pattern using an arbitrary token dictionary."""
    result = pattern
    env = tokens.get("env", "")
    if skip_envs and env in skip_envs:
        result = result.replace("{env}_", "").replace("{env}", "")

    for key, value in tokens.items():
        result = result.replace(f"{{{key}}}", value)

    while "__" in result:
        result = result.replace("__", "_")
    return result.strip("_")


def _uses_v1_placements(forge_config: dict | None) -> bool:
    placements = (forge_config or {}).get("placements", {})
    return isinstance(placements.get("families"), dict) and bool(placements["families"])


def _resolve_v1_profile_overrides(
    profile: dict[str, Any],
    placement_family: str,
) -> dict[str, Any]:
    overrides = profile.get("overrides", {})
    placement_overrides = overrides.get("placement_families", {})
    resolved = placement_overrides.get(placement_family, {})
    return resolved if isinstance(resolved, dict) else {}


def _validate_asset_class(class_name: str) -> None:
    if class_name not in ASSET_CLASSES:
        raise ValueError(
            f"Invalid asset class '{class_name}'. Expected one of: {', '.join(sorted(ASSET_CLASSES))}."
        )


def _validate_layer_for_class(class_name: str, layer: str) -> None:
    allowed = ALLOWED_LAYERS_BY_CLASS[class_name]
    if layer not in allowed:
        raise ValueError(
            f"Invalid layer '{layer}' for class '{class_name}'. "
            f"Allowed layers: {', '.join(sorted(allowed))}."
        )


def _resolve_v1_identity_tokens(
    *,
    asset_name: str,
    asset_kind: str,
    asset_def: dict[str, Any],
    class_name: str,
    forge_config: dict[str, Any],
) -> tuple[str, str | None, str | None, str | None]:
    """Resolve class-specific identity fields and default placement family."""
    placement_family = asset_def.get("placement_family")
    domain = asset_def.get("domain")
    namespace = asset_def.get("namespace")
    system = asset_def.get("system")

    if class_name == "domain":
        if not domain:
            domains = forge_config.get("domains", {})
            if asset_name in domains and isinstance(domains[asset_name], dict):
                domain = asset_name
        if not domain:
            raise ValueError(f"Asset '{asset_name}' is class 'domain' but does not declare 'domain'.")
        domain = _sanitize_identity(domain)
        if not placement_family:
            domain_cfg = forge_config.get("domains", {}).get(domain, {})
            if isinstance(domain_cfg, dict):
                placement_family = domain_cfg.get("placement_family")

    elif class_name == "shared":
        if not namespace:
            raise ValueError(f"Asset '{asset_name}' is class 'shared' but does not declare 'namespace'.")
        namespace = _sanitize_identity(namespace)
        if not placement_family:
            ns_cfg = forge_config.get("shared_namespaces", {}).get(namespace, {})
            if isinstance(ns_cfg, dict):
                placement_family = ns_cfg.get("placement_family")
                asset_def.setdefault("layer", ns_cfg.get("layer", asset_def.get("layer")))

    elif class_name == "system":
        if not system:
            system_cfg = forge_config.get("system_assets", {}).get(asset_name, {})
            if isinstance(system_cfg, dict):
                system = system_cfg.get("system")
                placement_family = placement_family or system_cfg.get("placement_family")
                asset_def.setdefault("layer", system_cfg.get("layer", asset_def.get("layer")))
        if not system:
            raise ValueError(f"Asset '{asset_name}' is class 'system' but does not declare 'system'.")
        system = _sanitize_identity(system)

    return placement_family or "", domain, namespace, system


def resolve_asset_location(
    asset_name: str,
    asset_def: dict[str, Any],
    profile: dict[str, Any],
    forge_config: dict | None = None,
    *,
    asset_kind: str = "model",
) -> AssetLocation:
    """Resolve a fully-qualified location for a v1 or legacy asset definition."""
    forge_config = forge_config or {}
    asset_def = dict(asset_def or {})

    if not _uses_v1_placements(forge_config):
        catalog, schema = resolve_model_schema(asset_name, asset_def, profile, forge_config=forge_config)
        return AssetLocation(
            name=asset_name,
            kind=asset_kind,
            class_name=asset_def.get("class", "domain"),
            layer=asset_def.get("layer", ""),
            placement_family="legacy",
            catalog=catalog,
            schema=schema,
            domain=asset_def.get("domain"),
            namespace=asset_def.get("namespace"),
            system=asset_def.get("system"),
        )

    profile = dict(profile)
    env = str(profile.get("env", profile.get("_name", "dev"))).lower()
    class_name = asset_def.get("class", "domain")
    _validate_asset_class(class_name)

    placement_family, domain, namespace, system = _resolve_v1_identity_tokens(
        asset_name=asset_name,
        asset_kind=asset_kind,
        asset_def=asset_def,
        class_name=class_name,
        forge_config=forge_config,
    )
    if not placement_family:
        raise ValueError(
            f"Asset '{asset_name}' does not resolve a placement_family. "
            f"Declare one explicitly or configure it in forge.yml."
        )

    layer = asset_def.get("layer")
    if not layer:
        raise ValueError(f"Asset '{asset_name}' does not declare a layer.")
    layer = _sanitize_identity(layer)
    _validate_layer_for_class(class_name, layer)

    families = forge_config.get("placements", {}).get("families", {})
    family = families.get(placement_family)
    if not isinstance(family, dict):
        raise ValueError(
            f"Asset '{asset_name}' references placement_family '{placement_family}' which is not defined in forge.yml."
        )

    family_classes = set(family.get("allowed_classes", []))
    if family_classes and class_name not in family_classes:
        raise ValueError(
            f"Placement family '{placement_family}' does not allow class '{class_name}'."
        )

    family_layers = set(family.get("allowed_layers", []))
    if family_layers and layer not in family_layers:
        raise ValueError(
            f"Placement family '{placement_family}' does not allow layer '{layer}'."
        )

    tokens = {
        "asset": _sanitize_identity(asset_name),
        "catalog": layer,
        "domain": domain or "",
        "env": env,
        "id": _sanitize_identity(forge_config.get("id", forge_config.get("name", "project"))),
        "kind": _sanitize_identity(asset_kind),
        "layer": layer,
        "name": _sanitize_identity(asset_name),
        "namespace": namespace or "",
        "schema": layer,
        "scope": _sanitize_identity(forge_config.get("scope", "")),
        "system": system or "",
        "user": _current_user_token(),
    }

    overrides = _resolve_v1_profile_overrides(profile, placement_family)
    catalog_pattern = overrides.get("catalog_pattern") or family.get("catalog_pattern")
    schema_pattern = overrides.get("schema_pattern") or family.get("schema_pattern")
    if not catalog_pattern and not overrides.get("catalog"):
        raise ValueError(f"Placement family '{placement_family}' has no catalog_pattern.")
    if not schema_pattern and not overrides.get("schema"):
        raise ValueError(f"Placement family '{placement_family}' has no schema_pattern.")

    skip_envs = set(forge_config.get("skip_env_prefix", ["prd", "prod"]))
    catalog = overrides.get("catalog") or _expand_named_pattern(
        catalog_pattern, tokens=tokens, skip_envs=set(),
    )
    schema = overrides.get("schema") or _expand_named_pattern(
        schema_pattern, tokens=tokens, skip_envs=skip_envs,
    )

    # Allow explicit asset-level override as the final escape hatch.
    catalog = asset_def.get("catalog") or catalog
    schema = asset_def.get("schema") or schema
    if not catalog or not schema:
        raise ValueError(f"Asset '{asset_name}' resolved to an empty catalog/schema.")

    return AssetLocation(
        name=asset_name,
        kind=asset_kind,
        class_name=class_name,
        layer=layer,
        placement_family=placement_family,
        catalog=catalog,
        schema=schema,
        domain=domain,
        namespace=namespace,
        system=system,
        tokens=tokens,
    )


def _v1_layer_maps(
    env: str,
    forge_config: dict[str, Any],
) -> tuple[dict[str, str], dict[str, str]]:
    """Build catalogs and schemas dicts from v1 placement families.

    Walks each placement family, expands its ``catalog_pattern`` and
    ``schema_pattern`` for every allowed layer, and returns two maps
    keyed by logical layer name (bronze, silver, meta, …).

    Identity tokens (domain / namespace / system) are resolved from
    the first matching entry in ``domains:``, ``shared_namespaces:``,
    or ``system_assets:`` respectively.
    """
    scope = _sanitize_identity(forge_config.get("scope", ""))
    project_id = _sanitize_identity(
        forge_config.get("id", forge_config.get("name", "project")),
    )
    user = _current_user_token()
    skip_envs = set(forge_config.get("skip_env_prefix", ["prd", "prod"]))

    families = forge_config.get("placements", {}).get("families", {})
    domains_cfg = forge_config.get("domains", {})
    namespaces_cfg = forge_config.get("shared_namespaces", {})

    # Build layer → (family_name, family_def) — first family wins per layer
    layer_family: dict[str, tuple[str, dict]] = {}
    for fname, fdef in families.items():
        if not isinstance(fdef, dict):
            continue
        for layer in fdef.get("allowed_layers", []):
            if layer not in layer_family:
                layer_family[layer] = (fname, fdef)

    catalogs: dict[str, str] = {}
    schemas: dict[str, str] = {}

    for layer, (fname, fdef) in layer_family.items():
        classes = set(fdef.get("allowed_classes", []))

        # Resolve the default identity token for this family's class
        identity: dict[str, str] = {"domain": "", "namespace": "", "system": ""}
        if "domain" in classes:
            identity["domain"] = _sanitize_identity(
                next(iter(domains_cfg), project_id),
            )
        if "shared" in classes:
            for ns_name, ns_def in namespaces_cfg.items():
                if isinstance(ns_def, dict) and ns_def.get("placement_family") == fname:
                    identity["namespace"] = _sanitize_identity(ns_name)
                    break

        tokens = {
            "env": env, "scope": scope, "id": project_id, "user": user,
            "layer": layer, "catalog": layer, "schema": layer,
            **identity,
        }

        cat_pattern = fdef.get("catalog_pattern")
        if cat_pattern:
            catalogs[layer] = _expand_named_pattern(
                cat_pattern, tokens=tokens, skip_envs=set(),
            )

        sch_pattern = fdef.get("schema_pattern")
        if sch_pattern:
            schemas[layer] = _expand_named_pattern(
                sch_pattern, tokens=tokens, skip_envs=skip_envs,
            )

    return catalogs, schemas


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

    # ---- v1 placement families ----
    if _uses_v1_placements(forge_config):
        if not profile.get("catalogs") or not profile.get("schemas"):
            catalogs_map, schemas_map = _v1_layer_maps(env, forge_config)
            if not profile.get("catalogs") and catalogs_map:
                profile["catalogs"] = catalogs_map
            if not profile.get("schemas") and schemas_map:
                profile["schemas"] = schemas_map
        base_catalog = profile.get("catalog", "bronze")
        if base_catalog and "catalogs" in profile:
            profile["catalog"] = profile["catalogs"].get(base_catalog, base_catalog)
        if "schemas" in profile:
            profile["schema"] = profile["schemas"].get(
                "silver", next(iter(profile["schemas"].values())),
            )
        return profile

    # ---- legacy path ----
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


def resolve_model_schema(
    model_name: str,
    model_def: dict,
    profile: dict[str, Any],
    forge_config: dict | None = None,
) -> tuple[str, str]:
    """
    Resolve catalog and schema for a specific model.

        Resolution order:
            1. Explicit model-level schema/catalog keys in DDL
            2. Explicit model-level layer injected from canonical dbt/ddl tree
            3. Profile-level defaults (catalog, schema) only when explicit schema is set

    Automatically expands naming patterns via ``forge_config``.

    Returns (catalog, schema).
    """
    if _uses_v1_placements(forge_config):
        location = resolve_asset_location(
            model_name,
            model_def,
            profile,
            forge_config=forge_config,
            asset_kind="model",
        )
        return location.catalog, location.schema

    profile = _expand_env_prefix(profile, forge_config)
    schemas_map = profile.get("schemas", {})
    catalogs_map = profile.get("catalogs", {})
    default_catalog = profile.get("catalog", "main")
    default_schema = profile.get("schema", "silver")

    # 1. Explicit model-level override
    model_schema = model_def.get("schema")
    model_catalog = model_def.get("catalog")

    # Resolve logical names through the maps whenever provided
    if model_catalog and catalogs_map:
        model_catalog = catalogs_map.get(model_catalog, model_catalog)
    if model_schema and schemas_map:
        model_schema = schemas_map.get(model_schema, model_schema)

    if model_schema and model_catalog:
        return model_catalog, model_schema
    if model_schema:
        return model_catalog or default_catalog, model_schema

    # 2. Resolve from the explicit canonical-tree layer
    layer = model_def.get("layer")
    if layer and schemas_map:
        resolved_schema = schemas_map.get(layer, default_schema)
        resolved_catalog = catalogs_map.get(layer, default_catalog) if catalogs_map else default_catalog
        return model_catalog or resolved_catalog, resolved_schema

    if layer:
        return model_catalog or default_catalog, default_schema

    raise ValueError(
        f"Model '{model_name}' has no explicit layer. Define it via the canonical dbt/ddl/<layer>/... tree "
        f"or provide an explicit schema/catalog override in its DDL."
    )


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

    # Auto-inject archive_table when operations catalog exists
    if "catalog_operations" in variables:
        fc = forge_config or {}
        skip_envs = set(fc.get("skip_env_prefix", ["prd", "prod"]))

        if _uses_v1_placements(fc):
            # Resolve backups schema from the system_assets → backups family
            systems = fc.get("system_assets", {})
            backups_def = systems.get("backups", {})
            backups_family_name = backups_def.get("placement_family", "")
            families = fc.get("placements", {}).get("families", {})
            backups_family = families.get(backups_family_name, {})
            sch_pattern = backups_family.get("schema_pattern", "{user}_{system}")
            tokens = {
                "env": profile.get("env", "dev"),
                "scope": _sanitize_identity(fc.get("scope", "")),
                "id": _sanitize_identity(fc.get("id", fc.get("name", "project"))),
                "user": _current_user_token(),
                "layer": "operations",
                "system": _sanitize_identity(backups_def.get("system", "backups")),
                "namespace": "", "domain": "",
            }
            backups_schema = _expand_named_pattern(
                sch_pattern, tokens=tokens, skip_envs=skip_envs,
            )
        else:
            user = re.sub(r"[^a-z0-9]+", "_", getpass.getuser().lower()).strip("_")
            backups_schema = _apply_pattern(
                fc.get("schema_pattern", "{user}_{id}"),
                env=profile.get("env", "dev"),
                project_id="backups",
                logical_name="backups",
                skip_envs=skip_envs,
                scope=fc.get("scope", ""),
                user=user,
            )

        variables["schema_backups"] = backups_schema
        variables["archive_table"] = (
            f"{variables['catalog_operations']}.{backups_schema}._backup_archive"
        )

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
    default_target = next(iter(profiles), "dev")

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
            "target": default_target if default_target in outputs else next(iter(outputs)),
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
