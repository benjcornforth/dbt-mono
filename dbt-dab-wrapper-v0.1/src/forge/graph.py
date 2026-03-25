# =============================================
# src/forge/graph.py
# =============================================
# THE CODE IS THE DOCUMENTATION
#
# The lineage graph engine. Reads dbt manifest.json,
# enriches with forge-specific nodes (quarantine,
# prior_version, Python UDFs), and produces an
# ODCS-aligned contract graph.
#
# ODCS = Open Data Contract Standard (https://github.com/bitol-io/open-data-contract-standard)
# Every node in the graph IS a data contract.
# Every edge IS a lineage relationship.
# Every diff IS an auditable change record.
#
# Usage:
#   graph = build_graph(forge_config, manifest_path)
#   diff  = diff_graphs(old_graph, new_graph)
#   text  = render_mermaid(graph, diff=diff)

from __future__ import annotations

import json
import hashlib
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml as _yaml

from forge.simple_ddl import load_raw_ddl as _load_raw_ddl


def _resolve_ddl_path(dbt_dir: Path) -> Path | None:
    """Return the canonical DDL directory, or None if not found."""
    ddl_dir = dbt_dir / "ddl"
    if ddl_dir.is_dir():
        return ddl_dir
    return None

# =============================================
# ODCS-ALIGNED NODE TYPES
# =============================================
# Maps dbt resource types → ODCS dataset types
RESOURCE_TYPE_MAP = {
    "model": "table",
    "seed": "table",
    "source": "source",
    "snapshot": "table",
    "test": "quality",
    "exposure": "application",
}

# Forge-injected asset types (not in dbt manifest)
FORGE_ASSET_TYPES = {
    "quarantine": "table",
    "prior_version": "table",
    "python_udf": "function",
    "sql_udf": "function",
    "dab_workflow": "workflow",
    "volume": "volume",
}


def _git_sha() -> str:
    """Current short git SHA, or 'unknown' if not in a repo."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
            timeout=5,
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        return "unknown"


def _content_hash(data: dict) -> str:
    """Deterministic hash of a dict for change detection."""
    raw = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


# =============================================
# GRAPH BUILDER – the core engine
# =============================================

def build_graph(
    forge_config: dict,
    manifest_path: Path | None = None,
    dbt_project_dir: Path | None = None,
) -> dict:
    """
    Build the full lineage graph from dbt manifest + forge config.

    Returns an ODCS-aligned graph dict with:
      - metadata (version, timestamps, provenance)
      - contracts (one per asset, ODCS-shaped)
      - edges (lineage relationships)

    If no manifest exists yet (pre-first-deploy), builds a
    stub graph from forge.yml features alone.
    """
    now = datetime.now(timezone.utc).isoformat()
    git_commit = _git_sha()
    project_name = forge_config.get("name", "unnamed")
    catalog = forge_config.get("catalog", "main")
    schema = forge_config.get("schema", "default")
    compute_type = forge_config.get("compute", {}).get("type", "serverless")
    features = forge_config.get("features", {})

    graph: dict[str, Any] = {
        "apiVersion": "odcs/v3.0.0",
        "kind": "DataContractGraph",
        "metadata": {
            "forge_version": "0.2.0",
            "project": project_name,
            "generated_at": now,
            "git_commit": git_commit,
            "compute_type": compute_type,
            "catalog": catalog,
            "schema": schema,
        },
        "contracts": {},
        "edges": [],
    }

    # ── Parse dbt manifest if it exists ──────────────────
    manifest = _load_manifest(manifest_path, dbt_project_dir)
    if manifest:
        _add_manifest_nodes(graph, manifest, catalog, schema, features, git_commit, now)
    else:
        # No manifest yet — scan SQL files for a best-effort graph
        _add_sql_scan_nodes(graph, dbt_project_dir or Path("dbt"), catalog, schema, features, git_commit, now)

    # ── Inject forge-specific assets ───────────────────
    if features.get("quarantine"):
        _add_quarantine_nodes(graph, git_commit, now)
    if features.get("prior_version"):
        _add_prior_version_nodes(graph, git_commit, now)
    if features.get("python_udfs"):
        _add_python_udf_nodes(graph, dbt_project_dir or Path("dbt"), catalog, schema, git_commit, now)

    # ── Inject SQL UDFs from dbt/ddl ──────────────────
    _add_sql_udf_nodes(graph, dbt_project_dir or Path("dbt"), forge_config, catalog, schema, git_commit, now)

    # ── Inject check nodes from dbt/ddl ───────────────
    _add_check_nodes(graph, dbt_project_dir or Path("dbt"), forge_config, git_commit, now)

    # ── Inject DAB workflow node ─────────────────────────
    project_id = forge_config.get("id", project_name)
    wf_name = f"PROCESS_{project_id}"
    wf_path = Path(f"resources/jobs/{wf_name}.yml")
    if wf_path.exists():
        graph["contracts"][f"workflow.{project_name}"] = _make_contract(
            dataset_name=project_name,
            dataset_domain="workflow",
            dataset_type="workflow",
            description=f"Databricks Asset Bundle workflow — {wf_path}",
            columns=[],
            quality_rules=[],
            catalog=catalog,
            schema=schema,
            git_commit=git_commit,
            generated_at=now,
            materialization="dab_workflow",
            tags=["dab", "workflow"],
            meta={"yaml_path": str(wf_path)},
        )
        # Edge from every model → workflow (workflow orchestrates all models)
        for cid in list(graph["contracts"]):
            if cid.startswith("model."):
                graph["edges"].append({"from": cid, "to": f"workflow.{project_name}", "type": "orchestrates"})

    # ── Inject custom task nodes ─────────────────────────
    _add_custom_task_nodes(graph, forge_config, catalog, schema, git_commit, now)

    # ── Inject volume nodes from DDL YAML ────────────────
    _add_volume_nodes(graph, dbt_project_dir or Path("dbt"), forge_config, catalog, schema, git_commit, now)

    # ── Mark python_managed models ───────────────────────
    _mark_python_managed_nodes(graph, dbt_project_dir or Path("dbt"), forge_config)

    # ── Compute content hashes for diffing ───────────────
    for contract_id, contract in graph["contracts"].items():
        contract["_content_hash"] = _content_hash(contract)

    return graph


# =============================================
# MANIFEST PARSER
# =============================================

def _load_manifest(
    manifest_path: Path | None,
    dbt_project_dir: Path | None,
) -> dict | None:
    """Load manifest.json from explicit path or default location."""
    candidates = []
    if manifest_path:
        candidates.append(manifest_path)
    if dbt_project_dir:
        candidates.append(dbt_project_dir / "target" / "manifest.json")
    candidates.append(Path("dbt") / "target" / "manifest.json")

    for path in candidates:
        if path.exists():
            return json.loads(path.read_text())
    return None


def _add_manifest_nodes(
    graph: dict,
    manifest: dict,
    catalog: str,
    schema: str,
    features: dict,
    git_commit: str,
    now: str,
) -> None:
    """Extract nodes and edges from dbt manifest.json."""
    nodes = manifest.get("nodes", {})
    sources = manifest.get("sources", {})
    parent_map = manifest.get("parent_map", {})
    child_map = manifest.get("child_map", {})

    # ── Add source nodes ─────────────────────────────────
    for source_id, source_data in sources.items():
        contract_id = source_id
        graph["contracts"][contract_id] = _make_contract(
            dataset_name=source_data.get("name", source_id),
            dataset_domain=source_data.get("schema", schema),
            dataset_type="source",
            description=source_data.get("description", ""),
            columns=_extract_columns(source_data),
            quality_rules=_extract_tests_for_node(source_id, nodes),
            catalog=source_data.get("database", catalog),
            schema=source_data.get("schema", schema),
            git_commit=git_commit,
            generated_at=now,
            materialization="external",
            tags=source_data.get("tags", []),
            meta=source_data.get("meta", {}),
        )

    # ── Add model / seed / snapshot nodes ────────────────
    for node_id, node_data in nodes.items():
        resource_type = node_data.get("resource_type", "")
        if resource_type not in RESOURCE_TYPE_MAP:
            continue
        if resource_type == "test":
            continue  # tests are attached to their parent

        materialization = node_data.get("config", {}).get("materialized", "view")
        contract_id = node_id

        graph["contracts"][contract_id] = _make_contract(
            dataset_name=node_data.get("name", node_id),
            dataset_domain=node_data.get("schema", schema),
            dataset_type=RESOURCE_TYPE_MAP[resource_type],
            description=node_data.get("description", ""),
            columns=_extract_columns(node_data),
            quality_rules=_extract_tests_for_node(node_id, nodes),
            catalog=node_data.get("database", catalog),
            schema=node_data.get("schema", schema),
            git_commit=git_commit,
            generated_at=now,
            materialization=materialization,
            tags=node_data.get("tags", []),
            meta=node_data.get("meta", {}),
            raw_sql=node_data.get("raw_code", ""),
        )

    # ── Add edges from parent_map ────────────────────────
    for node_id, parents in parent_map.items():
        for parent_id in parents:
            if parent_id in graph["contracts"] or node_id in graph["contracts"]:
                graph["edges"].append({
                    "from": parent_id,
                    "to": node_id,
                    "type": "ref",
                    "description": f"{_short_name(parent_id)} → {_short_name(node_id)}",
                })


def _extract_columns(node_data: dict) -> list[dict]:
    """Extract ODCS-shaped column definitions from a dbt node."""
    columns = []
    for col_name, col_data in node_data.get("columns", {}).items():
        columns.append({
            "name": col_name,
            "type": col_data.get("data_type", "string"),
            "description": col_data.get("description", ""),
            "isPrimaryKey": "primary_key" in col_data.get("tags", []),
            "isNullable": "not_null" not in [
                t.get("test_metadata", {}).get("name", "")
                for t in col_data.get("tests", [])
            ] if "tests" in col_data else True,
            "tags": col_data.get("tags", []),
        })
    return columns


def _extract_tests_for_node(node_id: str, all_nodes: dict) -> list[dict]:
    """Find dbt tests attached to a node and map them to ODCS quality rules."""
    rules = []
    for test_id, test_data in all_nodes.items():
        if test_data.get("resource_type") != "test":
            continue
        depends = test_data.get("depends_on", {}).get("nodes", [])
        if node_id not in depends:
            continue
        test_meta = test_data.get("test_metadata", {})
        rules.append({
            "type": test_meta.get("name", "custom"),
            "description": test_data.get("name", test_id),
            "column": test_meta.get("kwargs", {}).get("column_name", ""),
            "dimension": _test_to_dimension(test_meta.get("name", "")),
        })
    return rules


def _test_to_dimension(test_name: str) -> str:
    """Map dbt test names to ODCS data quality dimensions."""
    mapping = {
        "not_null": "completeness",
        "unique": "uniqueness",
        "accepted_values": "validity",
        "relationships": "consistency",
    }
    return mapping.get(test_name, "accuracy")


# =============================================
# SQL-SCAN FALLBACK (no manifest yet)
# =============================================

def _add_sql_scan_nodes(
    graph: dict,
    dbt_dir: Path,
    catalog: str,
    schema: str,
    features: dict,
    git_commit: str,
    now: str,
) -> None:
    """
    When no manifest.json exists (pre-first-compile),
    scan .sql files for {{ ref('...') }} and {{ source('...', '...') }}
    to build a best-effort graph.
    """
    import re
    ref_pattern = re.compile(r"\{\{\s*ref\(\s*['\"](\w+)['\"]\s*\)\s*\}\}")
    source_pattern = re.compile(r"\{\{\s*source\(\s*['\"](\w+)['\"]\s*,\s*['\"](\w+)['\"]\s*\)\s*\}\}")
    quarantine_pattern = re.compile(r"\{\{\s*quarantine\(")
    udf_pattern = re.compile(r"\{\{\s*python_udf\(\s*['\"](\w+)['\"]\s*\)\s*\}\}")
    broadcast_pattern = re.compile(r"/\*\+\s*BROADCAST\(([^)]+)\)\s*\*/")

    models_dir = dbt_dir / "models"
    if not models_dir.exists():
        return

    for sql_file in models_dir.rglob("*.sql"):
        model_name = sql_file.stem
        sql_content = sql_file.read_text()
        contract_id = f"model.{graph['metadata']['project']}.{model_name}"

        # Detect refs and sources
        refs = ref_pattern.findall(sql_content)
        sources_found = source_pattern.findall(sql_content)
        has_quarantine = bool(quarantine_pattern.search(sql_content))
        udfs_used = udf_pattern.findall(sql_content)
        broadcast_match = broadcast_pattern.search(sql_content)
        broadcast_aliases = [a.strip() for a in broadcast_match.group(1).split(",")] if broadcast_match else []

        graph["contracts"][contract_id] = _make_contract(
            dataset_name=model_name,
            dataset_domain=schema,
            dataset_type="table",
            description=f"Model from {sql_file.relative_to(dbt_dir)}",
            columns=[],
            quality_rules=[],
            catalog=catalog,
            schema=schema,
            git_commit=git_commit,
            generated_at=now,
            materialization="view",
            tags=[],
            meta={"sql_file": str(sql_file.relative_to(dbt_dir)),
                  **({"broadcast": broadcast_aliases} if broadcast_aliases else {})},
            raw_sql=sql_content,
            uses_quarantine=has_quarantine,
            udfs_used=udfs_used,
        )

        # Add ref edges
        for ref_name in refs:
            parent_id = f"model.{graph['metadata']['project']}.{ref_name}"
            graph["edges"].append({
                "from": parent_id,
                "to": contract_id,
                "type": "ref",
                "description": f"{ref_name} → {model_name}",
            })

        # Add source edges
        for source_name, table_name in sources_found:
            source_id = f"source.{graph['metadata']['project']}.{source_name}.{table_name}"
            if source_id not in graph["contracts"]:
                graph["contracts"][source_id] = _make_contract(
                    dataset_name=table_name,
                    dataset_domain=schema,
                    dataset_type="source",
                    description=f"Source {source_name}.{table_name}",
                    columns=[],
                    quality_rules=[],
                    catalog=catalog,
                    schema=schema,
                    git_commit=git_commit,
                    generated_at=now,
                    materialization="external",
                    tags=[],
                    meta={},
                )
            graph["edges"].append({
                "from": source_id,
                "to": contract_id,
                "type": "source",
                "description": f"{source_name}.{table_name} → {model_name}",
            })


# =============================================
# FORGE-SPECIFIC NODE INJECTION
# =============================================

def _add_quarantine_nodes(graph: dict, git_commit: str, now: str) -> None:
    """For every model, add a shadow quarantine table node + edge."""
    model_ids = [
        cid for cid, c in graph["contracts"].items()
        if c["dataset"]["type"] == "table" and not cid.endswith("_quarantine")
    ]
    for model_id in model_ids:
        contract = graph["contracts"][model_id]
        q_id = f"{model_id}_quarantine"
        uses_quarantine = contract.get("provenance", {}).get("uses_quarantine", False)

        # Only add if the model actually uses the quarantine macro,
        # or if we're in scan mode and detected the macro call
        if not uses_quarantine and not contract.get("_meta", {}).get("uses_quarantine", False):
            continue

        graph["contracts"][q_id] = _make_contract(
            dataset_name=f"{contract['dataset']['name']}_quarantine",
            dataset_domain=contract["dataset"]["domain"],
            dataset_type="table",
            description=f"Quarantined rows from {contract['dataset']['name']}",
            columns=contract["dataset"].get("columns", []),
            quality_rules=[],
            catalog=contract["dataset"].get("catalog", "main"),
            schema=contract["dataset"].get("schema", "default"),
            git_commit=git_commit,
            generated_at=now,
            materialization="table",
            tags=["quarantine", "auto-generated"],
            meta={"parent_model": model_id},
        )
        graph["edges"].append({
            "from": model_id,
            "to": q_id,
            "type": "quarantine",
            "description": f"{contract['dataset']['name']} → quarantine",
        })


def _add_prior_version_nodes(graph: dict, git_commit: str, now: str) -> None:
    """For every model, add a prior_version shadow table node + edge."""
    model_ids = [
        cid for cid, c in graph["contracts"].items()
        if c["dataset"]["type"] == "table"
        and "_quarantine" not in cid
        and "_v_previous" not in cid
    ]
    for model_id in model_ids:
        contract = graph["contracts"][model_id]
        pv_id = f"{model_id}_v_previous"
        graph["contracts"][pv_id] = _make_contract(
            dataset_name=f"{contract['dataset']['name']}_v_previous",
            dataset_domain=contract["dataset"]["domain"],
            dataset_type="table",
            description=f"Prior deployment version of {contract['dataset']['name']}",
            columns=contract["dataset"].get("columns", []),
            quality_rules=[],
            catalog=contract["dataset"].get("catalog", "main"),
            schema=contract["dataset"].get("schema", "default"),
            git_commit=git_commit,
            generated_at=now,
            materialization="table",
            tags=["prior_version", "auto-generated"],
            meta={"parent_model": model_id},
        )
        graph["edges"].append({
            "from": model_id,
            "to": pv_id,
            "type": "prior_version",
            "description": f"{contract['dataset']['name']} → prior version snapshot",
        })


def _add_python_udf_nodes(
    graph: dict,
    dbt_dir: Path,
    catalog: str,
    schema: str,
    git_commit: str,
    now: str,
) -> None:
    """Scan models for {{ python_udf('name') }} calls and add UDF nodes."""
    import re
    udf_pattern = re.compile(r"\{\{\s*python_udf\(\s*['\"](\w+)['\"]\s*\)\s*\}\}")
    models_dir = dbt_dir / "models"
    if not models_dir.exists():
        return

    udf_names_seen: set[str] = set()
    for sql_file in models_dir.rglob("*.sql"):
        model_name = sql_file.stem
        sql_content = sql_file.read_text()
        for udf_name in udf_pattern.findall(sql_content):
            udf_id = f"udf.{graph['metadata']['project']}.{udf_name}"
            model_id = f"model.{graph['metadata']['project']}.{model_name}"

            if udf_name not in udf_names_seen:
                udf_names_seen.add(udf_name)
                graph["contracts"][udf_id] = _make_contract(
                    dataset_name=udf_name,
                    dataset_domain=schema,
                    dataset_type="function",
                    description=f"Python UDF: {udf_name}",
                    columns=[],
                    quality_rules=[],
                    catalog=catalog,
                    schema=schema,
                    git_commit=git_commit,
                    generated_at=now,
                    materialization="function",
                    tags=["python_udf"],
                    meta={},
                )

            graph["edges"].append({
                "from": udf_id,
                "to": model_id,
                "type": "udf_call",
                "description": f"UDF {udf_name} used in {model_name}",
            })


def _add_sql_udf_nodes(
    graph: dict,
    dbt_dir: Path,
    forge_config: dict,
    catalog: str,
    schema: str,
    git_commit: str,
    now: str,
) -> None:
    """
    Read udfs: blocks from dbt/ddl and add UDF nodes to the graph.

    Also scans model columns for udf: references and creates edges.
    Purple UDF nodes in Mermaid.
    """
    ddl_path = _resolve_ddl_path(dbt_dir)
    if not ddl_path:
        return

    raw = _load_raw_ddl(ddl_path, forge_config=forge_config)
    udfs = raw.get("udfs", {})
    models = raw.get("models", {})

    if not udfs:
        return

    project = graph["metadata"]["project"]

    # Add UDF nodes
    for udf_name, udf_def in udfs.items():
        udf_id = f"udf.{project}.{udf_name}"
        raw_language = udf_def.get("language", "sql").upper()
        is_pandas = raw_language == "PANDAS"
        language = "PYTHON" if is_pandas else raw_language
        params = udf_def.get("params", [])
        returns = udf_def.get("returns", "STRING")

        param_desc = ", ".join(
            f"{p['name']}: {p['type']}" if isinstance(p, dict) else str(p)
            for p in params
        )

        udf_kind = "pandas_udf" if is_pandas else ("python_udf" if language == "PYTHON" else "sql_udf")
        desc_prefix = "Pandas UDF" if is_pandas else (f"{language} UDF")

        graph["contracts"][udf_id] = _make_contract(
            dataset_name=udf_name,
            dataset_domain=schema,
            dataset_type="function",
            description=udf_def.get("description", f"{desc_prefix}: {udf_name}({param_desc}) → {returns}"),
            columns=[],
            quality_rules=[],
            catalog=catalog,
            schema=schema,
            git_commit=git_commit,
            generated_at=now,
            materialization="function",
            tags=[udf_kind, f"lang:{language.lower()}"],
            meta={"language": language, "returns": returns, "params": param_desc,
                  "vectorized": is_pandas},
        )

    # Scan models for udf: column references → edges
    for model_name, model_def in models.items():
        model_id = f"model.{project}.{model_name}"
        for col_name, col_def in model_def.get("columns", {}).items():
            if isinstance(col_def, dict) and "udf" in col_def:
                # Extract UDF name from call expression e.g. "loyalty_tier(total_revenue)"
                udf_call = col_def["udf"]
                # Parse function name (everything before the first parenthesis)
                fn_name = udf_call.split("(")[0].strip()
                udf_id = f"udf.{project}.{fn_name}"

                # If not a locally-defined UDF, create an external node
                if fn_name not in udfs and udf_id not in graph["contracts"]:
                    graph["contracts"][udf_id] = _make_contract(
                        dataset_name=fn_name,
                        dataset_domain=schema,
                        dataset_type="function",
                        description=f"External UDF: {fn_name}",
                        columns=[],
                        quality_rules=[],
                        catalog=catalog,
                        schema=schema,
                        git_commit=git_commit,
                        generated_at=now,
                        materialization="function",
                        tags=["external_udf"],
                        meta={"external": True},
                    )

                # Parse input columns from call args
                paren_start = udf_call.find("(")
                paren_end = udf_call.rfind(")")
                udf_inputs = []
                if paren_start != -1 and paren_end != -1:
                    args_str = udf_call[paren_start + 1 : paren_end]
                    udf_inputs = [a.strip() for a in args_str.split(",") if a.strip()]

                graph["edges"].append({
                    "from": udf_id,
                    "to": model_id,
                    "type": "udf_call",
                    "description": f"UDF {fn_name}({', '.join(udf_inputs)}) → {model_name}.{col_name}",
                    **({"meta": {"inputs": udf_inputs, "output": col_name}} if udf_inputs else {}),
                })


def _add_check_nodes(
    graph: dict,
    dbt_dir: Path,
    forge_config: dict,
    git_commit: str,
    now: str,
) -> None:
    """
    Read checks: blocks from dbt/ddl and add check nodes + edges.

    Orange hexagons in Mermaid. Each check is a node connected to its model.
    Cross-model checks (reconcile) also connect to the parent model.
    """
    ddl_path = _resolve_ddl_path(dbt_dir)
    if not ddl_path:
        return

    raw = _load_raw_ddl(ddl_path, forge_config=forge_config)
    models = raw.get("models", {})
    project = graph["metadata"]["project"]
    schema = graph["metadata"]["schema"]

    for model_name, model_def in models.items():
        checks = model_def.get("checks", [])
        if not checks:
            continue

        model_id = f"model.{project}.{model_name}"

        for check in checks:
            check_name = check.get("name", f"{model_name}_check")
            check_type = check.get("type", "custom_sql")
            severity = check.get("severity", "warn")
            check_id = f"check.{project}.{model_name}.{check_name}"

            graph["contracts"][check_id] = _make_contract(
                dataset_name=check_name,
                dataset_domain=schema,
                dataset_type="quality",
                description=f"Check: {check_name} ({check_type}, {severity})",
                columns=[],
                quality_rules=[{
                    "type": check_type,
                    "description": check_name,
                    "column": check.get("column", ""),
                    "dimension": "accuracy",
                }],
                catalog=graph["metadata"]["catalog"],
                schema=schema,
                git_commit=git_commit,
                generated_at=now,
                materialization="check",
                tags=["check", f"type:{check_type}", f"severity:{severity}"],
                meta={
                    "check_type": check_type,
                    "severity": severity,
                    "column": check.get("column"),
                    "model": model_name,
                },
            )

            # Edge: model → check (model is validated by this check)
            graph["edges"].append({
                "from": model_id,
                "to": check_id,
                "type": "check",
                "description": f"{model_name} validated by {check_name}",
            })

            # Reconcile checks: also link to parent model
            if check_type == "reconcile":
                parent_model = check.get("parent_model", "")
                if parent_model:
                    parent_id = f"model.{project}.{parent_model}"
                    graph["edges"].append({
                        "from": parent_id,
                        "to": check_id,
                        "type": "check",
                        "description": f"{parent_model} reconciled in {check_name}",
                    })


# =============================================
# CUSTOM TASK NODES
# =============================================

def _add_custom_task_nodes(
    graph: dict,
    forge_config: dict,
    catalog: str,
    schema: str,
    git_commit: str,
    now: str,
) -> None:
    """
    Read custom_tasks from forge.yml and inject into the graph.

    Each custom task becomes a node (sql_task, python_task)
    with edges from its depends_on targets.
    """
    custom_tasks = forge_config.get("custom_tasks", [])
    if not custom_tasks:
        return

    project = graph["metadata"]["project"]

    for ct in custom_tasks:
        task_key = ct["task_key"]
        task_id = f"custom_task.{project}.{task_key}"

        # Determine task type
        if "python_task" in ct:
            task_type = "python"
            file_ref = ct["python_task"]
        elif "sql_task" in ct:
            task_type = "sql"
            file_ref = ct["sql_task"]
        elif "notebook_task" in ct:
            raise ValueError(
                f"custom_task '{task_key}' uses notebook_task, but notebook task support has been removed. "
                "Replace it with a python_task or sql_task."
            )
        else:
            task_type = "unknown"
            file_ref = ""

        graph["contracts"][task_id] = _make_contract(
            dataset_name=task_key,
            dataset_domain=schema,
            dataset_type="workflow_task",
            description=f"Custom {task_type} task: {task_key} → {file_ref}",
            columns=[],
            quality_rules=[],
            catalog=catalog,
            schema=schema,
            git_commit=git_commit,
            generated_at=now,
            materialization=f"custom_{task_type}_task",
            tags=["custom_task", f"type:{task_type}"],
            meta={
                "task_key": task_key,
                "task_type": task_type,
                "file": file_ref,
                "stage": ct.get("stage", "enrich"),
            },
        )

        # Add edges from dependencies
        for dep in ct.get("depends_on", []):
            dep_id = _resolve_custom_task_dep(graph, project, dep)
            if dep_id:
                graph["edges"].append({
                    "from": dep_id,
                    "to": task_id,
                    "type": "depends_on",
                    "description": f"{task_key} depends on {dep}",
                })


def _resolve_custom_task_dep(graph: dict, project: str, dep_name: str) -> str | None:
    """Resolve a dependency name to its contract ID in the graph."""
    # Try exact match first
    if dep_name in graph["contracts"]:
        return dep_name

    # Try common prefixes: model, custom_task, check
    for prefix in [
        f"model.{project}.{dep_name}",
        f"model.dbt_dab_wrapper.{dep_name}",
        f"custom_task.{project}.{dep_name}",
    ]:
        if prefix in graph["contracts"]:
            return prefix

    # Fallback: scan all contracts for a matching dataset name
    for cid, contract in graph["contracts"].items():
        if contract.get("dataset", {}).get("name") == dep_name:
            return cid

    return None


# =============================================
# VOLUME NODES
# =============================================

def _add_volume_nodes(
    graph: dict,
    dbt_dir: Path,
    forge_config: dict,
    catalog: str,
    schema: str,
    git_commit: str,
    now: str,
) -> None:
    """Inject volume declarations from DDL YAML into the graph."""
    ddl_path = _resolve_ddl_path(dbt_dir)
    if not ddl_path:
        return

    from forge.simple_ddl import load_volumes
    volumes = load_volumes(ddl_path, forge_config=forge_config)
    if not volumes:
        return

    project = graph["metadata"]["project"]

    for vol_name, vol_def in volumes.items():
        vol_id = f"volume.{project}.{vol_name}"
        vol_type = vol_def.get("type", "managed")
        location = vol_def.get("location", "")

        graph["contracts"][vol_id] = _make_contract(
            dataset_name=vol_name,
            dataset_domain=schema,
            dataset_type="volume",
            description=vol_def.get("description", f"Volume: {vol_name}"),
            columns=[],
            quality_rules=[],
            catalog=catalog,
            schema=schema,
            git_commit=git_commit,
            generated_at=now,
            materialization=f"{vol_type}_volume",
            tags=["volume", vol_type],
            meta={"location": location, "volume_type": vol_type},
        )


def _mark_python_managed_nodes(
    graph: dict,
    dbt_dir: Path,
    forge_config: dict,
) -> None:
    """Tag existing model contracts that have managed_by set in DDL."""
    ddl_path = _resolve_ddl_path(dbt_dir)
    if not ddl_path:
        return

    from forge.simple_ddl import load_ddl
    models = load_ddl(ddl_path, forge_config=forge_config)
    project = graph["metadata"]["project"]

    for model_name, model_def in models.items():
        managed_by = model_def.get("managed_by")
        domain_flag = model_def.get("domain")
        if not managed_by and domain_flag is None:
            continue
        # Find matching contract and tag it
        for prefix in [f"model.{project}.{model_name}", f"model.dbt_dab_wrapper.{model_name}"]:
            if prefix in graph["contracts"]:
                meta = graph["contracts"][prefix].setdefault("_meta", {})
                if managed_by:
                    graph["contracts"][prefix].setdefault("tags", []).append("python_managed")
                    meta["managed_by"] = managed_by
                if domain_flag is not None:
                    meta["domain"] = domain_flag
                    if not domain_flag:
                        graph["contracts"][prefix].setdefault("tags", []).append("shared")
                break


# =============================================
# ODCS CONTRACT FACTORY
# =============================================

def _make_contract(
    *,
    dataset_name: str,
    dataset_domain: str,
    dataset_type: str,
    description: str,
    columns: list[dict],
    quality_rules: list[dict],
    catalog: str,
    schema: str,
    git_commit: str,
    generated_at: str,
    materialization: str,
    tags: list[str],
    meta: dict | None = None,
    raw_sql: str = "",
    uses_quarantine: bool = False,
    udfs_used: list[str] | None = None,
) -> dict:
    """
    Produce an ODCS v3.0.0-aligned data contract for a single asset.

    Follows: https://bitol.io/open-data-contract-standard/
    Fields map directly to the ODCS spec where possible.
    Forge-specific extensions live under `_meta`.
    """
    contract: dict[str, Any] = {
        "apiVersion": "odcs/v3.0.0",
        "kind": "DataContract",
        "dataset": {
            "name": dataset_name,
            "domain": dataset_domain,
            "type": dataset_type,
            "description": description,
            "catalog": catalog,
            "schema": schema,
            "columns": columns,
        },
        "quality": {
            "rules": quality_rules,
            "dimension_summary": _summarize_dimensions(quality_rules),
        },
        "lineage": {
            "materialization": materialization,
            "struct_column": "_lineage",
            "schema_version": "3",
        },
        "provenance": {
            "git_commit": git_commit,
            "generated_at": generated_at,
            "uses_quarantine": uses_quarantine,
        },
        "tags": tags,
        "_meta": meta or {},
    }

    if raw_sql:
        contract["_meta"]["sql_hash"] = hashlib.sha256(raw_sql.encode()).hexdigest()[:12]

    if udfs_used:
        contract["_meta"]["udfs_used"] = udfs_used
        contract["_meta"]["uses_quarantine"] = uses_quarantine  # forward for quarantine injection

    return contract


def _summarize_dimensions(rules: list[dict]) -> dict[str, int]:
    """Count quality rules by dimension."""
    summary: dict[str, int] = {}
    for rule in rules:
        dim = rule.get("dimension", "other")
        summary[dim] = summary.get(dim, 0) + 1
    return summary


def _short_name(node_id: str) -> str:
    """model.project.customers → customers"""
    parts = node_id.split(".")
    return parts[-1] if parts else node_id


# =============================================
# COLUMN-LEVEL LINEAGE WALKER
# =============================================

def walk_column_lineage(
    graph: dict,
    model_name: str,
    column_name: str,
    dbt_dir: Path = Path("dbt"),
    forge_config: dict | None = None,
) -> dict:
    """
    Walk backwards from a column to its sources: expressions, UDFs, checks, git, version.

    Returns a provenance tree dict:
      {
        model, column, expression, op, type, version,
        source_model, source_columns,
        udf_calls: [{name, description, language, returns}],
        checks: [{name, type, severity, column, sql}],
        git_commit, generated_at,
        upstream: [recursive tree nodes]
      }
    """
    ddl_path = _resolve_ddl_path(dbt_dir)
    if not ddl_path:
        return {"error": f"No canonical dbt/ddl directory found in {dbt_dir}"}

    raw = _load_raw_ddl(ddl_path, forge_config=forge_config)
    models = raw.get("models", {})
    udfs = raw.get("udfs", {})

    if model_name not in models:
        return {"error": f"Model '{model_name}' not found in dbt/ddl"}

    model_def = models[model_name]
    columns = model_def.get("columns", {})

    if column_name not in columns:
        return {"error": f"Column '{column_name}' not found in model '{model_name}'"}

    col_def = columns[column_name]
    if not isinstance(col_def, dict):
        col_def = {}

    # ── Determine expression and operation ───────
    expression = ""
    op = "PASSTHROUGH"
    source_columns: list[str] = []
    udf_calls: list[dict] = []

    if "udf" in col_def:
        udf_call_str = col_def["udf"]
        fn_name = udf_call_str.split("(")[0].strip()
        expression = udf_call_str
        # Extract arg columns from the call
        import re
        args = re.findall(r"\(([^)]*)\)", udf_call_str)
        if args:
            source_columns = [a.strip() for a in args[0].split(",") if a.strip()]
        if fn_name in udfs:
            udf_def = udfs[fn_name]
            raw_lang = udf_def.get("language", "sql").upper()
            is_pandas = raw_lang == "PANDAS"
            language = "PYTHON" if is_pandas else raw_lang
            op = "PANDAS_UDF" if is_pandas else ("PYTHON_UDF" if language == "PYTHON" else "SQL_UDF")
            udf_entry: dict[str, Any] = {
                "name": fn_name,
                "description": udf_def.get("description", ""),
                "language": language,
                "returns": udf_def.get("returns", "STRING"),
                "vectorized": is_pandas,
            }
            if udf_def.get("runtime_version"):
                udf_entry["runtime_version"] = udf_def["runtime_version"]
            if udf_def.get("handler"):
                udf_entry["handler"] = udf_def["handler"]
            if udf_def.get("packages"):
                udf_entry["packages"] = list(udf_def["packages"])
            udf_calls.append(udf_entry)
        else:
            # External UDF — not defined locally
            op = "EXTERNAL_UDF"
            udf_calls.append({
                "name": fn_name,
                "description": "External UDF (not defined in this project)",
                "language": "UNKNOWN",
                "returns": col_def.get("type", "UNKNOWN"),
                "external": True,
            })
    elif "expr" in col_def:
        expression = col_def["expr"]
        op = "EXPRESSION"
        # Try to extract column references from the expression
        import re
        source_columns = re.findall(r"\b([a-z_][a-z0-9_]*)\b", expression.lower())
        # Filter out SQL keywords
        sql_keywords = {"count", "sum", "min", "max", "avg", "case", "when", "then",
                        "else", "end", "as", "and", "or", "not", "null", "is", "in",
                        "between", "like", "cast", "coalesce", "if", "select", "from"}
        source_columns = [c for c in source_columns if c not in sql_keywords]
    elif "from" in col_def:
        op = "JOIN_REF"
        expression = f"{col_def['from']}.{col_def.get('source_column', column_name)}"
        source_columns = [col_def.get("source_column", column_name)]
    elif "cast" in col_def and col_def.get("cast"):
        op = "CAST"
        expression = f"CAST({column_name} AS {col_def.get('type', 'string')})"
        source_columns = [column_name]
    else:
        expression = column_name
        source_columns = [column_name]

    # ── Source model(s) ──────────────────────────
    source_model = None
    if "source" in model_def:
        source_model = model_def["source"]
    elif "sources" in model_def:
        # For joins, find which alias this column comes from
        alias = col_def.get("from")
        if alias and alias in model_def["sources"]:
            source_model = model_def["sources"][alias]
        else:
            # First source as default
            source_model = next(iter(model_def["sources"].values()), None)

    # ── Checks on this column or model ───────────
    checks: list[dict] = []
    for chk in model_def.get("checks", []):
        chk_col = chk.get("column")
        # Include: column-specific checks for this column, or table-scope checks
        if chk_col == column_name or chk_col is None:
            checks.append({
                "name": chk.get("name", "unnamed"),
                "type": chk.get("type", "custom_sql"),
                "severity": chk.get("severity", "warn"),
                "column": chk_col,
            })

    # ── Git + metadata from graph ────────────────
    git_commit = graph.get("metadata", {}).get("git_commit", "unknown")
    generated_at = graph.get("metadata", {}).get("generated_at", "")
    version = model_def.get("version", "v1")

    # ── Origin tracking ──────────────────────────
    origin = model_def.get("origin")

    # ── Build the provenance node ────────────────
    node: dict[str, Any] = {
        "model": model_name,
        "column": column_name,
        "expression": expression,
        "op": op,
        "type": col_def.get("type", "inferred"),
        "version": version,
        "source_model": source_model,
        "source_columns": source_columns,
        "udf_calls": udf_calls,
        "checks": checks,
        "git_commit": git_commit,
        "generated_at": generated_at,
    }
    if origin:
        node["origin"] = origin

    # ── Recurse upstream ─────────────────────────
    upstream: list[dict] = []
    seeds = raw.get("seeds", {})
    if source_model and source_model in models:
        source_cols = models[source_model].get("columns", {})
        for src_col in source_columns:
            if src_col in source_cols:
                upstream.append(
                    walk_column_lineage(graph, source_model, src_col, dbt_dir, forge_config=forge_config)
                )
    elif source_model and source_model in seeds:
        # Terminal node: data originates from a seed
        seed_def = seeds[source_model]
        seed_origin = seed_def.get("origin", {"type": "seed", "path": f"dbt/seeds/{source_model}.csv"})
        for src_col in source_columns:
            upstream.append({
                "model": source_model,
                "column": src_col,
                "expression": src_col,
                "op": "SEED",
                "type": seed_def.get("columns", {}).get(src_col, {}).get("type", "inferred") if isinstance(seed_def.get("columns", {}).get(src_col), dict) else "inferred",
                "version": "v1",
                "source_model": None,
                "source_columns": [],
                "udf_calls": [],
                "checks": [],
                "git_commit": git_commit,
                "generated_at": generated_at,
                "origin": seed_origin,
                "upstream": [],
            })
    node["upstream"] = upstream

    return node


def render_provenance_tree(
    tree: dict,
    mermaid: bool = False,
    full: bool = False,
    light: bool = False,
    show_origin: bool = False,
) -> str:
    """
    Render a provenance tree as terminal text or Mermaid diagram.

    full:        Include UDF runtime details, packages, handler
    light:       Compact view — expressions and columns only
    show_origin: Include data origin / source provenance
    """
    if "error" in tree:
        return f"❌ {tree['error']}"

    if mermaid:
        return _render_provenance_mermaid(tree)
    if light:
        return _render_provenance_light(tree)
    return _render_provenance_terminal(tree, full=full, show_origin=show_origin)


def _render_provenance_terminal(
    tree: dict,
    depth: int = 0,
    full: bool = False,
    show_origin: bool = False,
) -> str:
    """Pretty-print provenance tree for terminal output."""
    lines: list[str] = []
    indent = "  " * depth
    prefix = "├─" if depth > 0 else "🔎"

    if depth == 0:
        lines.append(f"🔎 Explaining {tree['model']}.{tree['column']}")
        lines.append(f"├─ Expression: {tree['expression']}")
        lines.append(f"├─ Operation: {tree['op']}")
        if tree.get("type") and tree["type"] != "inferred":
            lines.append(f"├─ Type: {tree['type']}")
        lines.append(f"├─ Version: {tree['version']}")
        if tree.get("source_model"):
            lines.append(f"├─ From model: {tree['source_model']}")
        if tree.get("source_columns"):
            lines.append(f"├─ Source columns: {', '.join(tree['source_columns'])}")

        # UDFs
        for udf in tree.get("udf_calls", []):
            icon = "🟠" if udf.get("vectorized") else ("🟣" if udf["language"] == "PYTHON" else ("🔵" if udf["language"] in ("SQL", "UNKNOWN") else "⚪"))
            ext = " (external)" if udf.get("external") else ""
            lines.append(f"├─ {icon} UDF: {udf['name']}() → {udf['returns']}  [{udf['language']}]{ext}")
            if full and udf.get("description"):
                lines.append(f"│    {udf['description']}")
            if full and udf.get("runtime_version"):
                lines.append(f"│    Runtime: Python {udf['runtime_version']}")
            if full and udf.get("packages"):
                lines.append(f"│    Packages: {', '.join(udf['packages'])}")
            if full and udf.get("handler"):
                lines.append(f"│    Handler: {udf['handler']}")

        # Git + deploy
        lines.append(f"├─ Git commit: {tree['git_commit']}")
        if tree.get("generated_at"):
            lines.append(f"├─ Generated: {tree['generated_at']}")

        # Origin
        if show_origin and tree.get("origin"):
            o = tree["origin"]
            lines.append(f"├─ 📦 Origin: {o.get('type', 'unknown')}")
            if o.get("path"):
                lines.append(f"│    Path: {o['path']}")
            if o.get("endpoint"):
                lines.append(f"│    Endpoint: {o['endpoint']}")
            if o.get("format"):
                lines.append(f"│    Format: {o['format']}")

        # Checks
        checks = tree.get("checks", [])
        if checks:
            lines.append(f"└─ Checks ({len(checks)}):")
            for chk in checks:
                icon = "🔴" if chk["severity"] == "error" else "🟡" if chk["severity"] == "warn" else "⚪"
                scope = f" on {chk['column']}" if chk.get("column") else " (table)"
                lines.append(f"     {icon} {chk['name']} [{chk['type']}]{scope} → {chk['severity']}")
        else:
            lines.append(f"└─ Checks: (none)")
    else:
        lines.append(f"{indent}{prefix} {tree['model']}.{tree['column']}")
        lines.append(f"{indent}│  expr: {tree['expression']}  [{tree['op']}]")
        if tree.get("udf_calls"):
            for udf in tree["udf_calls"]:
                icon = "🟠" if udf.get("vectorized") else ("🟣" if udf.get("language") == "PYTHON" else "🔵")
                ext = " (ext)" if udf.get("external") else ""
                lines.append(f"{indent}│  {icon} {udf['name']}() [{udf.get('language', 'SQL')}]{ext}")
        if tree.get("checks") and full:
            for chk in tree["checks"]:
                icon = "🔴" if chk["severity"] == "error" else "🟡"
                lines.append(f"{indent}│  {icon} {chk['name']}")
        if show_origin and tree.get("origin"):
            o = tree["origin"]
            lines.append(f"{indent}│  📦 {o.get('type', 'unknown')}: {o.get('path') or o.get('endpoint', '')}")

    # Upstream
    for child in tree.get("upstream", []):
        if "error" not in child:
            lines.append("")
            lines.append(f"{indent}  ⬆ Upstream: {child['model']}.{child['column']}")
            lines.append(_render_provenance_terminal(child, depth + 1, full=full, show_origin=show_origin))

    return "\n".join(lines)


def _render_provenance_light(tree: dict, depth: int = 0) -> str:
    """Compact provenance view — just expressions and column flow."""
    lines: list[str] = []
    indent = "  " * depth

    if depth == 0:
        lines.append(f"{tree['model']}.{tree['column']}  ←  {tree['expression']}  [{tree['op']}]")
    else:
        lines.append(f"{indent}← {tree['model']}.{tree['column']}  ←  {tree['expression']}  [{tree['op']}]")

    if tree.get("origin"):
        o = tree["origin"]
        lines.append(f"{indent}   📦 {o.get('type', '?')}: {o.get('path') or o.get('endpoint', '')}")

    for child in tree.get("upstream", []):
        if "error" not in child:
            lines.append(_render_provenance_light(child, depth + 1))

    return "\n".join(lines)


def _render_provenance_mermaid(tree: dict) -> str:
    """Render provenance tree as a Mermaid flowchart."""
    lines = ["graph BT"]
    node_ids: set[str] = set()

    def _add_nodes(t: dict, parent_id: str | None = None) -> None:
        node_id = f"{t['model']}_{t['column']}".replace(".", "_")
        if node_id not in node_ids:
            node_ids.add(node_id)
            label = f"{t['model']}.{t['column']}"
            if t["op"] == "UDF":
                lines.append(f"    {node_id}{{{{{label}}}}}")
            else:
                lines.append(f'    {node_id}["{label}<br/>{t["expression"]}"]')

            # Check nodes
            for chk in t.get("checks", []):
                chk_id = f"chk_{t['model']}_{chk['name']}".replace(".", "_")
                if chk_id not in node_ids:
                    node_ids.add(chk_id)
                    sev_icon = "🔴" if chk["severity"] == "error" else "🟡"
                    lines.append(f'    {chk_id}{{"{sev_icon} {chk["name"]}"}}')
                    lines.append(f"    {node_id} -.->|check| {chk_id}")

            # UDF nodes
            for udf in t.get("udf_calls", []):
                udf_id = f"udf_{udf['name']}"
                if udf_id not in node_ids:
                    node_ids.add(udf_id)
                    lines.append(f"    {udf_id}{{{{{udf['name']}()}}}}")
                lines.append(f"    {udf_id} ==>|udf| {node_id}")

        if parent_id:
            lines.append(f"    {node_id} --> {parent_id}")

        for child in t.get("upstream", []):
            if "error" not in child:
                _add_nodes(child, node_id)

    _add_nodes(tree)

    return "\n".join(lines)


# =============================================
# GRAPH DIFF ENGINE
# =============================================

def diff_graphs(old_graph: dict, new_graph: dict) -> dict:
    """
    Compare two graph snapshots and produce a structured diff.

    Returns:
      {
        "added":    { contract_id: contract },
        "removed":  { contract_id: contract },
        "modified": { contract_id: { "old": contract, "new": contract, "changes": [...] } },
        "unchanged": [ contract_id, ... ],
        "edges_added":   [ edge, ... ],
        "edges_removed": [ edge, ... ],
        "summary": "plain English description"
      }
    """
    old_contracts = old_graph.get("contracts", {})
    new_contracts = new_graph.get("contracts", {})

    old_ids = set(old_contracts.keys())
    new_ids = set(new_contracts.keys())

    added_ids = new_ids - old_ids
    removed_ids = old_ids - new_ids
    common_ids = old_ids & new_ids

    added = {cid: new_contracts[cid] for cid in added_ids}
    removed = {cid: old_contracts[cid] for cid in removed_ids}
    modified = {}
    unchanged = []

    for cid in common_ids:
        old_hash = old_contracts[cid].get("_content_hash", "")
        new_hash = new_contracts[cid].get("_content_hash", "")
        if old_hash != new_hash:
            changes = _detect_changes(old_contracts[cid], new_contracts[cid])
            modified[cid] = {
                "old": old_contracts[cid],
                "new": new_contracts[cid],
                "changes": changes,
            }
        else:
            unchanged.append(cid)

    # Edge diff
    old_edge_set = {_edge_key(e) for e in old_graph.get("edges", [])}
    new_edge_set = {_edge_key(e) for e in new_graph.get("edges", [])}
    edges_added = [e for e in new_graph.get("edges", []) if _edge_key(e) not in old_edge_set]
    edges_removed = [e for e in old_graph.get("edges", []) if _edge_key(e) not in new_edge_set]

    summary = _build_summary(added, removed, modified, edges_added, edges_removed)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "old_commit": old_graph.get("metadata", {}).get("git_commit", "unknown"),
        "new_commit": new_graph.get("metadata", {}).get("git_commit", "unknown"),
        "added": added,
        "removed": removed,
        "modified": modified,
        "unchanged": unchanged,
        "edges_added": edges_added,
        "edges_removed": edges_removed,
        "summary": summary,
    }


def _edge_key(edge: dict) -> str:
    return f"{edge['from']}|{edge['to']}|{edge['type']}"


def _detect_changes(old: dict, new: dict) -> list[str]:
    """Describe what changed between two versions of a contract."""
    changes = []
    old_cols = {c["name"] for c in old.get("dataset", {}).get("columns", [])}
    new_cols = {c["name"] for c in new.get("dataset", {}).get("columns", [])}
    if new_cols - old_cols:
        changes.append(f"columns added: {', '.join(sorted(new_cols - old_cols))}")
    if old_cols - new_cols:
        changes.append(f"columns removed: {', '.join(sorted(old_cols - new_cols))}")

    old_mat = old.get("lineage", {}).get("materialization", "")
    new_mat = new.get("lineage", {}).get("materialization", "")
    if old_mat != new_mat:
        changes.append(f"materialization: {old_mat} → {new_mat}")

    old_sql = old.get("_meta", {}).get("sql_hash", "")
    new_sql = new.get("_meta", {}).get("sql_hash", "")
    if old_sql and new_sql and old_sql != new_sql:
        changes.append("SQL logic changed (methodology update)")

    old_rules = len(old.get("quality", {}).get("rules", []))
    new_rules = len(new.get("quality", {}).get("rules", []))
    if old_rules != new_rules:
        changes.append(f"quality rules: {old_rules} → {new_rules}")

    old_tags = set(old.get("tags", []))
    new_tags = set(new.get("tags", []))
    if old_tags != new_tags:
        changes.append(f"tags changed: {sorted(new_tags)}")

    if not changes:
        changes.append("metadata changed")

    return changes


def _build_summary(
    added: dict,
    removed: dict,
    modified: dict,
    edges_added: list,
    edges_removed: list,
) -> str:
    """Build a plain English summary of the diff."""
    parts = []
    if added:
        names = [c["dataset"]["name"] for c in added.values()]
        parts.append(f"Added {len(added)} asset(s): {', '.join(names)}")
    if removed:
        names = [c["dataset"]["name"] for c in removed.values()]
        parts.append(f"Removed {len(removed)} asset(s): {', '.join(names)}")
    if modified:
        for cid, detail in modified.items():
            name = detail["new"]["dataset"]["name"]
            change_str = "; ".join(detail["changes"])
            parts.append(f"Modified {name}: {change_str}")
    if edges_added:
        parts.append(f"{len(edges_added)} new lineage edge(s)")
    if edges_removed:
        parts.append(f"{len(edges_removed)} removed lineage edge(s)")
    if not parts:
        parts.append("No changes detected")
    return "\n".join(parts)


# =============================================
# MERMAID RENDERER
# =============================================

def render_mermaid(
    graph: dict,
    diff: dict | None = None,
    direction: str = "LR",
) -> str:
    """
    Render the graph as a Mermaid diagram.

    If a diff is supplied, colour-codes changes:
      - Green dashed borders  = added
      - Red dashed borders    = removed
      - Orange borders        = modified
      - Default               = unchanged
    """
    lines = [f"graph {direction}"]

    contracts = graph.get("contracts", {})
    edges = graph.get("edges", [])

    # Determine change status per contract
    statuses: dict[str, str] = {}
    if diff:
        for cid in diff.get("added", {}):
            statuses[cid] = "added"
        for cid in diff.get("removed", {}):
            statuses[cid] = "removed"
        for cid in diff.get("modified", {}):
            statuses[cid] = "modified"

    # Node shapes by type
    def _shape(contract: dict, cid: str) -> str:
        name = contract["dataset"]["name"]
        dtype = contract["dataset"]["type"]
        tags = set(contract.get("tags", []))
        safe_id = cid.replace(".", "_").replace("-", "_")
        if dtype == "volume":
            return f"    {safe_id}[(\"{name}\")]"      # cylinder / stadium
        elif dtype == "source":
            return f'    {safe_id}[("{name}")]'       # stadium / rounded
        elif dtype == "function":
            return f"    {safe_id}{{{{{name}}}}}"      # rhombus
        elif dtype == "quality":
            return f'    {safe_id}{{"{name}"}}'       # diamond
        elif dtype == "workflow":
            return f'    {safe_id}(["{name} DAB"])'    # stadium / pill
        elif dtype == "workflow_task":
            return f'    {safe_id}>"{name}"]'          # asymmetric / flag
        elif dtype == "table":
            if "python_managed" in tags:
                return f"    {safe_id}[[\"{name}\"]]"  # subroutine / double-border box
            if "quarantine" in name:
                return f"    {safe_id}[/\"{name}\"/]"  # parallelogram
            elif "_v_previous" in name:
                return f"    {safe_id}[[\"{name}\"]]"  # subroutine
            return f"    {safe_id}[\"{name}\"]"        # rectangle
        return f"    {safe_id}[\"{name}\"]"

    # Emit all nodes
    for cid, contract in contracts.items():
        lines.append(_shape(contract, cid))

    # Also emit removed nodes from diff
    if diff:
        for cid, contract in diff.get("removed", {}).items():
            if cid not in contracts:
                lines.append(_shape(contract, cid))

    # Emit edges
    emitted_edges = set()
    for edge in edges:
        src = edge["from"].replace(".", "_").replace("-", "_")
        tgt = edge["to"].replace(".", "_").replace("-", "_")
        etype = edge["type"]
        key = f"{src}|{tgt}"
        if key in emitted_edges:
            continue
        emitted_edges.add(key)

        if etype == "quarantine":
            lines.append(f"    {src} -.->|quarantine| {tgt}")
        elif etype == "prior_version":
            lines.append(f"    {src} -.->|prior_ver| {tgt}")
        elif etype == "udf_call":
            lines.append(f"    {src} ==>|udf| {tgt}")
        elif etype == "check":
            lines.append(f"    {src} -.->|check| {tgt}")
        elif etype == "orchestrates":
            lines.append(f"    {src} -.->|orchestrates| {tgt}")
        elif etype == "depends_on":
            lines.append(f"    {src} ==>|depends_on| {tgt}")
        else:
            lines.append(f"    {src} --> {tgt}")

    return "\n".join(lines)


# =============================================
# EXPORT: ODCS INDIVIDUAL CONTRACTS
# =============================================

def export_individual_contracts(graph: dict, output_dir: Path) -> list[Path]:
    """
    Export each contract as a standalone ODCS YAML file.
    One file per asset — ready for a data contract catalog.
    """
    import yaml

    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    for contract_id, contract in graph["contracts"].items():
        safe_name = contract_id.replace(".", "__")
        out_path = output_dir / f"{safe_name}.yml"

        # Strip internal fields
        exportable = {k: v for k, v in contract.items() if not k.startswith("_")}
        exportable["id"] = contract_id

        out_path.write_text(yaml.dump(exportable, sort_keys=False, default_flow_style=False))
        written.append(out_path)

    return written


# =============================================
# PERSISTENCE – save / load graph snapshots
# =============================================

def save_graph(graph: dict, path: Path) -> None:
    """Write graph to JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(graph, indent=2, default=str))


def load_graph(path: Path) -> dict | None:
    """Load graph from JSON file, or None if missing."""
    if path.exists():
        return json.loads(path.read_text())
    return None
