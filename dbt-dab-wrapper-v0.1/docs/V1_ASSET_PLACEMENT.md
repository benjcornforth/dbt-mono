# V1 Asset Placement Contract

This document defines the v1 contract for asset classification, folder layout, naming resolution, and validation.

The intent is to remove implicit placement behavior and replace it with a small number of framework rules:

- The DDL tree defines authored asset intent.
- `forge.yml` defines placement families and enabled system assets.
- The compiler resolves every asset through one placement contract.
- Compile-time validation rejects invalid class/layer/family combinations.

## Core Model

Every asset is described by four things:

1. `class`
2. `layer`
3. `placement_family`
4. identity token:
   - `domain` for `class: domain`
   - `namespace` for `class: shared`
   - `system` for `class: system`

### Asset Classes

| Class | Meaning | Authored In | Examples |
| --- | --- | --- | --- |
| `domain` | Pipeline data products and transforms | DDL tree | raw tables, staging, marts, domain UDFs |
| `shared` | Cross-domain metadata, config, reference, operational authored assets | DDL tree | ingestion config, shared metadata tables |
| `system` | Compiler-owned platform assets | `forge.yml` | lineage, quarantine, backups, deployment state |

### Allowed Layers

These rules are framework invariants. They are baked into the CLI/compiler and are not project-configurable.

| Class | Allowed Layers |
| --- | --- |
| `domain` | `bronze`, `silver`, `gold` |
| `shared` | `meta`, `operations` |
| `system` | `meta`, `operations` |

## Canonical V1 Tree

`forge setup` should scaffold this tree and only this tree:

```text
dbt/ddl/
  domain/
    sales/
      bronze/
        models/
        volumes/
      silver/
        models/
        udfs/
      gold/
        models/
  shared/
    meta/
      seeds/
      models/
    operations/
      models/
python/
  ingest_from_volume.py
```

### Tree Semantics

- `dbt/ddl/domain/<domain>/<layer>/<kind>/...`
  - class = `domain`
  - domain = `<domain>`
  - layer = `<layer>`
- `dbt/ddl/shared/<layer>/<kind>/...`
  - class = `shared`
  - layer = `<layer>`
  - namespace must be declared in the file
- `system` assets are not authored in the DDL tree by default

## forge.yml Shape

The current global `catalog_pattern` and `schema_pattern` are replaced by named placement families.

```yaml
name: sales_data
scope: fd

placements:
  families:
    domain:
      allowed_classes: [domain]
      allowed_layers: [bronze, silver, gold]
      catalog_pattern: "{env}_{scope}_{layer}"
      schema_pattern: "{user}_{domain}"

    shared_meta:
      allowed_classes: [shared]
      allowed_layers: [meta]
      catalog_pattern: "{env}_{scope}_meta"
      schema_pattern: "{user}_{namespace}"

    shared_ops:
      allowed_classes: [shared]
      allowed_layers: [operations]
      catalog_pattern: "{env}_{scope}_ops"
      schema_pattern: "{user}_{namespace}"

    system_meta:
      allowed_classes: [system]
      allowed_layers: [meta]
      catalog_pattern: "{env}_{scope}_meta"
      schema_pattern: "{user}_{system}"

    system_ops:
      allowed_classes: [system]
      allowed_layers: [operations]
      catalog_pattern: "{env}_{scope}_ops"
      schema_pattern: "{user}_{system}"

domains:
  sales:
    placement_family: domain

shared_namespaces:
  config:
    placement_family: shared_meta
    layer: meta
  metadata:
    placement_family: shared_meta
    layer: meta
  operations:
    placement_family: shared_ops
    layer: operations

system_assets:
  lineage:
    enabled: true
    placement_family: system_meta
    layer: meta
    system: lineage

  quarantine:
    enabled: true
    placement_family: system_meta
    layer: meta
    system: quarantine

  backups:
    enabled: true
    placement_family: system_ops
    layer: operations
    system: backups

  deployment_state:
    enabled: true
    placement_family: system_ops
    layer: operations
    system: deploy

profiles:
  dev:
    platform: databricks
    databricks_profile: dev
    env: dev
    compute:
      type: serverless
      auto_scale: true

  prod:
    platform: databricks
    databricks_profile: PROD
    env: prd
    compute:
      type: serverless
    schedule: "0 0 6 * * ?"

  local:
    platform: postgres
    connection:
      host: localhost
      port: 5432
      database: sales_db
    overrides:
      placement_families:
        domain:
          catalog: public
        shared_meta:
          catalog: public
        shared_ops:
          catalog: public
        system_meta:
          catalog: public
        system_ops:
          catalog: public
    compute:
      type: local
```

## Authored DDL Examples

### Domain Model

```yaml
models:
  customer_summary:
    class: domain
    domain: sales
    placement_family: domain
    description: "Aggregated customer metrics"
    source: customer_orders
    materialized: table
    group_by: [customer_id, first_name, last_name, email, country]
    columns:
      customer_id: { type: int, required: true, unique: true }
      total_orders: { expr: "count(order_id)", type: int }
      total_revenue: { expr: "sum(line_total)", type: "decimal(10,2)" }
```

### Shared Seed

```yaml
seeds:
  ingestion_config:
    class: shared
    namespace: config
    placement_family: shared_meta
    description: "Cross-domain ingestion rules"
    origin:
      type: seed
      path: dbt/seeds/ingestion_config.csv
      format: csv
    columns:
      config_id: { type: int, required: true, unique: true }
      ingest_type: { type: string, required: true }
      target_model: { type: string, required: true }
```

### Python-Managed Domain Table

```yaml
models:
  raw_customers:
    class: domain
    domain: sales
    placement_family: domain
    managed_by: ingest_from_volume
    columns:
      customer_id: { type: int }
      first_name: { type: string }
      last_name: { type: string }
      email: { type: string }
```

## Compiler Contract

The compiler should stop resolving "model schema" directly and instead resolve a normalized asset manifest.

Suggested internal shape:

```python
@dataclass
class AssetRef:
    name: str
    kind: str
    class_name: str
    layer: str
    domain: str | None
    namespace: str | None
    system: str | None
    placement_family: str
    catalog: str
    schema: str
    managed_by: str | None
    definition: dict[str, Any]
```

Everything should compile from `AssetRef` objects:

- graph generation
- workflow generation
- SQL generation
- system asset generation
- validation

## Required Code Changes

### CLI

File: `src/forge/cli.py`

- Replace generic `dbt/ddl/<layer>/<kind>` scaffolding with the canonical v1 tree.
- Stop deriving folder roots from `schemas` / `catalogs` config lists.
- Make `forge setup` scaffold a default domain tree plus shared folders.
- Add an optional `--domain` argument to `forge setup`.

### Resolver

File: `src/forge/compute_resolver.py`

- Replace `resolve_model_schema(...)` with `resolve_asset_location(...)`.
- Centralize framework grammar:
  - valid classes
  - allowed layers by class
  - required identity token by class
- Resolve catalog/schema from named placement families.
- Support profile-specific placement overrides.

Suggested invariants:

```python
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
```

### DDL Loader

File: `src/forge/simple_ddl.py`

- Parse canonical path semantics from:
  - `dbt/ddl/domain/<domain>/<layer>/<kind>/...`
  - `dbt/ddl/shared/<layer>/<kind>/...`
- Stop using top-level layers as the only meaning.
- Attach normalized class/layer/identity metadata before compile.
- Resolve all authored assets through `resolve_asset_location(...)`.

### System Asset Generation

File: `src/forge/simple_ddl.py`

- Stop hardcoding placement logic for:
  - lineage graph/log
  - transform quarantine
  - ingest quarantine
  - backup archive
  - deployment version/state if added
- Generate them from `system_assets` in `forge.yml`.
- Resolve them through the same placement resolver as authored assets.

## Validation Matrix

Validation is split across setup-time, DDL-load-time, and compile-time checks.

### 1. Setup Validation

Performed by `forge setup`.

| Check | Why |
| --- | --- |
| default domain name is valid | prevents invalid folder scaffolding |
| scaffolded folders match the baked-in tree | keeps the CLI opinionated and unambiguous |
| no legacy generic tree is created | avoids mixing v0 and v1 structures |

Setup should be strict about the tree it creates, but it should not try to validate full project semantics.

### 2. forge.yml Validation

Performed before loading DDL.

| Check | Why |
| --- | --- |
| `placements.families` exists | naming contract must be explicit |
| every family has `allowed_classes`, `allowed_layers`, `catalog_pattern`, `schema_pattern` | incomplete family definitions are unusable |
| family `allowed_classes` values are valid | protects resolver grammar |
| family `allowed_layers` values are valid | blocks impossible combinations |
| every declared domain has a placement family | domain assets must resolve deterministically |
| every declared shared namespace has a placement family and layer | shared assets must resolve deterministically |
| every enabled system asset has a placement family, layer, and system token | generated assets must resolve deterministically |
| profile overrides only reference known families | prevents dead config |

### 3. DDL Load Validation

Performed while parsing authored assets.

| Check | Why |
| --- | --- |
| file path matches the canonical v1 tree | no ambiguous source-of-truth |
| path-derived class is valid | enforces framework grammar |
| path-derived layer is allowed for that class | catches invalid tree placement immediately |
| top-level block matches asset kind (`models`, `seeds`, `udfs`, `volumes`) | avoids malformed files |
| `class` in file, if present, matches the path-derived class | blocks contradictory declarations |
| `placement_family` exists | every asset must be placeable |
| `domain` assets declare `domain` | required identity token |
| `shared` assets declare `namespace` | required identity token |
| `system` assets are not authored in normal DDL files | keeps authored vs generated boundary clear |
| `managed_by` references a discovered Python task | explicit ownership must resolve |

### 4. Placement Resolution Validation

Performed when converting authored or generated assets into `AssetRef` objects.

| Check | Why |
| --- | --- |
| placement family exists | required for resolution |
| asset class is allowed by that family | blocks invalid family reuse |
| layer is allowed by that family | blocks invalid layer reuse |
| all required template tokens are present | prevents malformed catalog/schema names |
| profile override shape is valid | ensures environment-specific behavior is deterministic |
| resolved catalog/schema are non-empty | avoids invalid SQL emission |

### 5. Manifest Validation

Performed after all authored and generated assets are normalized.

| Check | Why |
| --- | --- |
| asset names are unique within their resolved fq object | avoids collisions |
| no two assets resolve to the same `catalog.schema.object` | prevents destructive ambiguity |
| authored assets do not collide with system assets | protects platform objects |
| all `source` / `sources` references resolve | prevents broken graph edges |
| all UDF dependencies resolve | prevents broken setup order |
| all enabled system assets are present in the manifest | keeps feature contract explicit |

### 6. Compile Validation

Performed immediately before SQL/workflow emission.

| Check | Why |
| --- | --- |
| every SQL-emitting asset has a resolved catalog/schema | SQL generation requires a concrete location |
| system assets required by enabled features have resolved locations | setup workflow must be complete |
| workflow task ownership is consistent with manifest | prevents SQL/Python routing drift |
| no compile output ordering violations remain | protects independent Databricks task execution |

## Failure Philosophy

The v1 contract is intentionally strict.

- Invalid class/layer combinations are compile errors.
- Missing placement families are compile errors.
- Missing identity tokens are compile errors.
- Location collisions are compile errors.
- Ambiguous authored-vs-system ownership is a compile error.

The compiler should reject invalid projects before any Databricks setup or workflow run begins.

## Recommended Implementation Sequence

1. Add framework grammar constants and `resolve_asset_location(...)`.
2. Rewrite `forge setup` to scaffold the v1 tree.
3. Rewrite DDL loading around `domain/...` and `shared/...` path semantics.
4. Introduce `AssetRef` as the single internal manifest shape.
5. Move lineage/quarantine/backups/deploy-state onto `system_assets` in `forge.yml`.
6. Add the validation matrix above.
7. Regenerate SQL/workflows from the manifest only.

## Explicit Non-Goals

This v1 contract intentionally removes these older ideas:

- global `catalog_pattern`
- global `schema_pattern`
- top-level `catalogs` list as placement source
- top-level `schemas` list as placement source
- inferring semantic ownership from `meta` alone
- compiler-owned placement special cases hidden inside SQL helpers

That is deliberate. The goal is to reduce ambiguity, not preserve backward compatibility.