# Multi-Domain Pipeline — How-To Guide

> Run the same pipeline across EU, US, and APAC — each in its own schema,
> reading from its own source data, running in its own Databricks workflow.
> Zero code duplication.

---

## What You'll Build

The same four-model pipeline from the sales project, but deployed three
times — once per region — each with isolated schemas and Volumes:

```
  landing_eu  ─▶ ingest ─▶ raw_customers_eu ─▶ stg_customers_eu ─▶ customer_clean_eu ─▶ customer_orders_eu ─▶ customer_summary_eu
  landing_us  ─▶ ingest ─▶ raw_customers_us ─▶ stg_customers_us ─▶ customer_clean_us ─▶ customer_orders_us ─▶ customer_summary_us
  landing_apac─▶ ingest ─▶ raw_customers_apac─▶ stg_customers_apac─▶ customer_clean_apac─▶ customer_orders_apac─▶ customer_summary_apac
```

Raw data arrives via per-domain Volumes and is ingested by a Python task.
Each domain runs in its own Databricks workflow (`PROCESS_regional_eu`,
`PROCESS_regional_us`, `PROCESS_regional_apac`) for parallel execution.

**Prerequisite:** Familiarity with the [Sales Project HOW-TO](../sales_project/HOW-TO.md).
This guide covers only the domain-specific additions.

**Time to complete:** ~10 minutes (on top of a working single-domain project).

---

## Step 1 — Add Domains to forge.yml

The only config change needed. Add a `domains:` block and specify which
layers get per-domain instances:

```yaml
# forge.yml

# ── Domains — one schema per region ──
domains:
  eu:   { schema_suffix: "_eu" }
  us:   { schema_suffix: "_us" }
  apac: { schema_suffix: "_apac" }

# Which layers get per-domain instances
domain_layers: [bronze, silver]
```

**What this does:**
- Every model in bronze and silver is compiled N times (once per domain)
- Each instance writes to its own schema: `ben_regional_eu`, `ben_regional_us`, `ben_regional_apac`
- Refs between models are rewritten: `customer_orders_eu` references `customer_clean_eu`, not the base

---

## Step 2 — Define Raw Data Models (Per-Domain)

Raw data arrives via Volume ingestion, not CSV seeds. Declare `managed_by: python`
models with `domain: true` so forge creates per-domain raw tables:

**`dbt/ddl/bronze/models/raw_customers.yml`**

```yaml
models:
  raw_customers:
    description: "Raw customer data ingested from Volume"
    managed_by: python
    domain: true
    columns:
      customer_id:  { type: int, description: "Unique customer identifier" }
      email:        { type: string }
      country:      { type: string }
      revenue:      { type: "decimal(10,2)" }
```

**What this does:**
- `domain: true` → forge creates `raw_customers_eu`, `raw_customers_us`, `raw_customers_apac`
- `managed_by: python` → the Python ingestion task populates rows; forge manages the schema
- No `domain_sources:` needed on staging models — domain ref rewriting handles it automatically

Sample data files are in `sample_data/`. After deploying, upload them
to each domain's landing Volume:

```bash
databricks fs cp sample_data/raw_customers_eu.csv dbfs:/Volumes/bronze/ben_regional_eu/landing_eu/customers_20240101.csv
databricks fs cp sample_data/raw_customers_us.csv dbfs:/Volumes/bronze/ben_regional_us/landing_us/customers_20240101.csv
```

> **Note:** `domain_sources:` is still available for advanced cases where
> domain-specific routing differs from the default bifurcation pattern
> (e.g. overriding a single join source per domain). For simple Volume
> ingestion, `managed_by: python` with `domain: true` handles everything.

---

## Step 3 — Downstream Models Need No Changes

Models that reference an upstream domain model get the rewriting for free.
No `domain_sources:` needed:

**`dbt/ddl/silver/customer_clean.yml`**

```yaml
models:
  customer_clean:
    description: "Clean customer data with quarantine"
    source: stg_customers
    materialized: table
    quarantine: "email IS NULL OR revenue < 0"
    columns:
      customer_id:  { type: int, required: true, unique: true }
      email:        { type: string, required: true }
      country:      { type: string }
      revenue:      { type: "decimal(10,2)" }
```

Even though `source: stg_customers` is written once, forge automatically
compiles `customer_clean_eu.sql` with `{{ ref('stg_customers_eu') }}`,
`customer_clean_us.sql` with `{{ ref('stg_customers_us') }}`, etc.

The same applies to joins, aggregations, and any model that references
an upstream model in a bifurcated layer.

---

## Step 4 — Add Volume Ingestion (Per-Domain)

### 4a. Declare a landing Volume

**`dbt/ddl/bronze/volumes/landing.yml`**

```yaml
volumes:
  landing:
    description: "Landing zone for inbound data files"
    type: managed
    layer: bronze
    domain: true
```

With `domain: true`, forge generates three Volumes: `landing_eu`, `landing_us`,
`landing_apac` — each in its domain's schema. Files for each region land in
separate Volumes.

### 4b. Add the ingestion config seed (meta catalog)

**`dbt/ddl/meta/seeds/ingestion_config.yml`**

```yaml
seeds:
  ingestion_config:
    description: "File-matching rules for Volume ingestion"
    domain: false
    catalog: meta
    schema: config
    origin:
      type: seed
      path: dbt/seeds/ingestion_config.csv
      format: csv
    columns:
      config_id:      { type: int, required: true, unique: true }
      ingest_type:    { type: string, required: true }
      source_name:    { type: string, required: true }
      target_model:   { type: string, required: true }
      volume_name:    { type: string }
      file_regex:     { type: string }
      file_format:    { type: string }
      has_header:     { type: boolean }
      delimiter:      { type: string }
      active:         { type: boolean }
```

**`dbt/seeds/ingestion_config.csv`**

```csv
config_id,ingest_type,source_name,target_model,volume_name,file_regex,file_format,has_header,delimiter,active
1,volume,customer_feed,raw_customers,landing,"^customers_.*\.csv$",csv,true,",",true
2,volume,order_feed,raw_orders,landing,"^orders_.*\.csv$",csv,true,",",true
```

The config is shared — `domain: false` — but each domain's Python task
reads from its own `landing_{domain}` Volume.

**Don't forget:** add `meta` to the `catalogs:` list in forge.yml:

```yaml
catalogs: [bronze, silver, meta]
```

### 4c. Add the file manifest model (domain-aware)

**`dbt/ddl/bronze/models/file_manifest.yml`**

```yaml
models:
  file_manifest:
    description: "Tracks files ingested from Volumes"
    managed_by: python
    domain: true
    columns:
      file_path:       { type: string, required: true }
      file_name:       { type: string, required: true }
      model_name:      { type: string, required: true }
      domain:          { type: string }
      file_format:     { type: string }
      row_count:       { type: int }
      file_size_bytes: { type: int }
      checksum:        { type: string }
      ingested_at:     { type: timestamp, required: true }
      status:          { type: string }
```

With `domain: true`, forge creates `file_manifest_eu`, `file_manifest_us`,
`file_manifest_apac` — each in its domain schema.

### 4d. Write the Python ingestion task

Scaffold it:

```bash
forge python-task ingest_from_volume --template ingest
```

Or drop in your own at **`python/ingest_from_volume.py`**. The task reads
`forge.domain` from Spark conf to know which domain it's running for.

Register in forge.yml:

```yaml
python_tasks:
  ingest_from_volume:
    stage: ingest
    description: "Reads files from landing Volume → raw tables"
    template: ingest
```

Each per-domain workflow runs this task with its domain's context:
- `PROCESS_regional_eu` → reads from `landing_eu`, writes to `file_manifest_eu`
- `PROCESS_regional_us` → reads from `landing_us`, writes to `file_manifest_us`
- etc.

---

## Step 5 — Compile & Inspect

```bash
forge compile
```

Check the generated output:

```
dbt/models/regional/
├── bronze/
│   ├── eu/
│   │   ├── raw_customers_eu.sql     ← CREATE TABLE (managed_by: python)
│   │   ├── raw_orders_eu.sql
│   │   ├── file_manifest_eu.sql
│   │   ├── stg_customers_eu.sql     ← reads from raw_customers_eu
│   │   └── stg_orders_eu.sql
│   ├── us/
│   │   └── ...
│   └── apac/
│       └── ...
└── silver/
    ├── eu/
    │   ├── customer_clean_eu.sql
    │   ├── customer_orders_eu.sql
    │   └── customer_summary_eu.sql
    ├── us/
    │   └── ...
    └── apac/
        └── ...
```

Verify the source routing worked:

```bash
# EU should read from raw_customers_eu
grep "from" dbt/models/regional/bronze/eu/stg_customers_eu.sql
# → from {{ ref('raw_customers_eu') }}

# US should read from raw_customers_us
grep "from" dbt/models/regional/bronze/us/stg_customers_us.sql
# → from {{ ref('raw_customers_us') }}
```

---

## Step 6 — Per-Domain Workflows

By default, each domain gets its own Databricks workflow:

```bash
forge workflow
```

```
PROCESS_regional_eu   → ingest → stage → clean → serve (11 tasks)
PROCESS_regional_us   → ingest → stage → clean → serve (11 tasks)
PROCESS_regional_apac → ingest → stage → clean → serve (11 tasks)
```

Each workflow runs independently — EU, US, and APAC can be scheduled
at different times or parallelised.

Generate the DAB workflow YAML:

```bash
forge workflow --dab
# ✅ Workflow written to resources/jobs/PROCESS_regional_eu.yml
# ✅ Workflow written to resources/jobs/PROCESS_regional_us.yml
# ✅ Workflow written to resources/jobs/PROCESS_regional_apac.yml
```

### Opting for a single shared workflow

If you prefer all domains in one workflow (e.g. for simplicity):

```yaml
# forge.yml
domain_workflows: shared
```

```bash
forge workflow
# PROCESS_regional → ingest → stage → clean → serve (all domains in one DAG)
```

---

## Step 7 — Deploy

```bash
forge deploy
```

This:
1. Creates domain schemas (`ben_regional_eu`, `ben_regional_us`, `ben_regional_apac`)
2. Creates per-domain Volumes and raw tables
3. Seeds config data (`ingestion_config`)
4. Runs all domain instances (20+ models)
5. Generates per-domain DAB workflows
6. Deploys to Databricks via `databricks bundle deploy`

After deploying, upload sample data to each domain's Volume, then
run the ingestion task:

```bash
# Upload sample data to domain Volumes
databricks fs cp sample_data/raw_customers_eu.csv dbfs:/Volumes/.../landing_eu/
databricks fs cp sample_data/raw_orders_eu.csv    dbfs:/Volumes/.../landing_eu/
# Repeat for us, apac...
```

---

## Step 8 — Opt a Model Out of Domains

Reference tables or shared lookups that should NOT be duplicated:

```yaml
models:
  country_codes:
    domain: false                    # stays shared — no EU/US/APAC copies
    source: raw_country_codes
    columns:
      code: { type: string, required: true }
      name: { type: string }
```

Downstream domain models that reference `country_codes` will correctly
ref the single shared table, not a non-existent `country_codes_eu`.

---

## How It Works Under the Hood

1. `forge compile` reads `domains:` from forge.yml
2. For each model in a `domain_layers` layer:
   - Clones the model definition N times (once per domain)
   - Appends `_{domain}` to the model name
   - Hardcodes schema to `{base_schema}{schema_suffix}` (e.g. `ben_regional_eu`)
   - Rewrites upstream refs to domain-specific variants
   - Applies `domain_sources:` overrides if defined (optional, advanced)
3. Each instance is a real dbt model — tests, docs, graph, workflow all work
4. `build_domain_workflows()` splits the graph into per-domain subsets

---

## Quick Reference

| What | How |
|------|-----|
| Add a domain | `domains: { jp: { schema_suffix: "_jp" } }` in forge.yml |
| Choose which layers bifurcate | `domain_layers: [bronze, silver]` |
| Domain-specific source | `domain_sources:` in model DDL (advanced, for non-bifurcated overrides) |
| Opt a model out | `domain: false` in model DDL |
| Shared workflow | `domain_workflows: shared` in forge.yml |
| Separate workflows (default) | One workflow per domain, runs in parallel |
| Check compiled source | `grep "from" dbt/models/.../eu/stg_customers_eu.sql` |
| Deploy all domains | `forge deploy` (no extra flags) |
| Declare a per-domain Volume | `domain: true` in `dbt/ddl/{layer}/volumes/*.yml` |
| Upload domain data | `databricks fs cp sample_data/*.csv dbfs:/Volumes/.../landing_eu/` |
| Scaffold Python task | `forge python-task my_task --template ingest` |
| Python-managed model | `managed_by: python` in model DDL |

---

## Project Structure

```
multi_domain_project/
├── forge.yml                                ← domains + domain_layers here
├── HOW-TO.md                                ← this guide
├── python/                                  ← Python tasks (YOU edit)
│   └── ingest_from_volume.py                ← Volume ingestion (runs per domain)
├── sample_data/                             ← sample files to upload to Volumes
│   ├── raw_customers_eu.csv
│   ├── raw_customers_us.csv
│   ├── raw_customers_apac.csv
│   ├── raw_orders_eu.csv
│   ├── raw_orders_us.csv
│   └── raw_orders_apac.csv
└── dbt/
    ├── ddl/
    │   ├── bronze/
    │   │   ├── models/
    │   │   │   ├── raw_customers.yml        ← raw tables (managed_by: python, domain: true)
    │   │   │   ├── raw_orders.yml
    │   │   │   └── file_manifest.yml        ← ingestion audit (domain: true)
    │   │   ├── staging/
    │   │   │   ├── stg_customers.yml        ← staging (auto per domain)
    │   │   │   └── stg_orders.yml
    │   │   └── volumes/
    │   │       └── landing.yml              ← per-domain Volumes (domain: true)
    │   ├── meta/
    │   │   └── seeds/
    │   │       └── ingestion_config.yml     ← ingestion rules (shared, meta catalog)
    │   └── silver/
    │       ├── customer_clean.yml           ← quarantine (auto per domain)
    │       ├── customer_orders.yml          ← join (auto per domain)
    │       └── customer_summary.yml         ← aggregation + checks
    └── seeds/
        └── ingestion_config.csv             ← ingestion rules data
```

**The rule:** model definitions stay the same across all domains.
Raw data arrives via per-domain Volumes — not CSV seeds.
Only `forge.yml` (add domains) changes when you go multi-domain.
