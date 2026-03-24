# dbt-forge

**Production dbt pipelines on Databricks — without writing SQL, YAML boilerplate, or DAB config.**

You edit two files. Forge generates everything else: SQL models, schema tests, Databricks Asset Bundles, lineage graphs, data quality checks, and UDFs.

---

## Quick Start

```bash
# 1. Clone and setup
git clone <repo>
cd dbt-forge
./setup.sh

# 2. Configure Databricks
databricks auth login

# 3. Define your tables under dbt/ddl/<layer>/<section>/
# 4. Deploy
python -m src.forge.cli workflow
databricks bundle deploy --target dev
```

---

## What This Is

A CLI that sits on top of **dbt** and **Databricks Asset Bundles (DAB)**. It turns a declarative YAML definition of your tables into a fully deployed, tested, lineage-tracked data pipeline.

**The source-of-truth contract:** you edit `forge.yml` and the canonical DDL tree under `dbt/ddl/<layer>/<section>/`. Everything else — SQL, schema.yml, DAB bundles, UDF scripts — is generated.

---

## Installation

### Automated Setup (Recommended)

```bash
./setup.sh
```

This installs:
- Python dependencies via Poetry
- Databricks CLI (new version with bundle support)
- Verifies all installations

### Manual Setup

```bash
# Install Python dependencies
poetry install

# Install Databricks CLI
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Verify
poetry run python --version
databricks --version
```

---

## What You Do

```bash
# 1. Authenticate with Databricks
databricks auth login

# 2. Define your assets in dbt/ddl/<layer>/<section>/
#    (models, UDFs, seeds, volumes)

# 3. Generate workflows
python -m src.forge.cli workflow

# 4. Deploy bundle
databricks bundle deploy --target dev
```

That's the core loop. Edit `dbt/ddl/` → `python -m src.forge.cli workflow` → `databricks bundle deploy`.

### Atomic Commands

```bash
# Rebuild and redeploy everything
cd /path/to/dbt-forge
python -m src.forge.cli workflow && databricks bundle deploy --target dev

# Validate before deploy
databricks bundle validate

# Destroy deployment
databricks bundle destroy --target dev
```

### Multi-environment profiles

```yaml
# forge.yml — define once, deploy anywhere

id: myproject                              # used in catalog_pattern as {id}

# Naming patterns — control how catalog and schema names are constructed.
# Tokens: {env}, {id}, {catalog}, {schema}
# Production envs (prd/prod) skip the {env}_ prefix automatically.
catalog_pattern: "{env}_{id}_{catalog}"    # → dev_myproject_bronze, int_myproject_meta
schema_pattern:  "{env}_{schema}"          # → dev_bronze, int_silver
skip_env_prefix: [prd, prod]

# Logical names — actual names are built via the patterns above.
catalogs: [bronze, silver, meta, operations]
schemas:  [bronze, silver, gold]

profiles:
  dev:
    platform: databricks
    databricks_profile: DEFAULT
    env: dev                               # → dev_myproject_bronze, dev_silver, etc.
    catalog: bronze                        # default catalog for unmatched models
    compute: { type: serverless }
  prod:
    databricks_profile: PROD
    env: prd                               # → myproject_bronze, silver (no env prefix)
    catalog: bronze
  int:
    databricks_profile: INT
    env: int                               # → int_myproject_bronze, int_silver, etc.
    catalog: bronze
  local:
    platform: postgres
    connection: { host: localhost, port: 5432, database: analytics }
    schemas: { bronze: bronze, silver: silver, gold: gold }  # explicit — no pattern
    catalogs: { bronze: public, silver: public, meta: public, operations: public }
```

Set `env:` and the patterns auto-construct catalog and schema names. Production environments (`prd` / `prod`) drop the `{env}_` prefix. You can override with explicit `schemas:` / `catalogs:` dicts per profile.

**Catalog expansion** (`{env}_{id}_{catalog}` with `id: myproject`):

| Logical | `env: dev` | `env: int` | `env: prd` |
|---|---|---|---|
| bronze | `dev_myproject_bronze` | `int_myproject_bronze` | `myproject_bronze` |
| silver | `dev_myproject_silver` | `int_myproject_silver` | `myproject_silver` |
| meta | `dev_myproject_meta` | `int_myproject_meta` | `myproject_meta` |
| operations | `dev_myproject_operations` | `int_myproject_operations` | `myproject_operations` |

**Schema expansion** (`{env}_{schema}`):

| Prefix | Layer | `env: dev` | `env: prd` |
|---|---|---|---|
| `raw_`, `src_`, `seed_` | bronze | `dev_bronze` | `bronze` |
| `stg_`, `int_`, `clean_` | silver | `dev_silver` | `silver` |
| `fct_`, `dim_`, `agg_`, `rpt_` | gold | `dev_gold` | `gold` |

Variables (`schema_bronze`, `catalog_bronze`, `catalog_meta`, etc.) are injected into the generated `profiles.yml` for dbt Jinja: `{{ var('catalog_silver') }}`.

```bash
forge deploy --profile prod     # deploy to prod
forge compile --pure-sql -p dev  # compile for dev (stg_ → dev_myproject_silver.dev_silver)
forge compile --pure-sql -p prod # compile for prod (stg_ → myproject_silver.silver)
forge profiles                  # list all profiles + expanded names
forge profiles --generate       # auto-generate profiles.yml with vars
```

### Dev workflow

```bash
forge dev-up          # isolated dev schema (dev_silver_<you>)
forge dev             # watch mode — auto-recompiles on save
forge dev-down        # tears down dev schema
```

### Inspect & validate

```bash
forge diff            # what changed since last deploy
forge diff --mermaid  # color-coded graph diff

forge explain customer_summary.tier           # provenance tree
forge explain customer_summary.tier --mermaid # as a diagram

forge validate        # data quality check report
forge workflow        # pipeline DAG (INGEST → STAGE → CLEAN → ENRICH → SERVE)
forge workflow --dab  # writes Databricks jobs YAML → resources/jobs/
```

### SQL-only mode (no dbt at runtime)

```bash
forge compile --pure-sql   # emits numbered .sql files to sql/
```

Produces standalone SQL that runs directly on a Databricks SQL warehouse — no dbt installation, no Python, no cold starts. Files are numbered for execution order (UDFs first, then models by dependency, quarantine after each model).

```
sql/000_udfs.sql              ← CREATE FUNCTION statements
sql/001_stg_customers.sql     ← CREATE VIEW AS SELECT ...
sql/002_stg_orders.sql
sql/003_customer_clean.sql
sql/003_customer_clean_quarantine.sql
sql/004_customer_orders.sql
sql/005_customer_summary.sql
```

---

## What You Get In Return

| Capability | How |
|---|---|
| **Zero SQL authoring** | Define columns + types in YAML → SQL is generated |
| **Automatic lineage** | Every row carries `_lineage` (model, sources, git commit, deploy timestamp) |
| **Column-level provenance** | `forge explain` traces any column through expressions, UDFs, joins → back to raw source |
| **Data quality checks** | Inline `checks:` block (range, recency, row_count, regex, custom SQL) — no extra framework |
| **Reusable UDFs** | SQL-first, Python fallback. Defined in `dbt/ddl`, deployed via `dbt run-operation` |
| **Multi-platform profiles** | `databricks_profile: PROD` reads `~/.databrickscfg` — zero env vars. Postgres, Redshift profiles too |
| **Pattern-based naming** | `catalog_pattern: "{env}_{id}_{catalog}"` → `dev_myproject_bronze`. Configurable per-project, auto-expands per env |
| **Medallion layer routing** | Models auto-route to bronze/silver/gold schemas + catalogs by name prefix. Per-profile `schemas:` / `catalogs:` |
| **Graph diffs** | Before/after comparison of every asset, column, and lineage edge |
| **Quarantine** | Add `quarantine: "condition"` → bad rows auto-route to `{model}_quarantine` |
| **Prior versions** | Every table gets a `_v_previous` snapshot for safe rollback |
| **Schema migrations** | `forge migrate` applies YAML-defined changes; `--dry-run` to preview |
| **Type-safe Python SDK** | `forge codegen` → Pydantic models for every table |
| **Pipeline DAG** | `forge workflow --dab` → Databricks jobs YAML, auto-staged |
| **Dev isolation** | Per-developer schema, watch-mode recompile, zero-dependency teardown |
| **ODCS contracts** | Every model is an Open Data Contract Standard v3.0.0 node |
| **Portability flags** | `postgres_compatible: true` in forge.yml → avoids Databricks-only SQL |
| **SQL-only mode** | `--pure-sql` emits standalone SQL — no dbt install needed at runtime |
| **Multi-domain (regions/tenants)** | `domains:` in forge.yml → same model compiles to `_eu`, `_us`, `_apac` instances in isolated schemas |
| **Domain source routing** | `domain_sources:` per model → each domain reads from its own source (e.g. region-specific volumes) |
| **Per-domain workflows** | Each domain gets its own Databricks workflow for parallel execution (opt-out with `domain_workflows: shared`) |

---

## Project Structure

```
forge.yml                ← project config + profiles (YOU edit)
dbt/ddl/                ← canonical YAML source of truth (YOU edit)
dbt/models/*.sql         ← generated (never edit)
dbt/models/schema.yml    ← generated tests
dbt/migrations/*.yml     ← schema changes (you create)
sql/                     ← generated by --pure-sql (standalone, no dbt)
src/forge/               ← CLI + engines (you don't touch)
resources/jobs/          ← generated Databricks workflow YAML
artifacts/               ← graph snapshots, compiled output
docs/                    ← guides, roadmap, planning specs
docs/examples/           ← example projects
```

Macros (lineage, quarantine, prior_version, UDFs) live in the **dbt-dab-tools** package, pulled via `packages.yml`. They are never in this repo.

---

## All Commands

| Command | Purpose |
|---|---|
| `forge setup` | Scaffold project |
| `forge compile [--pure-sql]` | YAML → SQL + schema.yml (or standalone SQL) |
| `forge deploy [--env prod] [-p profile]` | Build + deploy to Databricks |
| `forge diff [--mermaid]` | Graph diff since last deploy |
| `forge explain model.col [--mermaid\|--json\|--full]` | Column provenance |
| `forge migrate [--dry-run]` | Apply schema migrations |
| `forge dev-up` / `dev` / `dev-down` | Dev mode lifecycle |
| `forge validate [--model X]` | Data quality checks |
| `forge workflow [--mermaid\|--dab] [-p profile]` | Pipeline DAG (--dab writes to resources/jobs/) |
| `forge codegen [--check]` | Type-safe Python SDK |
| `forge guide` | Regenerate full project guide |
| `forge profiles [--generate\|--show-connection]` | List profiles, generate profiles.yml |
| `forge teardown` | Destroy everything safely |

---

## Multi-Domain Pipelines (EU / US / APAC)

Run the same model logic across multiple domains (regions, tenants, business units) — each in its own schema, with its own source data, running in parallel.

### 1. Declare domains in forge.yml

```yaml
# forge.yml
domains:
  eu:   { schema_suffix: "_eu" }
  us:   { schema_suffix: "_us" }
  apac: { schema_suffix: "_apac" }
domain_layers: [bronze, silver]   # which layers get domain instances
```

### 2. Compile generates per-domain instances

```bash
forge compile
# Generates:
#   stg_customers_eu.sql   → schema: ben_demo_eu
#   stg_customers_us.sql   → schema: ben_demo_us
#   stg_customers_apac.sql → schema: ben_demo_apac
```

Refs are rewritten automatically — `customer_orders_eu` references `customer_clean_eu`.

### 3. Domain-specific sources (optional)

Each domain can read from its own source (e.g. region-specific volumes):

```yaml
# dbt/ddl/bronze/staging/stg_customers.yml
models:
  stg_customers:
    source: raw_customers              # default / fallback
    domain_sources:
      eu: raw_customers_eu             # EU reads from EU volume
      us: raw_customers_us
      apac: raw_customers_apac
    columns:
      customer_id: { type: int, required: true }
```

### 4. Per-domain workflows (default)

Each domain gets its own Databricks workflow for parallel scheduling:

```
PROCESS_demo_eu   → stg_customers_eu → customer_clean_eu → ...
PROCESS_demo_us   → stg_customers_us → customer_clean_us → ...
PROCESS_demo_apac → stg_customers_apac → customer_clean_apac → ...
```

To collapse into a single workflow: `domain_workflows: shared` in forge.yml.

### 5. Per-model opt-out

Reference tables that should stay shared across all domains:

```yaml
models:
  country_codes:
    domain: false   # no EU/US/APAC copies — all domains ref this one
```

---

## Requirements

- Python 3.10+
- [Poetry](https://python-poetry.org/)
- dbt-core 1.8+ with dbt-databricks adapter
- Databricks workspace with Unity Catalog

---

## Full Documentation

Run `forge guide` to generate a comprehensive step-by-step guide covering every feature, option, and edge case.

Deeper docs live in `docs/`:

| File | What it covers |
|---|---|
| `docs/GUIDE.md` | Full reference guide (all 18 sections) |
| `docs/ROADMAP_REVIEW.md` | Architecture decisions + roadmap |
| `docs/planning/` | Implementation specs + enablement audits |
| `docs/examples/sales_project/` | Single-domain example project |
| `docs/examples/multi_domain_project/` | Multi-domain (EU/US/APAC) example with `domain_sources` |
