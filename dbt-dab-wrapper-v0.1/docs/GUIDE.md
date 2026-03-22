# dbt-forge — Agentic Project Guide

> **Auto-generated**: 22 March 2026
> **Project**: dbt-forge-v0.1
> **Platform**: Databricks (Unity Catalog) with Serverless Compute

This guide teaches you — or an AI agent — exactly how to set up, build, deploy, and migrate this project. No SQL or Python experience needed. Every action is a single command.

---

## Architecture at a Glance

```
forge.yml            ← Project config + naming patterns + profiles (YOU edit)
dbt/models.yml       ← Table definitions (YOU edit)
dbt/migrations/      ← Schema changes (YOU create YAMLs here)
  ↓
forge compile        ← Generates all SQL automatically
  ↓
dbt/models/*.sql     ← Generated SQL (NEVER edit these directly)
dbt/models/schema.yml← Generated tests & docs
sql/*.sql            ← Generated standalone SQL (--pure-sql mode)
  ↓
forge deploy         ← Deploys to Databricks
```

**You only ever touch two files**: `forge.yml` and `dbt/models.yml`.
Everything else is generated.

---

## Table of Contents

1. [HOW-TO: Initial Setup](#1-how-to-initial-setup)
2. [HOW-TO: Build Models (No SQL)](#2-how-to-build-models-no-sql)
3. [HOW-TO: Deploy to Databricks](#3-how-to-deploy-to-databricks)
4. [HOW-TO: Migrations (Schema Changes)](#4-how-to-migrations-schema-changes)
5. [HOW-TO: Dev Mode (Isolated Development)](#5-how-to-dev-mode-isolated-development)
6. [HOW-TO: Define UDFs (Reusable Functions)](#6-how-to-define-udfs-reusable-functions)
7. [HOW-TO: Data Quality Checks](#7-how-to-data-quality-checks)
8. [HOW-TO: View Pipeline DAG](#8-how-to-view-pipeline-dag)
9. [HOW-TO: Generate Type-Safe Python SDK](#9-how-to-generate-type-safe-python-sdk)
10. [HOW-TO: Explain Provenance (Column-Level Lineage)](#10-how-to-explain-provenance-column-level-lineage)
11. [HOW-TO: Diff Changes](#11-how-to-diff-changes)
12. [HOW-TO: Teardown](#12-how-to-teardown)
13. [HOW-TO: Multi-Environment Profiles](#13-how-to-multi-environment-profiles)
14. [HOW-TO: Naming Patterns (Catalogs & Schemas)](#14-how-to-naming-patterns-catalogs--schemas)
15. [HOW-TO: SQL-Only Mode (No dbt at Runtime)](#15-how-to-sql-only-mode-no-dbt-at-runtime)
16. [Reference: Column Options](#16-reference-column-options)
17. [Reference: Model Options](#17-reference-model-options)
18. [Reference: Migration Actions](#18-reference-migration-actions)
19. [Reference: forge.yml Options](#19-reference-forgeyml-options)
20. [Reference: Project Structure](#20-reference-project-structure)
21. [Reference: Environment Variables & Authentication](#21-reference-environment-variables--authentication)
22. [Troubleshooting](#22-troubleshooting)

---

## 1. HOW-TO: Initial Setup

### Prerequisites

- Python 3.10+
- Poetry (`pip install poetry`)
- Databricks workspace with Unity Catalog
- A Databricks SQL Warehouse or cluster

### Step 1: Install dependencies

```bash
cd dbt-forge-v0.1
poetry install
```

This installs: `typer`, `pyyaml`, `databricks-sdk`, `dbt-databricks`, and all dev tools.

### Step 2: Connect to Databricks

**Recommended**: Use the Databricks CLI config file (`~/.databrickscfg`):

```bash
databricks configure --profile DEFAULT
```

This writes your host and token to `~/.databrickscfg`. Then reference it in `forge.yml`:

```yaml
profiles:
  dev:
    databricks_profile: DEFAULT    # reads ~/.databrickscfg [DEFAULT]
```

**Alternative**: Set environment variables:

```bash
export DBT_DATABRICKS_HOST="your-workspace.cloud.databricks.com"
export DBT_DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
export DBT_DATABRICKS_TOKEN="dapi..."
```

Find `HTTP_PATH` in: **Databricks → SQL Warehouses → your warehouse → Connection details**.

### Step 3: Create project scaffolding

```bash
forge setup --name my_project
```

This creates:
- `forge.yml` — your project config with profiles and naming patterns
- `profiles.yml` — auto-generated dbt connection config
- `dbt/models/` — where compiled SQL goes
- `dbt/seeds/` — CSV seed data files
- `dbt/sources/` — external source definitions
- `artifacts/` — graph exports and contracts

### Step 4: Configure forge.yml

Edit `forge.yml` to match your environment:

```yaml
name: my_project
scope: fd                                # project scope (e.g. fd = foundational_data)
id: myproject                            # project id — used in naming patterns
active_profile: dev

# Naming patterns — how catalog and schema names are constructed
catalog_pattern: "{env}_{scope}_{catalog}"   # → dev_fd_bronze, prd_fd_meta
schema_pattern:  "{env}_{id}"                # → dev_myproject (prd → myproject)
skip_env_prefix: [prd, prod]                 # schemas drop {env}_ for these envs

# Logical names — actual Databricks names are built via patterns
catalogs: [bronze, silver, meta, operations]
schemas:  [bronze, silver, gold]

profiles:
  dev:
    platform: databricks
    databricks_profile: DEFAULT
    env: dev
    catalog: bronze
    compute: { type: serverless, auto_scale: true }
  prod:
    databricks_profile: PROD
    env: prd
    catalog: bronze
    schedule: "0 0 6 * * ?"
  local:
    platform: postgres
    connection: { host: localhost, port: 5432, database: dev_warehouse, user: postgres }
    catalog: public
    schemas: { bronze: bronze, silver: silver, gold: gold }
    catalogs: { bronze: public, silver: public, meta: public, operations: public }

dbt:
  version: 1.8.0
features:
  graph: true
  quarantine: true
  validation_exceptions: true
  prior_version: true
  python_udfs: true
portability:
  postgres_compatible: true
```

### Step 5: Verify profiles

```bash
forge profiles                  # list all profiles + expanded catalog/schema names
forge profiles --show-connection # show resolved connection details
forge profiles --generate       # auto-generate profiles.yml from forge.yml
```

### Step 6: Add seed data (optional)

Place CSV files in `dbt/seeds/`:

```
dbt/seeds/raw_customers.csv
dbt/seeds/raw_orders.csv
```

Then run `dbt seed --project-dir .` to load them.

---

## 2. HOW-TO: Build Models (No SQL)

### The models.yml file

Everything is defined in `dbt/models.yml`. You describe your tables in YAML — the compiler generates all SQL.

### Pattern A: Simple staging model

Read from a source/seed, optionally cast types:

```yaml
models:
  stg_customers:
    description: "Staged customer data"
    source: raw_customers                     # ← reads from ref('raw_customers')
    columns:
      customer_id:  { type: int, required: true, unique: true }
      email:        { type: string }
      signup_date:  { type: date, cast: true }   # ← auto-CAST
      revenue:      { type: "decimal(10,2)", cast: true }
```

### Pattern B: Computed columns

Use `expr` for calculations — no SQL knowledge needed, just write the formula:

```yaml
  stg_orders:
    source: raw_orders
    columns:
      order_id:    { type: int, required: true, unique: true }
      quantity:    { type: int }
      unit_price:  { type: "decimal(10,2)", cast: true }
      line_total:  { expr: "quantity * unit_price" }    # ← computed
      order_date:  { type: date, cast: true }
```

### Pattern C: Data cleaning with quarantine

Bad rows automatically move to a `_quarantine` table:

```yaml
  customer_clean:
    description: "Clean customers"
    source: stg_customers
    materialized: table
    version: v2
    quarantine: "email IS NULL OR revenue < 0"  # ← bad rows quarantined
    columns:
      customer_id:  { type: int, required: true, unique: true }
      email:        { type: string, required: true }
      revenue:      { type: "decimal(10,2)" }
```

### Pattern D: Joining two tables

Use `sources` (plural) with aliases, plus a `join` condition:

```yaml
  customer_orders:
    description: "Customers with their orders"
    sources:                        # ← plural = join
      c: customer_clean             # alias 'c'
      o: stg_orders                 # alias 'o'
    join: "c.customer_id = o.customer_id"
    materialized: table
    columns:
      customer_id:  { from: c }     # ← which alias
      email:        { from: c }
      order_id:     { from: o, required: true, unique: true }
      product:      { from: o }
      line_total:   { from: o }
```

### Pattern E: Aggregations (summary/gold layer)

Use `group_by` plus `expr` for aggregate functions:

```yaml
  customer_summary:
    description: "Customer metrics"
    source: customer_orders
    materialized: table
    group_by: [customer_id, email]              # ← GROUP BY
    columns:
      customer_id:    { type: int, required: true, unique: true }
      email:          { type: string }
      total_orders:   { expr: "count(order_id)", type: int }        # ← COUNT
      total_revenue:  { expr: "sum(line_total)", type: "decimal(10,2)" }  # ← SUM
      first_order:    { expr: "min(order_date)", type: date }       # ← MIN
      last_order:     { expr: "max(order_date)", type: date }       # ← MAX
```

### Pattern F: Using UDFs (reusable functions)

Define a function once, call it in any model:

```yaml
udfs:
  loyalty_tier:
    language: sql
    returns: string
    params:
      - { name: revenue, type: "decimal(18,2)" }
    body: |
      CASE
        WHEN revenue >= 1000 THEN 'GOLD'
        WHEN revenue >=  500 THEN 'SILVER'
        ELSE 'BRONZE'
      END

models:
  customer_summary:
    columns:
      tier: { udf: "loyalty_tier(total_revenue)", description: "Loyalty tier" }
```

See [HOW-TO: Define UDFs](#6-how-to-define-udfs-reusable-functions) for full details.

### Compile YAML → SQL

```bash
forge compile
```

Output:
```
  ✅ stg_customers.sql → dbt/models/stg_customers.sql
  ✅ stg_orders.sql → dbt/models/stg_orders.sql
  ✅ customer_clean.sql → dbt/models/customer_clean.sql
  ✅ customer_orders.sql → dbt/models/customer_orders.sql
  ✅ customer_summary.sql → dbt/models/customer_summary.sql
  📋 schema.yml → dbt/models/schema.yml
  🔧 _udfs.sql → dbt/models/_udfs.sql
🎉 Compiled 5 models + UDFs from dbt/models.yml. Run 'forge deploy' next.
```

### What the compiler generates automatically

For every model, the compiler produces:
- **SQL file**: Complete `SELECT` with `config()`, `ref()`, `CAST()`, `GROUP BY`, `JOIN`, quarantine post-hooks
- **Lineage**: `{{ dbt_dab_tools.lineage_columns() }}` injected into every model with verbose tracking for expressions, casts, and aggregations
- **schema.yml**: Column definitions, `not_null`/`unique` tests, data types, descriptions

> **Rule**: Never edit the generated `.sql` files directly. Always edit `dbt/models.yml` and recompile.

---

## 3. HOW-TO: Deploy to Databricks

### Quick deploy

```bash
forge deploy
```

This:
1. Reads `forge.yml` for configuration
2. Resolves the active profile and connection (databrickscfg or env vars)
3. Generates the Databricks Asset Bundle
4. Runs `dbt run` with all models

### Deploy to a specific profile

```bash
forge deploy --profile prod     # or -p prod
forge deploy -p int
```

### Manual dbt commands (if needed)

```bash
# Run only one model
dbt run --project-dir . --select stg_customers

# Run with seeds
dbt seed --project-dir . && dbt run --project-dir .

# Test after deploy
dbt test --project-dir .

# Full refresh (rebuild tables from scratch)
dbt run --project-dir . --full-refresh
```

### Deploy workflow

The standard workflow is:

```
Edit dbt/models.yml
  ↓
forge compile        # YAML → SQL (or --pure-sql for standalone SQL)
  ↓
forge deploy -p dev  # SQL → Databricks (dev profile)
  ↓
forge diff           # See what changed
```

---

## 4. HOW-TO: Migrations (Schema Changes)

Migrations are YAML files — no SQL, no ALTER TABLE statements.

### Step 1: Create a migration file

Create a numbered YAML file in `dbt/migrations/`:

```yaml
# dbt/migrations/001_add_loyalty_tier.yml
migration: "001_add_loyalty_tier"
description: "Add loyalty tier column to customer summary"

changes:
  - model: customer_summary
    add_columns:
      loyalty_tier:
        type: string
        expr: "case when sum(line_total) > 1000 then 'gold' when sum(line_total) > 500 then 'silver' else 'bronze' end"
        description: "Customer loyalty tier based on total revenue"
```

### Step 2: Apply the migration

```bash
forge migrate
```

Output:
```
  📦 001_add_loyalty_tier: Add loyalty tier column to customer summary
    → customer_summary: added column 'loyalty_tier'
🔄 Recompiled SQL from updated dbt/models.yml
🎉 Applied 1 migration(s). Run 'forge deploy' next.
```

This does three things:
1. Updates `dbt/models.yml` with the new column
2. Recompiles all SQL files
3. Records the migration as applied (won't run again)

### Step 3: Deploy the changes

```bash
forge deploy
```

### Migration examples

#### Add a column

```yaml
changes:
  - model: customer_summary
    add_columns:
      avg_order_value:
        type: "decimal(10,2)"
        expr: "avg(line_total)"
        description: "Average order value"
```

#### Remove a column

```yaml
changes:
  - model: customer_orders
    remove_columns: [legacy_field, temp_flag]
```

#### Rename a column

```yaml
changes:
  - model: customer_clean
    rename_columns:
      revenue: total_revenue
```

#### Change a column's type

```yaml
changes:
  - model: stg_customers
    change_type:
      customer_id: { from: string, to: int, cast: true }
```

#### Update quarantine rules

```yaml
changes:
  - model: customer_clean
    quarantine: "email IS NULL OR revenue < 0 OR country = 'UNKNOWN'"
    version: v3    # bump version when changing rules
```

#### Change materialization

```yaml
changes:
  - model: stg_customers
    materialized: table   # was 'view', now a persisted table
```

#### Multiple changes in one migration

```yaml
migration: "002_q1_2026_updates"
description: "Q1 2026 schema updates"

changes:
  - model: customer_summary
    add_columns:
      churn_risk:
        type: string
        expr: "case when max(order_date) < current_date - interval 90 days then 'high' else 'low' end"

  - model: customer_clean
    quarantine: "email IS NULL OR revenue < 0 OR length(email) < 5"
    version: v3

  - model: stg_orders
    add_columns:
      order_year:
        type: int
        expr: "year(cast(order_date as date))"
```

### Migration numbering

Migrations run in alphabetical order. Use numbered prefixes:

```
dbt/migrations/
  001_add_loyalty_tier.yml
  002_q1_2026_updates.yml
  003_add_churn_risk.yml
```

Applied migrations are tracked in `dbt/migrations/.migrations_applied` — they won't run twice.

### Preview changes without applying (dry run)

```bash
forge migrate --dry-run
```

Shows exactly what would change — without modifying `models.yml` or the applied log.

### Skip recompilation

If you want to apply the migration to `models.yml` without recompiling SQL:

```bash
forge migrate --no-recompile
```

---

## 5. HOW-TO: Dev Mode (Isolated Development)

Dev mode gives you an isolated schema and auto-recompilation on save.

### Step 1: Create dev environment

```bash
forge dev-up
```

This:
1. Creates an isolated schema (`dev_silver_{username}`)
2. Compiles all models
3. Seeds sample data

### Step 2: Start watch mode

```bash
forge dev
```

Watches `dbt/models.yml` for changes — auto-recompiles on save. Press `Ctrl+C` to stop.

```
👀 Watching dbt/models.yml for changes (Ctrl+C to stop)...
   Auto-compiles → dbt/models/

  ✅ Initial compile: 5 models
  🔄 Change detected — recompiling...
  ✅ Compiled 5 models
```

### Step 3: Tear down when done

```bash
forge dev-down
```

Drops the isolated dev schema.

### Custom schema name

```bash
forge dev-up --schema feature_x
forge dev-down --schema feature_x
```

---

## 6. HOW-TO: Define UDFs (Reusable Functions)

UDFs let you write reusable expressions once and call them in any model.

### Step 1: Add a `udfs:` block to models.yml

```yaml
udfs:
  loyalty_tier:
    description: "Assigns GOLD/SILVER/BRONZE based on revenue"
    language: sql
    returns: string
    params:
      - { name: revenue, type: "decimal(18,2)" }
    body: |
      CASE
        WHEN revenue >= 1000 THEN 'GOLD'
        WHEN revenue >=  500 THEN 'SILVER'
        ELSE 'BRONZE'
      END
```

### Step 2: Use it in a column

```yaml
models:
  customer_summary:
    columns:
      tier: { udf: "loyalty_tier(total_revenue)", description: "Loyalty tier" }
```

The compiler generates `loyalty_tier(total_revenue) as tier` in the SELECT.

### Step 3: View defined UDFs

```bash
forge udfs
```

Output:
```
🔧 1 UDF(s) defined:

  🔵 loyalty_tier(revenue: decimal(18,2)) → string  [SQL]

💡 Use in columns: { udf: "loyalty_tier(total_revenue)" }
```

### Write UDF SQL to a file

```bash
forge udfs --output udfs.sql
```

### SQL vs Python UDFs

| Feature | SQL UDF | Python UDF |
|---------|---------|------------|
| Cold start | None | 5–15 seconds |
| Lineage | Full visibility | Opaque |
| Serverless | ✅ | ✅ |
| Use for | Calculations, CASE logic | Regex, ML inference, APIs |

SQL is the default. Use Python only when SQL can't solve the problem.

### UDFs in the graph

UDFs appear as **purple rhombus nodes** in the Mermaid pipeline diagram with edges to every model that calls them.

---

## 7. HOW-TO: Data Quality Checks

Define checks inline in `models.yml` to validate data after every build.

### Add checks to a model

```yaml
  customer_summary:
    columns:
      total_revenue: { expr: "sum(line_total)", type: "decimal(10,2)" }
    checks:
      - name: revenue_non_negative
        type: range
        column: total_revenue
        min: 0
        severity: error
      - name: data_is_fresh
        type: recency
        column: last_order_date
        max_age: "7 days"
        severity: warn
      - name: has_customers
        type: row_count
        min: 1
        severity: error
```

### View checks

```bash
forge validate
```

Output:
```
  📊 customer_summary: 3 check(s)
    🔴 revenue_non_negative [cell] → error
    🟡 data_is_fresh [column] → warn
    🔴 has_customers [table] → error

🎯 3 total check(s) across 1 model(s).
```

### Check for one model only

```bash
forge validate --model customer_summary
```

### Write check SQL to file

```bash
forge validate --output checks.sql
```

### Available check types

| Type | Scope | What it checks |
|------|-------|-----------------|
| `range` | cell | Column value between min and/or max |
| `recency` | column | Most recent value within a time window |
| `row_count` | table | Total rows above a minimum |
| `regex` | cell | Column values match a pattern |
| `custom_sql` | table | Arbitrary SQL condition |

### Severity levels

| Severity | Meaning |
|----------|---------|
| `error` | 🔴 Blocks pipeline — data is wrong |
| `warn` | 🟡 Alert — investigate but don't block |
| `info` | ⚪ Informational only |

---

## 8. HOW-TO: View Pipeline DAG

### Print the task summary

```bash
forge workflow
```

### Generate a Mermaid diagram

```bash
forge workflow --mermaid
```

Paste the output into any Mermaid renderer to visualize your pipeline.

### Generate Databricks Asset Bundle jobs YAML

```bash
forge workflow --dab              # prints to stdout
forge workflow --dab -p prod      # for a specific profile
```

With `--dab`, the workflow is automatically written to `resources/jobs/<pipeline-name>.yml` for use with Databricks Asset Bundles.

### Save to a file

```bash
forge workflow --mermaid --output docs/pipeline.mmd
```

### Pipeline stages

Models are automatically assigned to stages:

| Stage | Name Prefixes | Description |
|-------|--------------|-------------|
| **INGEST** | `raw_`, `src_`, `seed_` | Seeds, sources, raw loads |
| **STAGE** | `stg_`, `staging_` | Staging: light cleaning, type casting |
| **CLEAN** | `clean_` or has `quarantine` | Data quality + quarantine |
| **ENRICH** | `int_`, `fct_`, `dim_` or joins | Business logic, joins |
| **SERVE** | `agg_`, `rpt_`, `pub_` or aggregations | Gold layer, summaries |

---

## 9. HOW-TO: Generate Type-Safe Python SDK

If you need to interact with these tables from Python:

```bash
forge codegen
```

Generates `sdk/models.py` with Pydantic models for every table. Type-safe inserts — wrong types fail at runtime, never in the database.

```python
from sdk.models import StgOrders, CustomerClean

# Pydantic validates types immediately
order = StgOrders(order_id=1, customer_id=42, product="Widget", quantity=5, unit_price=10.0)
order.to_insert_sql()   # → INSERT INTO ...
order.to_spark_row()    # → PySpark Row object
```

### CI mode (fail if stale)

```bash
forge codegen --check
```

---

## 10. HOW-TO: Explain Provenance (Column-Level Lineage)

Trace any column value back to its source — expressions, UDF calls, checks, git commit, version:

```bash
# Full provenance tree in terminal
forge explain customer_summary.total_revenue

# With upstream checks and UDF descriptions
forge explain customer_summary.total_revenue --full

# As Mermaid diagram (purple UDFs, orange checks)
forge explain customer_summary.total_revenue --mermaid

# As JSON (for scripting / GUI integration)
forge explain customer_summary.total_revenue --json

# Write to file
forge explain customer_summary.total_revenue --mermaid --output docs/explain.mmd
```

### Example Output

```
🔎 Explaining customer_summary.total_revenue
├─ Expression: sum(line_total)
├─ Operation: EXPRESSION
├─ Type: decimal(10,2)
├─ Version: v1
├─ From model: customer_orders
├─ Source columns: line_total
├─ Git commit: c65af4f
├─ Generated: 2026-03-22T17:26:02+00:00
└─ Checks (2):
     🔴 revenue_non_negative [range] on total_revenue → error
     🔴 has_customers [row_count] (table) → error

  ⬆ Upstream: customer_orders.line_total
    ⬆ Upstream: stg_orders.line_total
       expr: quantity * unit_price  [EXPRESSION]
```

### UDF Columns

Columns using UDFs show the function call and its metadata:

```
🔎 Explaining customer_summary.tier
├─ Expression: loyalty_tier(total_revenue)
├─ Operation: UDF
├─ UDF: loyalty_tier() → string  [SQL]
│    Assigns GOLD/SILVER/BRONZE based on revenue
└─ Checks (1):
     🔴 has_customers [row_count] (table) → error
```

### Mermaid Provenance

The `--mermaid` flag produces a bottom-up flowchart:
- **Purple nodes** = UDF calls
- **Orange hexagons** = checks
- **Arrows** = data flows upstream → downstream

---

## 11. HOW-TO: Diff Changes

See what changed since the last graph snapshot:

```bash
# Plain English summary
forge diff

# Color-coded Mermaid diagram (green=added, red=removed, orange=modified)
forge diff --mermaid

# Write diff output to file
forge diff --output docs/diff.json
forge diff --mermaid --output docs/diff.mmd
```

The first time you run `forge diff`, it saves a baseline snapshot to `artifacts/graph.json`.
Subsequent runs compare against that snapshot and show:
- Assets added/removed
- Columns added/removed
- Materialization changes
- Quality rules changes
- New/removed lineage edges

---

## 12. HOW-TO: Teardown

Safely destroy everything:

```bash
forge teardown
```

Shows a graph diff of what would be deleted before taking action.

---

## 13. HOW-TO: Multi-Environment Profiles

Forge supports multiple deployment environments from a single `forge.yml`. Each profile defines a platform, connection, and compute config.

### Define profiles

```yaml
profiles:
  dev:
    platform: databricks
    databricks_profile: DEFAULT    # reads ~/.databrickscfg [DEFAULT]
    env: dev
    catalog: bronze
    compute: { type: serverless }
  prod:
    databricks_profile: PROD
    env: prd
    catalog: bronze
    schedule: "0 0 6 * * ?"
  int:
    databricks_profile: INT
    env: int
    catalog: bronze
  local:
    platform: postgres
    connection: { host: localhost, port: 5432, database: dev_warehouse, user: postgres }
    catalog: public
    schemas: { bronze: bronze, silver: silver, gold: gold }
    catalogs: { bronze: public, silver: public, meta: public, operations: public }
```

### Set the active profile

```yaml
active_profile: dev
```

Or override per-command:

```bash
forge deploy --profile prod
forge compile --pure-sql -p int
```

### Connection resolution order

1. `databricks_profile:` → reads `~/.databrickscfg` (recommended)
2. `connection:` block → inline host/token/http_path
3. Environment variables → `DBT_DATABRICKS_HOST`, `DBT_DATABRICKS_TOKEN`

### View and manage profiles

```bash
forge profiles                    # list all profiles + expanded names
forge profiles -p prod            # show details for one profile
forge profiles --show-connection  # show resolved host/token for all
forge profiles --generate         # auto-generate profiles.yml with dbt vars
```

### Databrickscfg setup

```bash
# Create profiles in ~/.databrickscfg
databricks configure --profile DEFAULT    # dev workspace
databricks configure --profile PROD       # prod workspace
databricks configure --profile INT        # integration workspace
```

The config file looks like:

```ini
[DEFAULT]
host  = https://dev.cloud.databricks.com
token = dapiXXX

[PROD]
host  = https://prod.cloud.databricks.com
token = dapiYYY
```

---

## 14. HOW-TO: Naming Patterns (Catalogs & Schemas)

Forge uses configurable patterns to construct Databricks catalog and schema names across environments. Change the pattern once — all environments update automatically.

### Available tokens

| Token | Source | Example |
|-------|--------|---------|
| `{env}` | Profile's `env:` key | `dev`, `int`, `prd` |
| `{id}` | Top-level `id:` | `myproject` |
| `{scope}` | Top-level `scope:` | `fd` (foundational_data) |
| `{user}` | Current OS / Databricks user | `ben` |
| `{catalog}` | Logical catalog name | `bronze`, `meta` |
| `{schema}` | Logical schema name | `bronze`, `silver` |

### Define patterns

```yaml
catalog_pattern: "{env}_{scope}_{catalog}"     # → dev_fd_bronze, prd_fd_meta
schema_pattern:  "{env}_{id}"                  # → dev_myproject
skip_env_prefix: [prd, prod]                   # schemas drop {env}_ for these envs
```

### Define logical names

```yaml
catalogs: [bronze, silver, meta, operations]
schemas:  [bronze, silver, gold]
```

### How expansion works

**Catalogs** always keep the `{env}_` prefix across all environments:

| Logical | `env: dev` | `env: int` | `env: prd` |
|---|---|---|---|
| bronze | `dev_fd_bronze` | `int_fd_bronze` | `prd_fd_bronze` |
| silver | `dev_fd_silver` | `int_fd_silver` | `prd_fd_silver` |
| meta | `dev_fd_meta` | `int_fd_meta` | `prd_fd_meta` |
| operations | `dev_fd_operations` | `int_fd_operations` | `prd_fd_operations` |

**Schemas** drop `{env}_` for production environments (controlled by `skip_env_prefix`):

| `env: dev` | `env: int` | `env: prd` |
|---|---|---|
| `dev_myproject` | `int_myproject` | `myproject` |

### Medallion layer routing

Models auto-route to the correct schema and catalog based on their name prefix:

| Prefix | Layer |
|---|---|
| `raw_`, `src_`, `seed_` | bronze |
| `stg_`, `staging_`, `clean_`, `int_` | silver |
| `fct_`, `dim_`, `agg_`, `rpt_`, `pub_` | gold |

### dbt variables

Running `forge profiles --generate` injects schema/catalog variables into `profiles.yml`:

```yaml
vars:
  schema_bronze: dev_myproject
  schema_silver: dev_myproject
  schema_gold:   dev_myproject
  catalog_bronze: dev_fd_bronze
  catalog_silver: dev_fd_silver
  catalog_meta:   dev_fd_meta
  catalog_operations: dev_fd_operations
```

Use in dbt Jinja: `{{ var('catalog_silver') }}`, `{{ var('schema_bronze') }}`.

### Explicit overrides

For non-patterned environments (e.g. local Postgres), spell out names explicitly:

```yaml
  local:
    platform: postgres
    schemas: { bronze: bronze, silver: silver, gold: gold }
    catalogs: { bronze: public, silver: public, meta: public, operations: public }
```

---

## 15. HOW-TO: SQL-Only Mode (No dbt at Runtime)

Compile your models to standalone SQL that runs directly on a Databricks SQL warehouse — no dbt installation, no Python, no cold starts.

### Compile

```bash
forge compile --pure-sql           # uses active profile
forge compile --pure-sql -p prod   # uses prod profile (prd_fd_bronze catalogs)
forge compile --pure-sql -p dev    # uses dev profile (dev_fd_bronze catalogs)
```

### Output

Files are written to `sql/`, numbered for execution order (topological sort):

```
sql/000_udfs.sql              ← CREATE FUNCTION statements
sql/001_stg_customers.sql     ← CREATE VIEW AS SELECT ...
sql/002_customer_clean.sql
sql/002_customer_clean_quarantine.sql  ← quarantine sidecar
sql/003_stg_orders.sql
sql/004_customer_orders.sql
sql/005_customer_summary.sql
```

Each file is self-contained:
- Fully qualified table references (`catalog.schema.table`)
- Inline `_lineage` STRUCT (model name, sources, git commit, deploy timestamp)
- Quarantine sidecar tables generated automatically
- UDFs compiled as `CREATE OR REPLACE FUNCTION`

### Execution

Run the files in order on any SQL warehouse:

```bash
# Databricks SQL warehouse
for f in sql/*.sql; do databricks sql execute --file "$f"; done

# Or paste into Databricks SQL editor in numbered order
```

---

## 16. Reference: Column Options

| Option | Type | Example | What it does |
|--------|------|---------|-------------|
| `type` | string | `int`, `string`, `date`, `"decimal(10,2)"` | Column data type |
| `required` | bool | `true` | Adds `not_null` test — column cannot be NULL |
| `unique` | bool | `true` | Adds `unique` test — no duplicate values |
| `cast` | bool | `true` | Wraps in `CAST(col AS type)` |
| `expr` | string | `"count(order_id)"` | Custom SQL expression |
| `from` | string | `c` | Which join alias this column comes from |
| `source_column` | string | `old_name` | Upstream column name (if different from output name) |
| `description` | string | `"Customer email"` | Human-readable docs |
| `accepted_values` | list | `["gold", "silver", "bronze"]` | Only allow these values (adds test) |
| `udf` | string | `"loyalty_tier(total_revenue)"` | Call a UDF defined in the `udfs:` block |

---

## 17. Reference: Model Options

| Option | Type | Example | What it does |
|--------|------|---------|-------------|
| `description` | string | `"Clean customers"` | Model documentation |
| `source` | string | `raw_customers` | Single upstream model (`ref()`) |
| `sources` | map | `{c: customer_clean, o: stg_orders}` | Multiple sources with aliases for joins |
| `join` | string | `"c.customer_id = o.customer_id"` | Join condition |
| `join_type` | string | `"left join"` | Join type (default: `inner join`) |
| `materialized` | string | `table` or `view` | How dbt materializes (default: `view`) |
| `version` | string | `v2` | Methodology version (tracked in lineage) |
| `quarantine` | string | `"email IS NULL"` | SQL condition — matching rows quarantined |
| `group_by` | list | `[customer_id, email]` | GROUP BY columns (triggers aggregation mode) |
| `checks` | list | See [Data Quality Checks](#7-how-to-data-quality-checks) | Inline data quality checks |

---

## 18. Reference: Migration Actions

| Action | Format | What it does |
|--------|--------|-------------|
| `add_columns` | `{col_name: {type, expr, description}}` | Add new columns |
| `remove_columns` | `[col1, col2]` | Remove columns by name |
| `rename_columns` | `{old_name: new_name}` | Rename columns |
| `change_type` | `{col: {from: old, to: new, cast: true}}` | Change column type |
| `quarantine` | `"condition"` | Update the quarantine SQL condition |
| `version` | `v3` | Bump the methodology version |
| `materialized` | `table` or `view` | Change materialization strategy |

---

## 19. Reference: forge.yml Options

| Key | Type | Example | Description |
|-----|------|---------|-------------|
| `name` | string | `my_project` | Project name |
| `scope` | string | `fd` | Project scope — used as `{scope}` in patterns |
| `id` | string | `myproject` | Project identifier — used as `{id}` in patterns |
| `active_profile` | string | `dev` | Default profile when no `--profile` flag |
| `catalog_pattern` | string | `"{env}_{scope}_{catalog}"` | Pattern for Databricks catalog names |
| `schema_pattern` | string | `"{env}_{id}"` | Pattern for schema names |
| `skip_env_prefix` | list | `[prd, prod]` | Envs that drop `{env}_` from schemas |
| `catalogs` | list | `[bronze, silver, meta, operations]` | Logical catalog names |
| `schemas` | list | `[bronze, silver, gold]` | Logical schema names |
| `profiles` | map | See section 13 | Named environment profiles |
| `dbt.version` | string | `1.8.0` | Required dbt version |
| `features.*` | bool | `true` | Feature flags (graph, quarantine, prior_version, python_udfs) |
| `portability.postgres_compatible` | bool | `true` | Avoid Databricks-only SQL |

### Profile options

| Key | Type | Example | Description |
|-----|------|---------|-------------|
| `platform` | string | `databricks` | Target platform (`databricks`, `postgres`, `redshift`) |
| `databricks_profile` | string | `DEFAULT` | `~/.databrickscfg` profile name |
| `env` | string | `dev` | Environment tag — used as `{env}` in patterns |
| `catalog` | string | `bronze` | Default logical catalog for this profile |
| `compute.type` | string | `serverless` | Compute type |
| `schedule` | string | `"0 0 6 * * ?"` | Cron schedule for DAB workflow |
| `connection` | map | `{ host, port, database, user }` | Inline connection for non-Databricks |
| `schemas` | map | `{ bronze: bronze }` | Explicit schema mapping (overrides pattern) |
| `catalogs` | map | `{ bronze: public }` | Explicit catalog mapping (overrides pattern) |

---

## 20. Reference: Project Structure

```
dbt-forge-v0.1/
│
├── forge.yml               ← YOU EDIT: project config + profiles + naming patterns
├── pyproject.toml             ← Poetry: dependencies + 'forge' CLI command
├── profiles.yml               ← Auto-generated dbt connection config
├── dbt_project.yml            ← dbt config (auto-managed by forge)
├── packages.yml               ← dbt packages (points to dbt-dab-tools)
│
├── dbt/
│   ├── models.yml             ← YOU EDIT: table definitions + UDFs in YAML
│   ├── models/                ← GENERATED: .sql files (never edit)
│   │   ├── stg_customers.sql
│   │   ├── stg_orders.sql
│   │   ├── customer_clean.sql
│   │   ├── customer_orders.sql
│   │   ├── customer_summary.sql
│   │   ├── schema.yml
│   │   └── _udfs.sql          ← GENERATED: CREATE FUNCTION statements
│   ├── seeds/                 ← CSV source data
│   │   ├── raw_customers.csv
│   │   └── raw_orders.csv
│   ├── migrations/            ← YOU CREATE: schema change YAMLs
│   │   └── 001_add_loyalty_tier.yml
│   └── sources/               ← External source definitions
│
├── sql/                       ← GENERATED: standalone SQL (--pure-sql mode)
│   ├── 000_udfs.sql
│   ├── 001_stg_customers.sql
│   └── ...
│
├── resources/
│   └── jobs/                  ← GENERATED: DAB workflow YAML (--dab mode)
│
├── src/forge/               ← Python engine (don't touch)
│   ├── cli.py                 ← CLI commands
│   ├── graph.py               ← Lineage graph engine + ODCS contracts
│   ├── simple_ddl.py          ← YAML compiler + migration + UDF + checks engine
│   ├── type_safe.py           ← Pydantic model generator
│   ├── workflow.py            ← Databricks Workflow DAG generator
│   └── compute_resolver.py   ← Profile + connection + naming-pattern resolver
│
├── artifacts/                 ← Graph exports, contracts
├── docs/                      ← Guides, roadmap, planning specs
└── target/                    ← dbt build artifacts
```

### External: dbt-dab-tools (macro package)

```
../dbt-dab-tools/
└── macros/
    ├── lineage.sql          ← _lineage STRUCT column (ODCS v3)
    ├── quarantine.sql       ← Quarantine post-hook
    ├── prior_version.sql    ← Safe rollback to _v_previous
    └── python_udf.sql       ← Python UDF creator
```

---

## 21. Reference: Environment Variables & Authentication

### Authentication priority

Forge resolves connections in this order:

1. **`databricks_profile:`** in `forge.yml` → reads `~/.databrickscfg` (recommended)
2. **`connection:`** block in `forge.yml` → inline host/token
3. **Environment variables** → fallback

### Environment variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DBT_DATABRICKS_HOST` | Only if no databrickscfg | — | Databricks workspace URL |
| `DBT_DATABRICKS_HTTP_PATH` | Only if no databrickscfg | — | SQL warehouse HTTP path |
| `DBT_DATABRICKS_TOKEN` | Only if no databrickscfg | — | Personal access token or OAuth |
| `DBT_DATABRICKS_CATALOG` | No | Profile's catalog | Unity Catalog name |
| `DBT_PG_USER` | For Postgres | `postgres` | Postgres username |
| `DBT_PG_PASSWORD` | For Postgres | — | Postgres password |

### Recommended setup

```bash
# One-time setup — creates ~/.databrickscfg
databricks configure --profile DEFAULT    # dev
databricks configure --profile PROD       # prod
```

Then in `forge.yml`:

```yaml
profiles:
  dev:
    databricks_profile: DEFAULT   # zero env vars needed
  prod:
    databricks_profile: PROD
```

---

## 22. Troubleshooting

| Problem | Solution |
|---------|----------|
| `forge: command not found` | Run `poetry install` then `poetry shell` or prefix commands with `poetry run forge` |
| `forge.yml not found` | Run `forge setup` from the project root directory |
| `models.yml not found` | Create `dbt/models.yml` with your model definitions |
| Models not updating after edit | Run `forge compile` — you must recompile after editing models.yml |
| `ModuleNotFoundError: typer` | Run `poetry install` to install all dependencies |
| Connection refused to Databricks | Check `databricks_profile:` in forge.yml, or run `databricks configure --profile DEFAULT` |
| `Profile [X] not found in ~/.databrickscfg` | Run `databricks configure --profile X` to create it |
| Wrong catalog/schema names | Check `forge profiles` to see expanded names. Verify `catalog_pattern` / `schema_pattern` in forge.yml |
| Bad data in table | Add a `quarantine: "condition"` to the model in models.yml |
| Need to rename a column | Create a migration YAML in `dbt/migrations/` |
| Want to see the full pipeline | Run `forge workflow --mermaid` |
| Pure SQL not using right env | Pass `--profile`: `forge compile --pure-sql -p prod` |
| Python type errors | Run `forge codegen` to regenerate the SDK |
| Migration ran twice | Check `dbt/migrations/.migrations_applied` — remove the entry to re-run |
| Schema mismatch after struct change | Run `forge deploy --full-refresh` with `dbt run --full-refresh` |
| `{user}` token resolves wrong | Check `whoami` — forge uses the OS username, sanitised for catalog names |

---

## All Commands Summary

| Command | What it does |
|---------|-------------|
| `forge setup` | Creates project structure + forge.yml |
| `forge compile` | Compiles models.yml → SQL + schema.yml + UDFs |
| `forge compile --pure-sql` | Compiles to standalone SQL (no dbt at runtime) |
| `forge compile --pure-sql -p prod` | Pure SQL for a specific profile |
| `forge deploy` | Builds + deploys to Databricks (active profile) |
| `forge deploy -p prod` | Deploys to a specific profile |
| `forge diff` | Shows what changed since last deploy |
| `forge diff --mermaid` | Color-coded Mermaid diff diagram |
| `forge diff --output diff.json` | Writes full diff JSON to file |
| `forge explain model.col` | Full provenance tree for any column |
| `forge explain model.col --full` | Provenance with upstream checks |
| `forge explain model.col --mermaid` | Provenance as Mermaid diagram |
| `forge explain model.col --json` | Provenance as JSON for scripting |
| `forge migrate` | Applies migration YAMLs to models.yml |
| `forge migrate --dry-run` | Preview migration changes without applying |
| `forge migrate --no-recompile` | Applies migrations without recompiling |
| `forge dev-up` | Creates isolated dev schema + seeds data |
| `forge dev` | Watch mode: auto-recompile on save |
| `forge dev-down` | Tears down isolated dev schema |
| `forge udfs` | Shows defined UDFs |
| `forge udfs --output udfs.sql` | Writes CREATE FUNCTION SQL to file |
| `forge validate` | Shows data quality checks for all models |
| `forge validate --model X` | Shows checks for one model |
| `forge validate --output checks.sql` | Writes check SQL to file |
| `forge workflow` | Prints the pipeline DAG |
| `forge workflow --mermaid` | Visual Mermaid diagram |
| `forge workflow --dab` | Databricks Asset Bundle jobs YAML → resources/jobs/ |
| `forge workflow --dab -p prod` | DAB workflow for a specific profile |
| `forge codegen` | Generates type-safe Python SDK |
| `forge codegen --check` | CI mode: fail if SDK is stale |
| `forge profiles` | List all profiles + expanded catalog/schema names |
| `forge profiles --generate` | Auto-generate profiles.yml with dbt vars |
| `forge profiles --show-connection` | Show resolved connection details |
| `forge profiles -p prod` | Show details for one profile |
| `forge guide` | Regenerates this guide |
| `forge teardown` | Safely destroys everything |

---

## Built-In Features (Automatic)

These work automatically when enabled in `forge.yml` — no setup needed:

- **Lineage tracking**: Every row carries `_lineage` metadata (model, sources, git commit, deploy timestamp, methodology version). Column-level tracking for expressions, casts, UDFs, and aggregations.
- **Quarantine**: Add `quarantine: "condition"` to a model → bad rows move to `{model}_quarantine` table.
- **Prior Version**: Every table gets a `_v_previous` snapshot for safe rollback.
- **SQL & Python UDFs**: Define reusable functions in the `udfs:` block of `models.yml`. SQL-first, Python when needed. UDFs appear as purple nodes in the lineage graph.
- **Data Quality Checks**: Add inline `checks:` to any model — range, recency, row_count, regex, or custom SQL. View with `forge validate`.
- **Dev Mode**: `forge dev-up` → isolated schema, `forge dev` → auto-recompile on save, `forge dev-down` → teardown. Zero extra dependencies.
- **Dry-Run Migrations**: `forge migrate --dry-run` previews changes without touching files.
- **Provenance Explain**: `forge explain model.column` traces any value through expressions, UDFs, checks, and git commits — all the way to the raw source. Terminal tree, Mermaid, or JSON output.
- **Graph Diff**: `forge diff` compares graph snapshots — shows added/removed/modified assets, columns, and lineage edges. Color-coded Mermaid with `--mermaid`.
- **Check Nodes in Graph**: Data quality checks appear as orange hexagons in the graph. Diff detects when checks are added or removed.
- **ODCS Contracts**: Every model is an Open Data Contract Standard node. Every dependency is a lineage edge.
- **Workflow DAG**: Pipeline automatically splits into INGEST → STAGE → CLEAN → ENRICH → SERVE stages.
