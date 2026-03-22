# dbt-dab-wrapper — Agentic Project Guide

> **Auto-generated**: 22 March 2026
> **Project**: dbt-dab-wrapper-v0.1
> **Platform**: Databricks (Unity Catalog) with Serverless Compute

This guide teaches you — or an AI agent — exactly how to set up, build, deploy, and migrate this project. No SQL or Python experience needed. Every action is a single command.

---

## Architecture at a Glance

```
wrapper.yml          ← Project config (YOU edit this)
dbt/models.yml       ← Table definitions (YOU edit this)
dbt/migrations/      ← Schema changes (YOU create YAMLs here)
  ↓
wrapper compile      ← Generates all SQL automatically
  ↓
dbt/models/*.sql     ← Generated SQL (NEVER edit these directly)
dbt/models/schema.yml← Generated tests & docs
  ↓
wrapper deploy       ← Deploys to Databricks
```

**You only ever touch two files**: `wrapper.yml` and `dbt/models.yml`.
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
13. [Reference: Column Options](#13-reference-column-options)
14. [Reference: Model Options](#14-reference-model-options)
15. [Reference: Migration Actions](#15-reference-migration-actions)
16. [Reference: Project Structure](#16-reference-project-structure)
17. [Reference: Environment Variables](#17-reference-environment-variables)
18. [Troubleshooting](#18-troubleshooting)

---

## 1. HOW-TO: Initial Setup

### Prerequisites

- Python 3.10+
- Poetry (`pip install poetry`)
- Databricks workspace with Unity Catalog
- A Databricks SQL Warehouse or cluster

### Step 1: Install dependencies

```bash
cd dbt-dab-wrapper-v0.1
poetry install
```

This installs: `typer`, `pyyaml`, `databricks-sdk`, `dbt-databricks`, and all dev tools.

### Step 2: Set environment variables

```bash
export DBT_DATABRICKS_HOST="your-workspace.cloud.databricks.com"
export DBT_DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
export DBT_DATABRICKS_TOKEN="dapi..."
export DBT_DATABRICKS_CATALOG="main"   # optional, defaults to 'workspace'
```

Find `HTTP_PATH` in: **Databricks → SQL Warehouses → your warehouse → Connection details**.

### Step 3: Create project scaffolding

```bash
wrapper setup --name my_project
```

This creates:
- `wrapper.yml` — your project config
- `dbt/models/` — where compiled SQL goes
- `dbt/seeds/` — CSV seed data files
- `dbt/sources/` — external source definitions
- `artifacts/` — graph exports and contracts

### Step 4: Configure wrapper.yml

Edit `wrapper.yml` to match your environment:

```yaml
name: my_project
environment: dev
target_platform: databricks
catalog: main
schema: silver
compute:
  type: serverless
  auto_scale: true
dbt:
  version: 1.8.0
features:
  graph: true           # lineage graph + ODCS contracts
  quarantine: true       # auto-quarantine bad rows
  validation_exceptions: true
  prior_version: true    # safe rollback snapshots
  python_udfs: true      # Python UDF support
portability:
  avoid_databricks_only: true
  postgres_compatible: true
schedule: "0 0 6 * * ?"  # Daily at 06:00 UTC (cron)
```

### Step 5: Add seed data (optional)

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
wrapper compile
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
🎉 Compiled 5 models + UDFs from dbt/models.yml. Run 'wrapper deploy' next.
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
wrapper deploy
```

This:
1. Reads `wrapper.yml` for configuration
2. Resolves compute (serverless/dedicated)
3. Generates the Databricks Asset Bundle
4. Runs `dbt run` with all models

### Deploy to a specific environment

```bash
wrapper deploy --env prod
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
wrapper compile        # YAML → SQL
  ↓
wrapper deploy         # SQL → Databricks
  ↓
wrapper diff           # See what changed
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
wrapper migrate
```

Output:
```
  📦 001_add_loyalty_tier: Add loyalty tier column to customer summary
    → customer_summary: added column 'loyalty_tier'
🔄 Recompiled SQL from updated dbt/models.yml
🎉 Applied 1 migration(s). Run 'wrapper deploy' next.
```

This does three things:
1. Updates `dbt/models.yml` with the new column
2. Recompiles all SQL files
3. Records the migration as applied (won't run again)

### Step 3: Deploy the changes

```bash
wrapper deploy
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
wrapper migrate --dry-run
```

Shows exactly what would change — without modifying `models.yml` or the applied log.

### Skip recompilation

If you want to apply the migration to `models.yml` without recompiling SQL:

```bash
wrapper migrate --no-recompile
```

---

## 5. HOW-TO: Dev Mode (Isolated Development)

Dev mode gives you an isolated schema and auto-recompilation on save.

### Step 1: Create dev environment

```bash
wrapper dev-up
```

This:
1. Creates an isolated schema (`dev_silver_{username}`)
2. Compiles all models
3. Seeds sample data

### Step 2: Start watch mode

```bash
wrapper dev
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
wrapper dev-down
```

Drops the isolated dev schema.

### Custom schema name

```bash
wrapper dev-up --schema feature_x
wrapper dev-down --schema feature_x
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
wrapper udfs
```

Output:
```
🔧 1 UDF(s) defined:

  🔵 loyalty_tier(revenue: decimal(18,2)) → string  [SQL]

💡 Use in columns: { udf: "loyalty_tier(total_revenue)" }
```

### Write UDF SQL to a file

```bash
wrapper udfs --output udfs.sql
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
wrapper validate
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
wrapper validate --model customer_summary
```

### Write check SQL to file

```bash
wrapper validate --output checks.sql
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
wrapper workflow
```

### Generate a Mermaid diagram

```bash
wrapper workflow --mermaid
```

Paste the output into any Mermaid renderer to visualize your pipeline.

### Generate Databricks Asset Bundle jobs section

```bash
wrapper workflow --dab
```

### Save to a file

```bash
wrapper workflow --mermaid --output docs/pipeline.mmd
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
wrapper codegen
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
wrapper codegen --check
```

---

## 10. HOW-TO: Explain Provenance (Column-Level Lineage)

Trace any column value back to its source — expressions, UDF calls, checks, git commit, version:

```bash
# Full provenance tree in terminal
wrapper explain customer_summary.total_revenue

# With upstream checks and UDF descriptions
wrapper explain customer_summary.total_revenue --full

# As Mermaid diagram (purple UDFs, orange checks)
wrapper explain customer_summary.total_revenue --mermaid

# As JSON (for scripting / GUI integration)
wrapper explain customer_summary.total_revenue --json

# Write to file
wrapper explain customer_summary.total_revenue --mermaid --output docs/explain.mmd
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
wrapper diff

# Color-coded Mermaid diagram (green=added, red=removed, orange=modified)
wrapper diff --mermaid

# Write diff output to file
wrapper diff --output docs/diff.json
wrapper diff --mermaid --output docs/diff.mmd
```

The first time you run `wrapper diff`, it saves a baseline snapshot to `artifacts/graph.json`.
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
wrapper teardown
```

Shows a graph diff of what would be deleted before taking action.

---

## 13. Reference: Column Options

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

## 14. Reference: Model Options

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

## 15. Reference: Migration Actions

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

## 16. Reference: Project Structure

```
dbt-dab-wrapper-v0.1/
│
├── wrapper.yml               ← YOU EDIT: project config
├── pyproject.toml             ← Poetry: dependencies + 'wrapper' CLI command
├── profiles.yml               ← Databricks connection (or ~/.dbt/profiles.yml)
├── dbt_project.yml            ← dbt config (auto-managed by wrapper)
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
├── src/wrapper/               ← Python wrapper (don't touch)
│   ├── cli.py                 ← CLI commands
│   ├── graph.py               ← Lineage graph engine + ODCS contracts
│   ├── simple_ddl.py          ← YAML compiler + migration + UDF + checks engine
│   ├── type_safe.py           ← Pydantic model generator
│   ├── workflow.py            ← Databricks Workflow DAG generator
│   └── compute_resolver.py   ← Serverless/dedicated resolver
│
├── artifacts/                 ← Graph exports, contracts
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

## 17. Reference: Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DBT_DATABRICKS_HOST` | Yes | — | Databricks workspace URL |
| `DBT_DATABRICKS_HTTP_PATH` | Yes | — | SQL warehouse HTTP path |
| `DBT_DATABRICKS_TOKEN` | Yes | — | Personal access token or OAuth |
| `DBT_DATABRICKS_CATALOG` | No | `workspace` | Unity Catalog name |

---

## 18. Troubleshooting

| Problem | Solution |
|---------|----------|
| `wrapper: command not found` | Run `poetry install` then `poetry shell` or prefix commands with `poetry run wrapper` |
| `wrapper.yml not found` | Run `wrapper setup` from the project root directory |
| `models.yml not found` | Create `dbt/models.yml` with your model definitions |
| Models not updating after edit | Run `wrapper compile` — you must recompile after editing models.yml |
| `ModuleNotFoundError: typer` | Run `poetry install` to install all dependencies |
| Connection refused to Databricks | Check `DBT_DATABRICKS_HOST` and `DBT_DATABRICKS_TOKEN` env vars |
| Bad data in table | Add a `quarantine: "condition"` to the model in models.yml |
| Need to rename a column | Create a migration YAML in `dbt/migrations/` |
| Want to see the full pipeline | Run `wrapper workflow --mermaid` |
| Python type errors | Run `wrapper codegen` to regenerate the SDK |
| Migration ran twice | Check `dbt/migrations/.migrations_applied` — remove the entry to re-run |
| Schema mismatch after struct change | Run `wrapper deploy --full-refresh` with `dbt run --full-refresh` |

---

## All Commands Summary

| Command | What it does |
|---------|-------------|
| `wrapper setup` | Creates project structure + wrapper.yml |
| `wrapper compile` | Compiles models.yml → SQL + schema.yml + UDFs |
| `wrapper deploy` | Builds + deploys to Databricks |
| `wrapper deploy --env prod` | Deploys to production |
| `wrapper diff` | Shows what changed since last deploy |
| `wrapper diff --mermaid` | Color-coded Mermaid diff diagram |
| `wrapper diff --output diff.json` | Writes full diff JSON to file |
| `wrapper explain model.col` | Full provenance tree for any column |
| `wrapper explain model.col --full` | Provenance with upstream checks |
| `wrapper explain model.col --mermaid` | Provenance as Mermaid diagram |
| `wrapper explain model.col --json` | Provenance as JSON for scripting |
| `wrapper migrate` | Applies migration YAMLs to models.yml |
| `wrapper migrate --dry-run` | Preview migration changes without applying |
| `wrapper migrate --no-recompile` | Applies migrations without recompiling |
| `wrapper dev-up` | Creates isolated dev schema + seeds data |
| `wrapper dev` | Watch mode: auto-recompile on save |
| `wrapper dev-down` | Tears down isolated dev schema |
| `wrapper udfs` | Shows defined UDFs |
| `wrapper udfs --output udfs.sql` | Writes CREATE FUNCTION SQL to file |
| `wrapper validate` | Shows data quality checks for all models |
| `wrapper validate --model X` | Shows checks for one model |
| `wrapper validate --output checks.sql` | Writes check SQL to file |
| `wrapper workflow` | Prints the pipeline DAG |
| `wrapper workflow --mermaid` | Visual Mermaid diagram |
| `wrapper workflow --dab` | Databricks Asset Bundle jobs YAML |
| `wrapper codegen` | Generates type-safe Python SDK |
| `wrapper codegen --check` | CI mode: fail if SDK is stale |
| `wrapper guide` | Regenerates this guide |
| `wrapper teardown` | Safely destroys everything |

---

## Built-In Features (Automatic)

These work automatically when enabled in `wrapper.yml` — no setup needed:

- **Lineage tracking**: Every row carries `_lineage` metadata (model, sources, git commit, deploy timestamp, methodology version). Column-level tracking for expressions, casts, UDFs, and aggregations.
- **Quarantine**: Add `quarantine: "condition"` to a model → bad rows move to `{model}_quarantine` table.
- **Prior Version**: Every table gets a `_v_previous` snapshot for safe rollback.
- **SQL & Python UDFs**: Define reusable functions in the `udfs:` block of `models.yml`. SQL-first, Python when needed. UDFs appear as purple nodes in the lineage graph.
- **Data Quality Checks**: Add inline `checks:` to any model — range, recency, row_count, regex, or custom SQL. View with `wrapper validate`.
- **Dev Mode**: `wrapper dev-up` → isolated schema, `wrapper dev` → auto-recompile on save, `wrapper dev-down` → teardown. Zero extra dependencies.
- **Dry-Run Migrations**: `wrapper migrate --dry-run` previews changes without touching files.
- **Provenance Explain**: `wrapper explain model.column` traces any value through expressions, UDFs, checks, and git commits — all the way to the raw source. Terminal tree, Mermaid, or JSON output.
- **Graph Diff**: `wrapper diff` compares graph snapshots — shows added/removed/modified assets, columns, and lineage edges. Color-coded Mermaid with `--mermaid`.
- **Check Nodes in Graph**: Data quality checks appear as orange hexagons in the graph. Diff detects when checks are added or removed.
- **ODCS Contracts**: Every model is an Open Data Contract Standard node. Every dependency is a lineage edge.
- **Workflow DAG**: Pipeline automatically splits into INGEST → STAGE → CLEAN → ENRICH → SERVE stages.
