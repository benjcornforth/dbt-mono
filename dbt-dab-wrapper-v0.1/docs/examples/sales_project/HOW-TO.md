# Sales Data Pipeline — How-To Guide

> A complete walkthrough: from zero to a deployed, tested, lineage-tracked
> sales pipeline on Databricks. No SQL or dbt experience needed.

---

## What You'll Build

A five-model pipeline that takes raw customer and order CSVs and produces
a customer summary with loyalty tiers, data quality checks, quarantine
for bad rows, and full column-level lineage.

```
raw_customers.csv ─┐
                   ├─▶ stg_customers ─▶ customer_clean ─┐
raw_orders.csv ────┴─▶ stg_orders    ──────────────────┴─▶ customer_orders ─▶ customer_summary
                                                                                     ↳ tier (UDF)
```

**Time to complete:** ~15 minutes.

---

## Prerequisites

- Python 3.10+
- [Poetry](https://python-poetry.org/) installed
- Databricks workspace with Unity Catalog (or use `--pure-sql` for local testing)

---

## Step 1 — Set Up the Project

```bash
# Clone and install
cd sales_project
poetry install

# Scaffold project structure (creates folders + starter files)
forge setup --name sales_data
```

This creates:

```
forge.yml                 ← project config (YOU edit)
dbt/ddl/                  ← model definitions (YOU edit)
dbt/models/               ← generated SQL (never edit)
dbt/seeds/                ← CSV seed files (you place here)
```

---

## Step 2 — Configure forge.yml

Open `forge.yml`. The key settings:

```yaml
name: sales_data
scope: sales
id: sales
active_profile: dev

catalog_pattern: "{env}_{scope}_{catalog}"    # → dev_sales_bronze
schema_pattern:  "{user}_{id}"                # → ben_sales

profiles:
  dev:
    platform: databricks
    databricks_profile: DEFAULT               # reads ~/.databrickscfg
    env: dev
    catalog: bronze
    compute: { type: serverless }
```

**What this means:**
- Tables land in catalog `dev_sales_bronze`, schema `ben_sales`
- Silver models go to `dev_sales_silver.ben_sales`
- Switch to prod by running `forge deploy --profile prod`

To connect to Databricks:

```bash
databricks configure --profile DEFAULT
# Enter your workspace URL and token
```

---

## Step 3 — Define Your Seeds (Raw Data)

Place your CSV files in `dbt/seeds/`:

```
dbt/seeds/raw_customers.csv
dbt/seeds/raw_orders.csv
```

Then declare them in DDL YAML so forge knows their columns and origin:

**`dbt/ddl/bronze/seeds/raw_customers.yml`**

```yaml
seeds:
  raw_customers:
    description: "Raw customer data loaded from CSV seed"
    origin:
      type: seed
      path: dbt/seeds/raw_customers.csv
      format: csv
    columns:
      customer_id:  { type: int, description: "Unique customer identifier" }
      first_name:   { type: string }
      last_name:    { type: string }
      email:        { type: string }
      signup_date:  { type: date }
      country:      { type: string }
      revenue:      { type: "decimal(10,2)" }
```

The `origin:` block tells `forge explain` where this data physically comes from.

---

## Step 4 — Define Staging Models (Bronze Layer)

Staging models read from seeds, apply types, and add lineage.
No SQL — just list your columns:

**`dbt/ddl/bronze/staging/stg_customers.yml`**

```yaml
models:
  stg_customers:
    description: "Staged customers with typed columns and lineage"
    source: raw_customers
    columns:
      customer_id:  { type: int, required: true, unique: true }
      first_name:   { type: string }
      last_name:    { type: string }
      email:        { type: string }
      signup_date:  { type: date, cast: true }
      country:      { type: string }
      revenue:      { type: "decimal(10,2)", cast: true }
```

**Key options:**
- `source: raw_customers` — reads from the seed you defined
- `required: true` — generates a not_null test
- `unique: true` — generates a unique test
- `cast: true` — wraps the column in `CAST()` for type safety

**`dbt/ddl/bronze/staging/stg_orders.yml`**

```yaml
models:
  stg_orders:
    description: "Staged orders with computed line_total"
    source: raw_orders
    columns:
      order_id:     { type: int, required: true, unique: true }
      customer_id:  { type: int, required: true }
      product:      { type: string }
      quantity:     { type: int }
      unit_price:   { type: "decimal(10,2)", cast: true }
      line_total:   { expr: "quantity * unit_price" }
      order_date:   { type: date, cast: true }
```

Note `line_total` uses `expr:` — a computed column. Forge generates
`quantity * unit_price AS line_total` in the SQL.

---

## Step 5 — Define Cleaning Model (Silver Layer)

Quarantine bad rows automatically — no manual WHERE clause needed:

**`dbt/ddl/silver/customer_clean.yml`**

```yaml
models:
  customer_clean:
    description: "Clean customers — nulls and negatives quarantined"
    source: stg_customers
    materialized: table
    quarantine: "email IS NULL OR revenue < 0"
    columns:
      customer_id:  { type: int, required: true, unique: true }
      first_name:   { type: string }
      last_name:    { type: string }
      email:        { type: string, required: true }
      signup_date:  { type: date }
      country:      { type: string }
      revenue:      { type: "decimal(10,2)" }
```

**What happens:**
- Rows where `email IS NULL OR revenue < 0` go to `customer_clean_quarantine`
- Clean rows go to `customer_clean`
- Both tables have full lineage

---

## Step 6 — Define a Join Model

Join customers with their orders using a simple `sources:` + `join:` block:

**`dbt/ddl/silver/customer_orders.yml`**

```yaml
models:
  customer_orders:
    description: "Joined clean customers with their orders"
    sources:
      c: customer_clean
      o: stg_orders
    join: "c.customer_id = o.customer_id"
    broadcast: c
    materialized: table
    columns:
      customer_id:  { from: c }
      first_name:   { from: c }
      email:        { from: c }
      order_id:     { from: o, required: true, unique: true }
      product:      { from: o }
      line_total:   { from: o }
      order_date:   { from: o }
```

**Key options:**
- `sources:` maps aliases (`c`, `o`) to upstream models
- `join:` defines the ON clause
- `broadcast: c` generates a `/*+ BROADCAST(c) */` hint for Databricks join optimization
- `from: c` tells forge which alias each column comes from

---

## Step 7 — Define an Aggregation + UDF + Checks

The summary model groups, aggregates, calls a UDF, and validates:

**`dbt/ddl/silver/customer_summary.yml`**

```yaml
models:
  customer_summary:
    description: "Aggregated customer metrics"
    source: customer_orders
    materialized: table
    group_by: [customer_id, first_name, last_name, email, country]
    columns:
      customer_id:      { type: int, required: true, unique: true }
      first_name:       { type: string }
      last_name:        { type: string }
      email:            { type: string }
      country:          { type: string }
      total_orders:     { expr: "count(order_id)", type: int }
      total_revenue:    { expr: "sum(line_total)", type: "decimal(10,2)" }
      first_order_date: { expr: "min(order_date)", type: date }
      last_order_date:  { expr: "max(order_date)", type: date }
      tier:             { udf: "loyalty_tier(total_revenue)", type: string }
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

The `tier` column calls a UDF (defined next). The `checks:` block generates
dbt tests that run automatically on deploy.

---

## Step 8 — Define UDFs

Create reusable functions — SQL or Python:

**`dbt/ddl/00_udfs.yml`**

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

  clean_email:
    description: "Lowercase and trim email addresses"
    language: python
    returns: string
    runtime_version: "3.11"
    handler: clean
    params:
      - { name: raw_email, type: string }
    body: |
      def clean(raw_email):
          if raw_email is None:
              return None
          return raw_email.strip().lower()
```

Use any UDF in a column with `udf: "loyalty_tier(total_revenue)"`.
Forge generates the CREATE FUNCTION SQL and wires it through lineage.

---

## Step 9 — Compile & Deploy

```bash
# Generate SQL + schema tests from your YAML
forge compile

# See what was generated
ls dbt/models/sales/

# Deploy to Databricks (seeds + models + tests)
forge deploy
```

**What `forge compile` produces:**

```
dbt/models/sales/bronze/stg_customers.sql
dbt/models/sales/bronze/stg_orders.sql
dbt/models/sales/silver/customer_clean.sql
dbt/models/sales/silver/customer_orders.sql
dbt/models/sales/silver/customer_summary.sql
dbt/models/schema.yml         ← all tests (not_null, unique, checks)
dbt/functions/loyalty_tier.sql ← CREATE FUNCTION SQL
dbt/functions/clean_email.sql
```

You never edit these files — they're regenerated on every `forge compile`.

---

## Step 10 — Inspect Your Pipeline

```bash
# See the full pipeline DAG
forge workflow
# INGEST → STAGE → CLEAN → ENRICH → SERVE (5 tasks)

# Visual diagram
forge workflow --mermaid

# Trace any column to its source
forge explain customer_summary.total_revenue

# Data quality check summary
forge validate

# Generate Databricks workflow YAML
forge workflow --dab
```

---

## What's Next

### Add a new column

Edit the model YAML (e.g. add `avg_order_value: { expr: "avg(line_total)" }` to
customer_summary) and recompile:

```bash
forge compile
forge deploy
```

### Add multi-domain support

Run the same pipeline for EU, US, APAC — each in its own schema:

```yaml
# Add to forge.yml:
domains:
  eu:   { schema_suffix: "_eu" }
  us:   { schema_suffix: "_us" }
  apac: { schema_suffix: "_apac" }
domain_layers: [bronze, silver]
```

```bash
forge compile   # generates stg_customers_eu.sql, stg_customers_us.sql, etc.
forge deploy    # deploys all domain instances
```

Each domain can read from its own source with `domain_sources:` in the model DDL.
See `docs/examples/multi_domain_project/` for the full pattern.

### SQL-only mode (no dbt at runtime)

```bash
forge compile --pure-sql
# Outputs numbered .sql files to sql/ — runs directly on a SQL warehouse
```

### Generate a project guide

```bash
forge guide
# Writes .instructions.md — share with new team members or AI agents
```

---

## Project Structure

```
sales_project/
├── forge.yml                          ← project config (YOU edit)
├── HOW-TO.md                          ← this guide
└── dbt/
    ├── ddl/                           ← model definitions (YOU edit)
    │   ├── 00_udfs.yml                ← UDF definitions
    │   ├── bronze/
    │   │   ├── seeds/
    │   │   │   ├── raw_customers.yml  ← seed declarations
    │   │   │   └── raw_orders.yml
    │   │   └── staging/
    │   │       ├── stg_customers.yml  ← staging models
    │   │       └── stg_orders.yml
    │   └── silver/
    │       ├── customer_clean.yml     ← cleaning + quarantine
    │       ├── customer_orders.yml    ← joins
    │       └── customer_summary.yml   ← aggregations + UDFs + checks
    ├── models/                        ← generated SQL (never edit)
    └── seeds/                         ← your CSV files
        ├── raw_customers.csv
        └── raw_orders.csv
```

**The rule:** you edit files in `dbt/ddl/` and `forge.yml`. Everything else is generated.

---

## Quick Reference

| Action | Command |
|--------|---------|
| Scaffold project | `forge setup` |
| Compile YAML → SQL | `forge compile` |
| Deploy to Databricks | `forge deploy` |
| Deploy to prod | `forge deploy --profile prod` |
| See pipeline DAG | `forge workflow` |
| Trace a column | `forge explain customer_summary.tier` |
| Check data quality | `forge validate` |
| What changed? | `forge diff` |
| Generate standalone SQL | `forge compile --pure-sql` |
| Safe teardown | `forge teardown` |
| Dev isolation | `forge dev-up` |
| Regenerate guide | `forge guide` |
