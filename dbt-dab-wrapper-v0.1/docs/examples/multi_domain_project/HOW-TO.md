# Multi-Domain Pipeline — How-To Guide

> Run the same pipeline across EU, US, and APAC — each in its own schema,
> reading from its own source data, running in its own Databricks workflow.
> Zero code duplication.

---

## What You'll Build

The same four-model pipeline from the sales project, but deployed three
times — once per region — each with isolated schemas and source data:

```
                    ┌─ stg_customers_eu ─▶ customer_clean_eu ─▶ customer_orders_eu ─▶ customer_summary_eu
raw_customers_eu  ──┤
raw_customers_us  ──┼─ stg_customers_us ─▶ customer_clean_us ─▶ customer_orders_us ─▶ customer_summary_us
raw_customers_apac──┤
                    └─ stg_customers_apac─▶ customer_clean_apac─▶ customer_orders_apac─▶ customer_summary_apac
```

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

## Step 2 — Define Domain-Specific Sources

If each region reads from its own raw data (e.g. separate Unity Catalog
volumes), add `domain_sources:` to the model DDL:

**`dbt/ddl/bronze/staging/stg_customers.yml`**

```yaml
models:
  stg_customers:
    description: "Staged customers — one instance per domain"
    source: raw_customers                    # default / fallback
    domain_sources:
      eu: raw_customers_eu                   # EU reads from EU volume
      us: raw_customers_us                   # US reads from US volume
      apac: raw_customers_apac              # APAC reads from APAC volume
    columns:
      customer_id:  { type: int, required: true, unique: true }
      email:        { type: string }
      country:      { type: string }
      revenue:      { type: "decimal(10,2)", cast: true }
```

**What this does:**
- `stg_customers_eu.sql` reads from `{{ ref('raw_customers_eu') }}`
- `stg_customers_us.sql` reads from `{{ ref('raw_customers_us') }}`
- `stg_customers_apac.sql` reads from `{{ ref('raw_customers_apac') }}`

Without `domain_sources:`, all three domains read from the same `raw_customers`.

### For join models

Override specific join sources per domain with a dict:

```yaml
  customer_orders:
    sources: { c: customer_clean, o: stg_orders }
    domain_sources:
      eu: { o: stg_orders_eu_special }       # override just the orders source for EU
```

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

## Step 4 — Compile & Inspect

```bash
forge compile
```

Check the generated output:

```
dbt/models/regional/
├── bronze/
│   ├── eu/
│   │   ├── stg_customers_eu.sql       ← reads from raw_customers_eu
│   │   └── stg_orders_eu.sql
│   ├── us/
│   │   ├── stg_customers_us.sql       ← reads from raw_customers_us
│   │   └── stg_orders_us.sql
│   └── apac/
│       ├── stg_customers_apac.sql
│       └── stg_orders_apac.sql
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

## Step 5 — Per-Domain Workflows

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

## Step 6 — Deploy

```bash
forge deploy
```

This:
1. Creates domain schemas (`ben_regional_eu`, `ben_regional_us`, `ben_regional_apac`)
2. Seeds source data
3. Runs all domain instances (15+ models)
4. Generates per-domain DAB workflows
5. Deploys to Databricks via `databricks bundle deploy`

---

## Step 7 — Opt a Model Out of Domains

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
   - Applies `domain_sources:` overrides if defined
3. Each instance is a real dbt model — tests, docs, graph, workflow all work
4. `build_domain_workflows()` splits the graph into per-domain subsets

---

## Quick Reference

| What | How |
|------|-----|
| Add a domain | `domains: { jp: { schema_suffix: "_jp" } }` in forge.yml |
| Choose which layers bifurcate | `domain_layers: [bronze, silver]` |
| Domain-specific source | `domain_sources: { eu: raw_customers_eu }` in model DDL |
| Opt a model out | `domain: false` in model DDL |
| Shared workflow | `domain_workflows: shared` in forge.yml |
| Separate workflows (default) | One workflow per domain, runs in parallel |
| Check compiled source | `grep "from" dbt/models/.../eu/stg_customers_eu.sql` |
| Deploy all domains | `forge deploy` (no extra flags) |

---

## Project Structure

```
multi_domain_project/
├── forge.yml                                ← domains + domain_layers here
├── HOW-TO.md                                ← this guide
└── dbt/
    └── ddl/
        ├── bronze/
        │   ├── seeds/
        │   │   ├── raw_customers.yml        ← shared default seed
        │   │   └── raw_orders.yml
        │   └── staging/
        │       ├── stg_customers.yml        ← has domain_sources
        │       └── stg_orders.yml
        └── silver/
            ├── customer_clean.yml           ← quarantine (auto per domain)
            ├── customer_orders.yml          ← join (auto per domain)
            └── customer_summary.yml         ← aggregation + checks
```

**The rule:** model definitions stay the same across all domains.
Only `forge.yml` (add domains) and optionally `domain_sources:` (route sources)
change when you go multi-domain.
