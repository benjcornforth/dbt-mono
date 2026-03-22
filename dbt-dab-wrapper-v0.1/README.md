# dbt-forge

**Production dbt pipelines on Databricks — without writing SQL, YAML boilerplate, or DAB config.**

You edit two files. Forge generates everything else: SQL models, schema tests, Databricks Asset Bundles, lineage graphs, data quality checks, and UDFs.

---

## What This Is

A CLI that sits on top of **dbt** and **Databricks Asset Bundles (DAB)**. It turns a declarative YAML definition of your tables into a fully deployed, tested, lineage-tracked data pipeline.

**The two-file contract:** you only edit `forge.yml` (project config) and `dbt/models.yml` (table definitions). Everything else — SQL, schema.yml, DAB bundles, UDF scripts — is generated.

---

## What You Do

```bash
# 1. Install
poetry install

# 2. Scaffold
forge setup

# 3. Define your tables in dbt/models.yml
#    (columns, types, sources, checks, UDFs — all in one place)

# 4. Deploy
forge compile   # generates SQL + tests
forge deploy    # builds graph + DAB + runs dbt
```

That's the core loop. Edit `dbt/models.yml` → `forge compile` → `forge deploy`.

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
```

---

## What You Get In Return

| Capability | How |
|---|---|
| **Zero SQL authoring** | Define columns + types in YAML → SQL is generated |
| **Automatic lineage** | Every row carries `_lineage` (model, sources, git commit, deploy timestamp) |
| **Column-level provenance** | `forge explain` traces any column through expressions, UDFs, joins → back to raw source |
| **Data quality checks** | Inline `checks:` block (range, recency, row_count, regex, custom SQL) — no extra framework |
| **Reusable UDFs** | SQL-first, Python fallback. Defined in `models.yml`, compiled to `CREATE FUNCTION` |
| **Graph diffs** | Before/after comparison of every asset, column, and lineage edge |
| **Quarantine** | Add `quarantine: "condition"` → bad rows auto-route to `{model}_quarantine` |
| **Prior versions** | Every table gets a `_v_previous` snapshot for safe rollback |
| **Schema migrations** | `forge migrate` applies YAML-defined changes; `--dry-run` to preview |
| **Type-safe Python SDK** | `forge codegen` → Pydantic models for every table |
| **Pipeline DAG** | `forge workflow --dab` → Databricks jobs YAML, auto-staged |
| **Dev isolation** | Per-developer schema, watch-mode recompile, zero-dependency teardown |
| **ODCS contracts** | Every model is an Open Data Contract Standard v3.0.0 node |
| **Portability flags** | `postgres_compatible: true` in forge.yml → avoids Databricks-only SQL |

---

## Project Structure

```
forge.yml                ← project config (YOU edit)
dbt/models.yml           ← table definitions (YOU edit)
dbt/models/*.sql         ← generated (never edit)
dbt/models/schema.yml    ← generated tests
dbt/migrations/*.yml     ← schema changes (you create)
src/forge/               ← CLI + engines (you don't touch)
artifacts/               ← graph snapshots, compiled output
```

Macros (lineage, quarantine, prior_version, UDFs) live in the **dbt-dab-tools** package, pulled via `packages.yml`. They are never in this repo.

---

## All Commands

| Command | Purpose |
|---|---|
| `forge setup` | Scaffold project |
| `forge compile` | YAML → SQL + schema.yml + UDFs |
| `forge deploy [--env prod]` | Build + deploy to Databricks |
| `forge diff [--mermaid]` | Graph diff since last deploy |
| `forge explain model.col [--mermaid\|--json\|--full]` | Column provenance |
| `forge migrate [--dry-run]` | Apply schema migrations |
| `forge dev-up` / `dev` / `dev-down` | Dev mode lifecycle |
| `forge validate [--model X]` | Data quality checks |
| `forge workflow [--mermaid\|--dab]` | Pipeline DAG |
| `forge codegen [--check]` | Type-safe Python SDK |
| `forge guide` | Regenerate full project guide |
| `forge teardown` | Destroy everything safely |

---

## Requirements

- Python 3.10+
- [Poetry](https://python-poetry.org/)
- dbt-core 1.8+ with dbt-databricks adapter
- Databricks workspace with Unity Catalog

---

## Full Documentation

Run `forge guide` to generate a comprehensive step-by-step guide covering every feature, option, and edge case.
