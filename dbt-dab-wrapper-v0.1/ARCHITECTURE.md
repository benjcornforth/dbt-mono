# Forge Architecture

## Three-Workflow Lifecycle

Every project compiles to three Databricks Asset Bundle workflows:

| Workflow | Purpose | Runs when |
|---|---|---|
| `SETUP_{id}` | Create schemas, UDFs, lineage tables, seed data, python-managed table schemas | First deploy, or after schema changes |
| `PROCESS_{id}` | Execute data transformations (SQL models + python ingest) | Every scheduled run |
| `TEARDOWN_{id}` | Remove workflow resources; optionally drop data | Decommissioning |

## Compilation Rules

`forge compile --pure-sql` generates numbered SQL files in `sql/`. These rules are enforced at compile time — violations produce errors, never runtime failures.

### Schema Ordering

| Rule | Enforced by |
|---|---|
| Every SQL file that does DDL (CREATE TABLE/VIEW/FUNCTION, TRUNCATE) must have its own `CREATE SCHEMA IF NOT EXISTS` for every schema it creates objects in | `_validate_sql_schema_ordering()` |
| Each SQL file runs as an independent Databricks sql_task — no cross-file assumptions | Compile-time validator |
| INSERT/SELECT referencing existing tables are exempt (tables guaranteed by SETUP dependency chain) | Validator only checks DDL lines |

### File Numbering

| Sequence | Content | Workflow |
|---|---|---|
| `000_udfs.sql` | Schema creation + UDF definitions + lineage UDFs | SETUP |
| `001_lineage_graph.sql` | Lineage graph + log tables, TRUNCATE + INSERT edges | SETUP |
| `002+` managed_by models | `CREATE TABLE IF NOT EXISTS` (schema only, no data) | SETUP |
| `002+` transformation models | `CREATE OR REPLACE VIEW/TABLE` with full SQL | PROCESS |
| `NNN_*_quarantine.sql` | Quarantine sidecars (same sequence as parent model) | PROCESS |

### Domain Protection

| Condition | Compile behaviour |
|---|---|
| `domain: false` + model in domain-suffixed schema | **Compile error** — shared assets must live in the base schema |
| `domain: false` + teardown `--wipe-all` | **Preserved** — never dropped by sub-domain teardown |
| `domain: true` + teardown `--wipe-all` | Dropped (domain-specific asset) |
| No `domain` key | Treated as domain-scoped (droppable) |

### Managed-by Models

| Condition | Behaviour |
|---|---|
| `managed_by: python` in DDL | Compiled to `CREATE TABLE IF NOT EXISTS` with typed columns only (no SELECT/FROM) |
| `managed_by` model in PROCESS | **Excluded** — schema created in SETUP, data populated by python task |
| `managed_by` model in SETUP | Added as `create_{name}` task, depends on `seed_lineage_graph` |

## Lineage System

Two-tier lineage: static graph + runtime log.

### Static Graph (`lineage_graph` table)

- Built at compile time from DDL column mappings
- Stored in meta catalog (`dev_fd_meta.{schema}`)
- TRUNCATE + re-INSERT on every deploy
- Tracks: source→target model, column mappings, transform type, join keys, cross-catalog refs

### Runtime Log (`lineage_log` table)

| Source | How it writes to lineage_log |
|---|---|
| SQL models | `INSERT INTO lineage_log` appended to each compiled SQL file, uses `{{job.run_id}}` |
| Python tasks | `task.log_lineage(model, row_count, sources=[...])` method on `ForgeTask` |

### Transform Types

| Type | Meaning |
|---|---|
| `PASSTHROUGH` | Column copied unchanged |
| `CAST` | Type conversion |
| `EXPRESSION` | Computed from expression (e.g. `quantity * unit_price`) |
| `AGGREGATION` | Aggregate function (SUM, COUNT, AVG) |
| `UDF` | User-defined function applied |
| `JOIN` | Column from a joined table |

### Trace UDFs

| UDF | Returns | Usage |
|---|---|---|
| `trace_lineage(model, column)` | Text: full upstream path | `SELECT trace_lineage('customer_summary', 'total_revenue')` |
| `trace_lineage_json(model, column)` | JSON array of hops | For programmatic consumption |
| `last_run_id(model)` | Most recent `run_id` | `SELECT last_run_id('customer_orders')` |

## Serverless Constraints

All SQL runs on serverless SQL warehouses. These constraints apply:

| Feature | Supported | Workaround |
|---|---|---|
| `RUNTIME_VERSION` | No | Stripped at compile time |
| `PACKAGES` | No | Stripped at compile time |
| `HANDLER` | No | Python function renamed to match UDF name |
| Python UDFs | Yes | Function name must equal UDF name |

## Task Dependencies

### SETUP Workflow

```
setup (dbt deps + seed)
  → create_udfs (sql_task: 000_udfs.sql)
    → seed_lineage_graph (sql_task: 001_lineage_graph.sql)
      → create_file_manifest (sql_task: managed_by DDL)
      → create_raw_customers (sql_task: managed_by DDL)
      → create_raw_orders (sql_task: managed_by DDL)
```

### PROCESS Workflow

- Each model is a separate `sql_task` with lineage-derived dependencies
- Quarantine sidecars depend on their parent model
- Python tasks bring their own dependency configuration
- `managed_by: python` models are **not** in PROCESS

### TEARDOWN Workflow

```
teardown (dbt run-operation drop_all_models)
```

Assets with `domain: false` or `shared` tag are always preserved.
