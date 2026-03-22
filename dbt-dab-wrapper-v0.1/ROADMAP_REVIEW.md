# dbt-dab-wrapper — Roadmap Review & Technical Assessment

> **Date**: 22 March 2026
> **Scope**: Review of TODO.yml roadmap items against current codebase state
> **Audience**: Development team

---

## Executive Summary

The TODO.yml roadmap has 7 items and 5 backlog ideas. After auditing the codebase, **2 backlog items are already implemented** and **4 roadmap items are now DONE**: UDF support, checks MVP, dev experience, and `--dry-run`. The remaining high-value work is checks standardisation (patterns + methodologies) and provenance query ergonomics.

> **Update (current)**: Items 1–3 and the `--dry-run` quick-win from item 5 have been implemented. The wrapper now ships 14 CLI commands. See status markers (✅) below.

---

## Current Codebase State

### What Exists Today

| Component | File | Status |
|-----------|------|--------|
| YAML DDL compiler | `src/wrapper/simple_ddl.py` | Working — compiles `models.yml` → SQL + schema.yml |
| Migration engine | `src/wrapper/simple_ddl.py` | Working — YAML migrations, numbered, idempotent |
| Lineage struct (v3) | `dbt-dab-tools/macros/lineage.sql` | Working — `_lineage` struct with verbose column tracking |
| Quarantine | `dbt-dab-tools/macros/quarantine.sql` | Working — post-hook, moves bad rows to `_quarantine` table |
| Prior version | `dbt-dab-tools/macros/prior_version.sql` | Stub — `_v_previous` reference only |
| Python UDF macro | `dbt-dab-tools/macros/python_udf.sql` | Stub — `pass` placeholder in function body |
| Graph engine | `src/wrapper/graph.py` | Working — ODCS v3 contracts, Mermaid rendering, diff |
| Workflow DAG | `src/wrapper/workflow.py` | Working — 5-stage pipeline, DAB jobs YAML output |
| Type-safe SDK | `src/wrapper/type_safe.py` | Working — Pydantic codegen from manifest/schema.yml |
| Checks engine | `src/wrapper/simple_ddl.py` | Working — 5 check types (range, recency, row_count, regex, custom_sql) in `models.yml` |
| UDF compiler | `src/wrapper/simple_ddl.py` | Working — `udfs:` block in `models.yml` → `CREATE FUNCTION` SQL |
| Dev mode | `src/wrapper/cli.py` | Working — `dev-up` / `dev` / `dev-down`, polling watcher, schema isolation |
| CLI | `src/wrapper/cli.py` | 14 commands: setup, deploy, teardown, diff, compile, migrate, udfs, validate, dev-up, dev, dev-down, guide, codegen, workflow |

### Architecture Principle

Users touch exactly **two files**: `wrapper.yml` (config) and `dbt/models.yml` (table definitions). Everything else is generated. This principle must hold for every new feature.

---

## Item-by-Item Assessment

### 1. `core-udf-support` — UDF Registration ✅ DONE

**Roadmap status**: ~~High / In progress~~ → **Done**
**Implemented in**: `simple_ddl.py` (`load_udfs`, `compile_udf_sql`, `compile_all_udfs`), `cli.py` (`wrapper udfs`), `graph.py` (purple UDF nodes)

#### Current State

`dbt-dab-tools/macros/python_udf.sql` exists but contains a `pass` placeholder:

```sql
CREATE OR REPLACE FUNCTION {{ target.schema }}.{{ name }} (input STRING)
RETURNS {{ return_type }}
LANGUAGE PYTHON
AS $$
    def {{ name }}(input):
        pass  # placeholder - real code in dbt-tools/udfs/
    return {{ name }}(input)
$$;
```

The graph engine (`graph.py`) already has `_add_python_udf_nodes()` wired up — UDFs already appear as nodes in the lineage graph.

#### Recommendations

1. **Add a `udfs:` block to `models.yml`** rather than a separate `udfs.yml`. Preserves the single-file principle.

    ```yaml
    udfs:
      clean_email:
        language: sql                    # SQL-first default
        params: [email STRING]
        returns: STRING
        body: "LOWER(TRIM(email))"

      classify_revenue:
        language: python                 # Python fallback
        params: [revenue DOUBLE]
        returns: STRING
        body: |
          if revenue > 1000: return "enterprise"
          elif revenue > 100: return "mid-market"
          else: return "smb"
    ```

2. **SQL-first is correct**. SQL UDFs on Databricks: no cold start, full lineage visibility, serverless-compatible. Python UDFs: 5–15s cold start, opaque to lineage, required only for regex-heavy / ML inference / external API calls.

3. **Compiler change**: `simple_ddl.py` needs a `compile_udfs()` function that emits `CREATE FUNCTION` statements. These run before model SQL via a pre-hook or dedicated `wrapper udfs` command.

4. **Graph impact**: Already handled — `_add_python_udf_nodes()` exists. Just needs to read from `models.yml` instead of scanning SQL files.

#### Effort estimate: M (2–4 days) — agree with roadmap.

> **Result**: All recommendations implemented. SQL-first UDFs in `models.yml`, `wrapper udfs` command, purple graph nodes, `_udfs.sql` emitted by compile. Python UDF fallback supported.

---

### 2. `rock-solid-checks-framework` — Data Quality — MVP ✅ DONE

**Roadmap status**: ~~Critical / Planning~~ → **MVP Done (steps 1–4)**
**Implemented in**: `simple_ddl.py` (check compilation + `wrapper validate` SQL), `cli.py` (`wrapper validate`)
**Remaining**: Steps 5–6 (patterns.yml + methodologies.yml + extends support)

This is the highest-value differentiator. The design should be modular from day one. Proposed three-layer architecture:

#### Layer 1: Check Primitives

Atomic, parameterized assertions with fixed SQL templates. These are the building blocks:

| Primitive | Scope | What it checks |
|-----------|-------|---------------|
| `range` | cell | Value between min and max |
| `recency` | column | Most recent value within N hours |
| `reconcile` | cross-model | Child aggregate matches parent ± tolerance |
| `cardinality` | column | Distinct count within bounds |
| `row_count` | column | Total rows within bounds |
| `regex` | cell | Value matches pattern |
| `monotonic` | column | Values always increasing/decreasing |

Each primitive is a YAML entry with `scope`, `params`, `sql` template, and `message` template. Lives in `dbt-dab-tools/checks/primitives.yml`.

#### Layer 2: Check Patterns

Composable bundles that apply multiple primitives to a column type or model type:

| Pattern | Description | Composed from |
|---------|-------------|--------------|
| `monetary_column` | Any column holding money | range(0, 999M) + decimal precision regex |
| `identifier_column` | PKs and FKs | not_null + unique |
| `email_column` | Email validation | regex + on_fail: quarantine |
| `date_range` | Sane date boundaries | range(min_date, current_date) |
| `fresh_data` | Data should not be stale | recency(timestamp, max_hours) |

Patterns live in `dbt-dab-tools/checks/patterns.yml` (shared) and optionally `project/checks/patterns.yml` (project overrides).

#### Layer 3: Methodologies

Named validation contracts for model types. A methodology bundles patterns + custom checks + cross-model reconciliation:

| Methodology | Target | What it enforces |
|-------------|--------|-----------------|
| `financial_model` | Revenue/cost models | monetary column checks, row count bounds, freshness, reconciliation hooks |
| `pii_model` | Models with PII | email validation, SSN scanning, audit requirements |
| `timeseries_model` | Date-grained models | no gaps, no future dates, freshness |

Applied in `models.yml` with a single line:

```yaml
customer_summary:
  methodology: financial_model
  checks:
    - name: revenue_reconciliation
      reconcile:
        child: "SUM(total_revenue)"
        parent: { model: customer_orders, expr: "SUM(line_total)" }
        tolerance: 0.001
      severity: error
```

Three validation levels stack: methodology (team-wide) → column patterns (per-column) → inline checks (model-specific).

#### Methodologies live in the shared package, not the project

```
dbt-dab-tools/checks/                  ← shared, versioned
  primitives.yml
  patterns.yml
  methodologies/
    financial_model.yml
    pii_model.yml

project/checks/                         ← project-specific
  patterns.yml                          ← overrides/additions
  methodologies/
    our_revenue_model.yml               ← extends financial_model
```

Projects inherit via `extends:` and can override specific check parameters. Changing the shared methodology updates all projects on `dbt deps`.

#### Check Results Schema

Proposed `_checks` struct (same `named_struct()` approach as `_lineage`):

```
_checks.schema_version  → "1"
_checks.model           → model name
_checks.methodology     → methodology name
_checks.run_at          → timestamp
_checks.passed          → count
_checks.warned          → count
_checks.failed          → count
_checks.results[]       → array of {name, scope, status, severity, detail}
```

#### Implementation Path

| Step | Scope | Effort |
|------|-------|--------|
| 1. Add `checks:` block to `models.yml` schema | `simple_ddl.py` | S |
| 2. Create `checks.sql` macro (runs check SQL, records results) | `dbt-dab-tools` | M |
| 3. Add `_checks` struct to model output | `dbt-dab-tools/macros/lineage.sql` | S |
| 4. Add `wrapper validate` CLI command | `cli.py` | S |
| 5. Add `patterns.yml` + `methodologies.yml` loading | `simple_ddl.py` | M |
| 6. Add `extends` / overrides support | `simple_ddl.py` | S |

Steps 1–4 = working MVP. Steps 5–6 = standardization layer. Total estimate: **L (5–8 days)** for MVP + standardization.

> **Result**: Steps 1–4 done. Five cell/column/table-scope primitives (`range`, `recency`, `row_count`, `regex`, `custom_sql`) work inline in `models.yml`. `wrapper validate` emits check SQL. Cross-model reconciliation deferred as recommended.

#### Key risk

Scope creep. The reconciliation layer (cross-model checks) requires querying two tables in a post-hook, which needs careful transaction handling on Databricks. Recommendation: ship cell/row/column scope first, cross-model as a fast-follow.

---

### 3. `dev-experience-one-command` — Dev Mode ✅ DONE

**Roadmap status**: ~~High / Early ideas~~ → **Done**
**Implemented in**: `cli.py` (`wrapper dev-up`, `wrapper dev`, `wrapper dev-down`)

#### Rationale

Every developer hits the compile→deploy→diff loop multiple times per session. A file watcher that auto-compiles on save eliminates the most common user error: editing `models.yml` but forgetting to recompile.

#### Proposed Design

```
wrapper dev up      → creates dev_{username} schema, seeds sample data
wrapper dev         → watch mode: auto-compile + incremental dbt run on save
wrapper dev down    → drops the dev schema
```

#### Isolation Strategy

**Schema-level isolation** is the right choice for Unity Catalog:

- `dev_silver_ben` (dev) vs `silver` (prod)
- Same catalog — no permission complexity
- `profiles.yml` already supports this pattern (dev output exists with `dev_silver` schema)
- Avoids catalog proliferation

#### Implementation Notes

- File watching: `watchdog` library (Python, cross-platform, pip-installable)
- Debounce: 500ms after last save — prevents rapid-fire recompiles
- Incremental deploy: only `dbt run --select` the models that changed
- Terminal feedback: print which models recompiled + any test failures inline
- `wrapper preview` (stretch): dry-run SQL without deploying

#### Effort estimate: M (3–5 days) for `dev up` / `dev` / `dev down`. Watch mode adds ~2 days.

> **Result**: Implemented with polling-based watcher (`os.stat` / `st_mtime`) instead of `watchdog` — zero extra dependencies. Schema isolation via `dev_silver_{username}` as recommended. Watch mode included.

---

### 4. `provenance-query-gui-friendly` — Querying Lineage

**Roadmap status**: Medium-High / Partial
**Recommended priority**: 5

#### Current State

The `_lineage` struct in `lineage.sql` already captures model, sources, git commit, deploy timestamp, methodology version, and per-column expression tracking (v3). The data is there — the gap is ergonomics.

#### Recommendation: CLI + Macro, Not Custom SQL Syntax

The roadmap suggests `WITH PROVENANCE` SQL syntax. **This requires a custom SQL parser or Spark extension** — fragile, non-portable, high maintenance. Two cheaper approaches give 90% of the value:

1. **`wrapper explain customer_summary.total_revenue`** — CLI command that queries the table, walks the graph via `graph.py`, and prints a full lineage tree in the terminal. Could also output Mermaid for visual rendering.

2. **`{{ dbt_dab_tools.explain('total_revenue') }}` macro** — generates a SQL view that flattens the `_lineage.columns` struct for one column, making it queryable in any BI tool.

#### Effort estimate: S–M — agree with roadmap if scoped to CLI + macro. Much larger if pursuing custom SQL syntax (not recommended).

---

### 5. `migration-enhancements` — Safety Improvements

**Roadmap status**: Medium / Nice-to-have
**Recommended priority**: 4

#### Quick Wins (< 1 day each)

| Enhancement | Implementation |
|-------------|---------------|
| `--dry-run` ✅ | **Done** — `wrapper migrate --dry-run` shows changes without applying. |
| Auto-version-bump | If a migration touches `quarantine` or `checks`, auto-increment the `version` field in the model definition. ~20 lines in `apply_migration()`. |

#### Harder: `wrapper rollback --to v2`

`prior_version.sql` only keeps one previous version (`_v_previous`). For real rollback you need either:

- **Git-based rollback** of `models.yml` + recompile — most honest approach, uses existing VCS
- **Multiple `_v_N` snapshots** — storage-expensive, complex to manage

Recommendation: **git-based rollback** (`git checkout HEAD~1 -- dbt/models.yml && wrapper compile`). Document it as the rollback pattern rather than building custom snapshot management.

#### Effort estimate: S for dry-run + auto-version. M if building rollback infrastructure.

---

### 6. `documentation-refresh` — Guide Updates

**Roadmap status**: Medium / Blocked on features
**Recommended priority**: 6 (ongoing, not standalone)

#### Current State

`wrapper guide` command exists and works. It reads `wrapper.yml` + `models.yml` and generates a project-specific guide via `generate_agent_guide()` in `simple_ddl.py`. `GUIDE.md` also exists as a comprehensive hand-written HOW-TO.

#### Recommendation

Not blocked — and should not be treated as a standalone task. Each feature PR should update the guide generator function to include a new section. The `generate_agent_guide()` function is structured as a series of appended sections — adding a new one is ~15 lines.

---

### 7. `examples-repo` — Adoption

**Roadmap status**: Low-Medium / Future
**Recommended priority**: 7

#### Current State

The existing 5 models in `dbt/models.yml` already tell a complete story:

```
raw_customers → stg_customers → customer_clean → customer_orders → customer_summary
                raw_orders → stg_orders ↗
```

This covers: staging, type casting, quarantine, joins, aggregations. It's already "Example 1: Customer Revenue Pipeline."

#### Recommendation

Package the current project as the first example. Add 2–3 more after the checks framework lands (to show reconciliation and methodologies in action). No dedicated effort needed now.

---

## Backlog Items — Status Check

| Backlog Idea | Status |
|-------------|--------|
| `wrapper explain customer_summary.daily_revenue` | Not built — recommended as part of provenance item |
| `wrapper challenge` (interactive data challenge) | Not built — depends on checks framework |
| Auto-generate Pydantic SDK including check results | **Partially done** — `type_safe.py` generates Pydantic models now; extending for `_checks` struct is incremental |
| Integration with Databricks Workflows (DAB jobs auto-gen) | **Done** — `workflow.py` + `wrapper workflow --dab` |
| Alerting hooks for failed checks | Unblocked — checks MVP is done, hooks can now be wired to check results |

---

## Recommended Priority Order

| Priority | Item | Status | Effort |
|----------|------|--------|--------|
| ~~1~~ | ~~Dev experience (`wrapper dev`)~~ | ✅ Done | — |
| ~~2~~ | ~~Checks framework (MVP: steps 1–4)~~ | ✅ Done | — |
| ~~3~~ | ~~UDF support~~ | ✅ Done | — |
| ~~4~~ | ~~Migration `--dry-run`~~ | ✅ Done | — |
| **1** | Provenance query (CLI + macro) | High value, works today, just not ergonomic | S–M |
| **2** | Checks standardization (steps 5–6) | Patterns + methodologies layer on top of MVP | S–M |
| **3** | Documentation / examples | Ongoing, not standalone | XS |

### Suggested Sprint Plan

~~**Sprint 1** (week 1): Dev experience + migration `--dry-run`~~ ✅ Done
~~**Sprint 2** (weeks 2–3): Checks framework MVP~~ ✅ Done
~~**Sprint 3** (week 4): UDF support in models.yml~~ ✅ Done

**Next sprint**: Provenance query (`wrapper explain`) + checks standardization (patterns.yml + methodologies.yml + extends)

---

## Architectural Decisions to Make

These need team alignment before implementation:

| Decision | Options | Recommendation |
|----------|---------|---------------|
| Check results storage | `_checks` struct in model vs separate `_check_results` table | Struct in model (consistent with `_lineage` approach) |
| Cross-model reconciliation timing | Post-hook (same transaction) vs separate validation step | Separate `wrapper validate` step (post-hooks can't easily query other tables mid-transaction) |
| UDF definition location | `models.yml` `udfs:` block vs separate `udfs.yml` | `models.yml` ✅ Implemented as recommended |
| Dev isolation | Schema-level vs catalog-level | Schema-level (`dev_silver_{user}`) ✅ Implemented as recommended |
| Rollback mechanism | Git-based vs multi-version snapshots | Git-based (simpler, uses existing VCS) |
| `WITH PROVENANCE` SQL syntax | Custom parser vs CLI command + macro | CLI + macro (90% value at 10% complexity) |
