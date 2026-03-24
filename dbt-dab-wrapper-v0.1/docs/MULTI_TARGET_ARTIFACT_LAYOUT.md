# Multi-Target Artifact Layout

This note defines a practical artifact layout for Forge so compiled SQL and Python can be deployed to multiple target locations without one target overwriting another.

## Problem

The current output layout is convenient for a single active target:

- `sql/`
- `dbt/models/`
- `dbt/functions/`
- `resources/jobs/`
- `dist/`

That breaks down once the same project needs to deploy to more than one target location in the same workspace.

Examples:

- `dev` and `prod` Databricks targets
- multiple workspaces or regions
- multiple catalogs/schemas for the same logical project
- customer-specific or domain-specific deployment bundles

The core issue is that target-specific compiled outputs are written into shared root folders.

That means:

- one build can overwrite another
- deployment inputs are not self-contained per target
- it is hard to diff what changed for one target only
- packaging Python and SQL together for a given destination is awkward

## Design Goal

Separate build outputs into:

1. shared build artifacts
2. target-rendered deployment bundles

The source tree remains the same:

- `dbt/ddl/` is the authored source of truth
- `python/` is the authored task source
- `forge.yml` defines targets and placement rules

Compiled artifacts should live under `artifacts/`, not in shared repo-root deployment folders.

## Proposed Layout

```text
artifacts/
  manifest/
    asset_manifest.json
    graph.json
    workflow_manifest.json
    location_matrix.json
  packages/
    python/
      forge_runtime-<version>.whl
    metadata/
      build_info.json
  targets/
    dev/
      deploy_manifest.json
      sql/
        000_udfs.sql
        001_lineage_graph.sql
        ...
      dbt/
        models/
        functions/
        sources/
        volumes/
      python/
        task_entrypoints.json
      resources/
        jobs/
          SETUP_sales.yml
          PROCESS_sales_sales.yml
          TEARDOWN_sales.yml
      databricks.yml
    prod/
      deploy_manifest.json
      sql/
      dbt/
      python/
      resources/
      databricks.yml
```

## Principle

Use a two-phase build.

### Phase 1: Normalize Once

Build target-neutral manifests from authored inputs:

- asset graph
- workflow graph
- logical asset definitions
- dependency ordering
- Python task metadata

These go under:

- `artifacts/manifest/`

This layer should contain no destination-specific file paths or workspace-specific bundle wiring unless explicitly required.

### Phase 2: Render Per Target

For each deployment target, render a self-contained bundle under:

- `artifacts/targets/<target_name>/`

Each target folder should contain everything needed for deployment to that destination:

- rendered SQL
- rendered dbt artifacts
- generated workflow YAML
- target-local bundle config
- deployment manifest describing exactly what was rendered

## Target Identity

A target should be a named deployment destination, not just a profile.

Recommended target key:

- `<profile_name>` when profiles are already unique deployment destinations
- or `<profile_name>__<location_name>` if one profile can deploy to multiple logical locations

Examples:

- `dev`
- `prod`
- `dev_eu`
- `prod_customer_a`

## What Should Be Shared

These artifacts should be built once and reused by every target where possible.

### Shared Python Package

The Forge runtime wheel belongs in:

- `artifacts/packages/python/`

Reason:

- the wheel is not target-specific
- target bundles should reference the same built wheel instead of rebuilding it repeatedly

### Shared Build Metadata

Store build metadata in:

- `artifacts/packages/metadata/build_info.json`

Suggested contents:

- git SHA
- build timestamp
- forge version
- wheel filename
- source manifest hashes

## What Must Be Target-Specific

These artifacts should be rendered per target.

### Pure SQL

Pure SQL contains resolved catalog/schema names, so it is target-specific.

Canonical location:

- `artifacts/targets/<target>/sql/`

### dbt-Compiled Assets

If dbt SQL is rendered with target-specific database/schema config, it is target-specific.

Canonical location:

- `artifacts/targets/<target>/dbt/`

This includes:

- `models/`
- `functions/`
- `sources/`
- `volumes/`

### Workflow YAML

Workflow definitions point at target-specific SQL files, profiles, warehouses, and bundle targets.

Canonical location:

- `artifacts/targets/<target>/resources/jobs/`

### Bundle Config

Each target should have its own rendered deployment entrypoint:

- `artifacts/targets/<target>/databricks.yml`

That keeps deployment trivial:

- deploy one folder
- know exactly which files belong to that target

## Deploy Manifest

Each target folder should include a machine-readable manifest:

- `artifacts/targets/<target>/deploy_manifest.json`

Suggested fields:

```json
{
  "target": "dev",
  "profile": "dev",
  "platform": "databricks",
  "generated_at": "2026-03-24T00:00:00Z",
  "git_commit": "abcdef1",
  "artifacts": {
    "sql": ["sql/000_udfs.sql", "sql/001_lineage_graph.sql"],
    "jobs": ["resources/jobs/SETUP_sales.yml"],
    "bundle": "databricks.yml",
    "wheel": "../../packages/python/forge_runtime-0.1.2-py3-none-any.whl"
  }
}
```

This becomes the source of truth for automation and promotion.

## Git Policy

Target-scoped artifacts should also be separated by source-control policy.

Recommended rule:

- non-`prd` rendered targets are build outputs only
- only `prd` target artifacts are commit-eligible

That means:

- `artifacts/targets/dev/` should not be committed
- `artifacts/targets/test/` should not be committed
- `artifacts/targets/qa/` should not be committed
- `artifacts/targets/prd/` may be committed when the team wants a deployable, auditable release artifact in Git

### Why

Non-production targets are usually:

- developer-local
- workspace-specific
- temporary
- frequently rebuilt

Committing them creates noise and leaks environment-specific deployment state into the repo.

Production artifacts are different:

- they are the release candidate
- they are the promotion boundary
- they are the useful audit trail

### Recommended Enforcement

Use two layers:

1. `.gitignore` for default developer behavior
2. CI or pre-commit validation for hard enforcement

Recommended `.gitignore` policy:

```gitignore
artifacts/targets/**
!artifacts/targets/prd/
!artifacts/targets/prd/**
```

Important limitation:

- `.gitignore` is only a default guardrail
- a user can still bypass it with forced adds

So if you want a real policy, add a repository check that fails commits or pull requests containing:

- `artifacts/targets/dev/**`
- `artifacts/targets/test/**`
- `artifacts/targets/qa/**`
- any other non-`prd` target path

### Practical Release Model

The clean model is:

- build many targets locally or in CI
- deploy non-`prd` targets directly from generated artifacts
- only promote and optionally commit `artifacts/targets/prd/`

That keeps Git focused on releasable deployment state, not ephemeral environment state.

## Recommended CLI Shape

Add explicit target rendering commands.

### Build One Target

```bash
forge build --target dev
```

Outputs:

- `artifacts/targets/dev/...`

### Build All Targets

```bash
forge build --all-targets
```

Outputs:

- `artifacts/targets/dev/...`
- `artifacts/targets/prod/...`

### Deploy One Target

```bash
forge deploy --target dev
```

This should deploy from:

- `artifacts/targets/dev/`

not from repo-root `sql/` or `resources/jobs/`.

## Transition Plan

This can be introduced without a large rewrite if done in steps.

### Step 1

Keep existing compile functions, but add target-specific output roots.

Examples:

- `compile_all_pure_sql(..., output_dir=artifacts/targets/dev/sql)`
- `compile_all(..., output_dir=artifacts/targets/dev/dbt/models)`
- workflow generation writes to `artifacts/targets/dev/resources/jobs/`

### Step 2

Move bundle generation to render target-local `databricks.yml` files in each target folder.

### Step 3

Build the wheel once and reference it from all target bundles.

### Step 4

Stop treating repo-root `sql/` and `resources/jobs/` as canonical deployment inputs.

Optional compatibility mode:

- keep repo-root outputs as a convenience alias for the last built target
- but document that canonical deployment input is always `artifacts/targets/<target>/`

## Recommended Concrete Change Set

The lowest-friction implementation path is:

1. introduce a `target_root` concept
2. render all deployable artifacts under `artifacts/targets/<target>/`
3. keep shared wheel output under `artifacts/packages/python/`
4. emit a `deploy_manifest.json` per target
5. make `forge deploy` consume a target folder instead of implicit repo-root outputs

## Why This Fits Forge

This model matches how Forge already works:

- DDL remains the single source of truth
- placement resolution still happens in the compiler
- workflows already consume generated SQL files by path
- bundle sync already gathers generated folders into deployable inputs

The only real change is that those generated inputs become target-scoped instead of global.

That is the correct abstraction if the same project must be deployed effortlessly to more than one destination.