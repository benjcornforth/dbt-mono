# QUICKSTART

This guide is for a fresh machine install.

## What You Actually Need

To build and deploy a Forge project, you need:

- `forge.yml`
- `dbt/ddl/`
- optional `dbt/project/python/`
- Python + Poetry
- the modern Databricks CLI

You do not need the repo's test scripts to run Forge.

## Prerequisites

- Python 3.11+
- Poetry
- Databricks CLI with both `auth` and `bundle` commands

Install the modern Databricks CLI:

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
```

Verify the correct CLI is active:

```bash
which databricks
databricks auth -h
databricks bundle --help
```

If `databricks auth -h` fails, you are still hitting the legacy CLI.

## Fresh Install

```bash
git clone <repo>
cd dbt-dab-wrapper-v0.1
poetry install
export PATH="$HOME/.local/bin:$PATH"
```

## Project Bootstrap

If the repo already contains `forge.yml`, do not use `forge setup` to initialize a new project. `forge setup` is for scaffolding missing project structure and should not be part of the normal fresh-install verification loop.

For an existing project, just verify the checked-in config and authored folders are present:

```bash
ls forge.yml
ls dbt/ddl
ls dbt/project/python
```

## Databricks Auth

Login with the modern Databricks CLI:

```bash
databricks auth login --host <workspace-url>
```

Then validate CLI auth:

```bash
poetry run forge auth --cli --profile dev
```

If you want to run local dbt-backed commands such as `forge deploy --local`, `forge dev-up`, or `forge dev-down`, make sure the matching profile in `~/.databrickscfg` also has:

- `host`
- `token`
- `http_path`

Then validate dbt readiness:

```bash
poetry run forge auth --dbt --profile dev
```

See [docs/DATABRICKS_AUTH.md](docs/DATABRICKS_AUTH.md) for the full auth model.

## Build And Deploy

Build the staged target bundle:

```bash
poetry run forge build --target dev
```

Deploy from the staged target root:

```bash
cd artifacts/targets/dev
databricks bundle deploy --target dev
```

## Optional Local Runtime Checks

Run local dbt-backed deployment logic:

```bash
poetry run forge deploy --local --profile dev
```

Inspect lineage:

```bash
poetry run forge explain customer_summary.total_revenue
```

## About The Test Files

The repo currently contains these root-level test scripts:

- `test_parity.py`
- `test_workflow_sdk.py`

They are optional developer checks. They are not required for:

- installation
- authentication
- build
- bundle deployment

For a fresh install, you can ignore them unless you are actively changing workflow generation internals.

## Recommended Fresh-Install Flow

```bash
poetry install
export PATH="$HOME/.local/bin:$PATH"
databricks auth login --host <workspace-url>
poetry run forge auth --cli --profile dev
poetry run forge auth --dbt --profile dev
poetry run forge build --target dev
cd artifacts/targets/dev && databricks bundle deploy --target dev
```