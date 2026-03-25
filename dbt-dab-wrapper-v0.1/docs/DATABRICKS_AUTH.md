# Databricks Auth

Forge now treats Databricks auth as two separate checks because Databricks CLI auth and local dbt runtime requirements are not the same thing.

## The Two Checks

### 1. CLI auth

This covers:

- `databricks bundle ...`
- `databricks workspace ...`
- other Databricks CLI commands that use unified authentication

Forge validates this with:

```bash
forge auth --cli
```

Internally this calls:

```bash
databricks auth env --profile <profile>
```

That makes the check match the Databricks CLI authentication model instead of guessing from profile fields.

This requires the modern Databricks CLI that supports `databricks auth ...` commands. Legacy `databricks-cli` installations such as `0.18.x` will fail this check and should be upgraded.

### 2. dbt runtime auth

This covers Forge commands that run dbt locally:

- `forge deploy --local`
- `forge dev-up`
- `forge dev-down`

Forge validates this with:

```bash
forge auth --dbt
```

This is stricter than CLI auth because local dbt needs warehouse connectivity details, not just Databricks identity auth.

## Recommended Pattern

1. Map each Forge profile to a Databricks profile in `forge.yml`.
2. Use the modern Databricks CLI auth flow for interactive users.
3. Store any extra dbt warehouse fields such as `http_path` in the same `.databrickscfg` profile.
4. Validate both concerns explicitly.

Example:

```yaml
profiles:
  dev:
    platform: databricks
    databricks_profile: DEFAULT
  prod:
    platform: databricks
    databricks_profile: PROD
```

## Profile Name Matching

Forge resolves the Databricks profile name like this:

1. If `forge.yml` sets `databricks_profile`, Forge uses that value.
2. If `databricks_profile` is omitted, Forge falls back to the Forge profile name.

Recommended pattern: keep them the same.

### Recommended: matching names

```yaml
# forge.yml
profiles:
  dev:
    platform: databricks
    databricks_profile: dev
    env: dev
    catalog: main
    schema: silver
```

```ini
# ~/.databrickscfg
[dev]
host = https://dbc-12345678-abcd.cloud.databricks.com
token = dapiXXXXXXXXXXXXXXXX
http_path = /sql/1.0/warehouses/abcdef1234567890
```

That makes these commands line up cleanly:

```bash
poetry run forge auth --cli --profile dev
poetry run forge auth --dbt --profile dev
databricks auth env --profile dev
```

### Also valid: explicit mapping to a different profile name

```yaml
# forge.yml
profiles:
  dev:
    platform: databricks
    databricks_profile: DEFAULT
    env: dev
    catalog: main
    schema: silver
```

```ini
# ~/.databrickscfg
[DEFAULT]
host = https://dbc-12345678-abcd.cloud.databricks.com
token = dapiXXXXXXXXXXXXXXXX
http_path = /sql/1.0/warehouses/abcdef1234567890
```

### Fallback: omit `databricks_profile`

```yaml
# forge.yml
profiles:
  dev:
    platform: databricks
    env: dev
```
```

In that case Forge looks for:

```ini
[dev]
host = https://dbc-12345678-abcd.cloud.databricks.com
token = dapiXXXXXXXXXXXXXXXX
http_path = /sql/1.0/warehouses/abcdef1234567890
```

## Bootstrap

### Interactive user auth

Use the Databricks CLI login flow described in the Databricks docs:

```bash
export PATH="$HOME/.local/bin:$PATH"
databricks auth login --host <workspace-url>
```

This creates or updates a profile in `~/.databrickscfg`.

If you already have an older `databricks` binary elsewhere in `PATH`, make sure the modern CLI installed under `~/.local/bin` resolves first.

### Service principal auth

For automation, define a profile manually with OAuth M2M fields such as:

- `host`
- `client_id`
- `client_secret`

### dbt warehouse connectivity

For local dbt-backed commands, Forge still expects these fields in the selected Databricks profile:

- `host`
- `token`
- `http_path`

`cluster_id` is optional and reported when present.

## Validate

Check both modes for all Forge profiles:

```bash
forge auth
```

Check CLI auth only:

```bash
forge auth --cli --profile dev
```

Check dbt runtime only:

```bash
forge auth --dbt --profile dev
```

If you need a non-default config file, Forge respects `DATABRICKS_CONFIG_FILE`.

## Failure Mode

When a check fails, Forge reports:

- the Forge profile name
- the mapped Databricks profile name
- the config file path
- whether the failure is CLI auth or dbt runtime auth
- the missing dbt fields or the first Databricks CLI validation error

## Operational Rule

Use `~/.databrickscfg` as the source of truth for Databricks profiles in this project.

Use `forge auth --cli` to validate Databricks unified authentication.

Use `forge auth --dbt` to validate local dbt runtime readiness.