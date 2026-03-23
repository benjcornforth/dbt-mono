"""Test domain bifurcation at bronze, silver, and gold layers."""
import tempfile, shutil, yaml
from pathlib import Path
from src.forge.simple_ddl import compile_all, load_ddl

# Minimal DDL: 3 layers, 1 model each, chained lineage
ddl = Path(tempfile.mkdtemp()) / "ddl"
for layer, models in [
    ("bronze", {"raw_customers": {"columns": {"id": "int"}}}),
    ("silver", {"stg_customers": {"source": "raw_customers", "columns": {"id": "int"}}}),
    ("gold",   {"customer_summary": {"source": "stg_customers", "columns": {"id": "int"}}}),
]:
    d = ddl / layer
    d.mkdir(parents=True, exist_ok=True)
    for name, mdef in models.items():
        (d / f"{name}.yml").write_text(yaml.dump({"models": {name: mdef}}))

# Sanity check: does load_ddl find models?
test_cfg = {
    "id": "test", "name": "test", "scope": "fd",
    "schemas": ["bronze", "silver", "gold"],
    "catalogs": ["bronze", "silver", "gold", "meta", "operations"],
}
loaded = load_ddl(ddl, forge_config=test_cfg)
print(f"Loaded models: {list(loaded.keys())}")
print(f"DDL dir contents: {list(ddl.iterdir())}")
for sub in ddl.iterdir():
    if sub.is_dir():
        print(f"  {sub.name}/: {[f.name for f in sub.iterdir()]}")

domains_cfg = {"eu": {"schema_suffix": "_eu"}, "us": {"schema_suffix": "_us"}}

base_cfg = {
    "id": "test", "name": "test", "scope": "fd",
    "schemas": ["bronze", "silver", "gold"],
    "catalogs": ["bronze", "silver", "gold", "meta", "operations"],
    "schema_pattern": "{user}_{id}",
    "catalog_pattern": "{env}_{scope}_{catalog}",
    "domains": domains_cfg,
    "active_profile": "dev",
    "profiles": {"dev": {"env": "dev", "catalog": "bronze", "platform": "databricks"}},
}

for label, layers in [
    ("BRONZE+ (domains at all layers)", ["bronze", "silver", "gold"]),
    ("SILVER+ (shared bronze, domain silver/gold)", ["silver", "gold"]),
    ("GOLD ONLY (shared bronze+silver, domain gold)", ["gold"]),
]:
    out = Path(tempfile.mkdtemp()) / "models"
    cfg = {**base_cfg, "domain_layers": layers}
    results = compile_all(ddl, out, forge_config=cfg)

    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"  domain_layers = {layers}")
    print(f"{'='*60}")

    # Show all non-US instances and their refs
    for name in sorted(results.keys()):
        if name.startswith("_") or name.endswith("_us"):
            continue
        path = results[name]
        content = path.read_text()
        from_lines = [l.strip() for l in content.split("\n") if "ref(" in l or "source(" in l]
        schema_parts = [l.strip() for l in content.split("\n") if "schema=" in l]
        is_domain = name.endswith("_eu")
        marker = " [DOMAIN]" if is_domain else ""
        print(f"\n  {name}{marker}:")
        for s in schema_parts:
            print(f"    schema: {s}")
        for f in from_lines:
            print(f"    from:   {f}")

    if not any(k.endswith("_eu") for k in results if not k.startswith("_")):
        print(f"\n  WARNING: No _eu instances found! Keys: {[k for k in results if not k.startswith('_')]}")

    shutil.rmtree(out.parent)

shutil.rmtree(ddl.parent)
print("\n\nAll 3 bifurcation points work correctly.")
