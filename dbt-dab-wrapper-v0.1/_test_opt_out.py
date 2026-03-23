"""Test per-model domain opt-out (domain: false)."""
from src.forge.simple_ddl import compile_all, load_raw_ddl
from pathlib import Path
import yaml, tempfile, shutil

config = yaml.safe_load(Path("forge.yml").read_text())
config["domain_layers"] = ["silver", "gold"]

ddl_path = Path("dbt/ddl")

# Patch: set domain: false on customer_orders
with tempfile.TemporaryDirectory() as ddl_tmp:
    shutil.copytree(ddl_path, Path(ddl_tmp) / "ddl")
    patched_ddl = Path(ddl_tmp) / "ddl"

    # Read and patch customer_orders.yml
    co_path = patched_ddl / "silver" / "customer_orders.yml"
    co_data = yaml.safe_load(co_path.read_text())
    co_data["models"]["customer_orders"]["domain"] = False
    co_path.write_text(yaml.dump(co_data, default_flow_style=False))

    with tempfile.TemporaryDirectory() as out_tmp:
        out = Path(out_tmp) / "models"
        results = compile_all(patched_ddl, out, forge_config=config)

        print("Generated .sql files:")
        for sql in sorted(out.rglob("*.sql")):
            if sql.name.startswith("schema"):
                continue
            rel = sql.relative_to(out)
            print(f"  {rel}")

        # Verify customer_orders is shared (no domain instances)
        print()
        co_files = [f for f in out.rglob("customer_orders*.sql") if "schema" not in f.name]
        co_names = sorted(f.name for f in co_files)
        print(f"customer_orders files: {co_names}")

        has_default = "customer_orders.sql" in co_names
        has_domain = any("_eu" in n or "_us" in n or "_apac" in n for n in co_names)
        print(f"  Default instance exists: {has_default}")
        print(f"  Domain instances exist: {has_domain}")

        # Verify customer_clean IS still bifurcated
        cc_files = [f for f in out.rglob("customer_clean*.sql") if "schema" not in f.name]
        cc_names = sorted(f.name for f in cc_files)
        print(f"\ncustomer_clean files: {cc_names}")
        cc_has_default = "customer_clean.sql" in cc_names
        cc_has_domain = any("_eu" in n for n in cc_names)
        print(f"  Default instance exists: {cc_has_default}")
        print(f"  Domain instances exist: {cc_has_domain}")

        # Check that domain refs in customer_summary still work
        # customer_summary_eu should ref customer_orders (shared) not customer_orders_eu
        cs_eu = out / "demo" / "silver" / "eu" / "customer_summary_eu.sql"
        if cs_eu.exists():
            content = cs_eu.read_text()
            print(f"\ncustomer_summary_eu refs:")
            for line in content.splitlines():
                if "ref(" in line or "from" in line.lower():
                    print(f"  {line.strip()}")

        ok = has_default and not has_domain and not cc_has_default and cc_has_domain
        print(f"\n{'PASS' if ok else 'FAIL'}: per-model domain opt-out")
