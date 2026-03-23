"""Verify that bifurcated layers do NOT produce default .sql files."""
from src.forge.simple_ddl import compile_all, load_raw_ddl
from pathlib import Path
import yaml, tempfile, subprocess

config = yaml.safe_load(Path("forge.yml").read_text())
ddl_path = Path("dbt/ddl")

for label, layers in [
    ("BRONZE+ (all bifurcated)", ["bronze", "silver", "gold"]),
    ("SILVER+ (bronze shared)", ["silver", "gold"]),
    ("GOLD ONLY", ["gold"]),
]:
    config["domain_layers"] = layers
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "models"
        compile_all(ddl_path, out, forge_config=config)
        print(f"\n{'='*60}")
        print(f"  {label}  |  domain_layers = {layers}")
        print(f"{'='*60}")
        for sql in sorted(out.rglob("*.sql")):
            if sql.name.startswith("schema"):
                continue
            rel = sql.relative_to(out)
            print(f"  {rel}")

print("\nDone.")
