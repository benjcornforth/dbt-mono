from __future__ import annotations

from pathlib import Path


BUILTIN_MACRO_OUTPUT_DIR = Path("artifacts") / "runtime" / "macros"
_PACKAGE_MACRO_DIR = Path(__file__).with_name("builtin_macros")


def ensure_builtin_runtime_macros(project_root: Path | None = None) -> Path:
    """Materialize Forge-managed dbt macros into a generated hidden directory.

    User-authored overrides in repo-root macros/ win over built-ins of the same name.
    """
    root = (project_root or Path.cwd()).resolve()
    output_dir = root / BUILTIN_MACRO_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    user_macro_dir = root / "macros"
    package_macros = {path.name: path for path in _PACKAGE_MACRO_DIR.glob("*.sql")}

    for existing in output_dir.glob("*.sql"):
        if existing.name not in package_macros:
            existing.unlink()

    for name, source in sorted(package_macros.items()):
        if (user_macro_dir / name).exists():
            generated_override = output_dir / name
            if generated_override.exists():
                generated_override.unlink()
            continue
        (output_dir / name).write_text(source.read_text())

    return output_dir