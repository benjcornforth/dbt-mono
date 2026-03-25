from __future__ import annotations

from pathlib import Path


PROJECT_CODE_ROOT = Path("dbt") / "project"
PROJECT_PYTHON_DIR = PROJECT_CODE_ROOT / "python"

GENERATED_CODE_ROOT = Path("dbt") / "generated"
PROJECT_SDK_DIR = GENERATED_CODE_ROOT / "sdk"
DEFAULT_SDK_OUTPUT = PROJECT_SDK_DIR / "models.py"