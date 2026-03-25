# =============================================
# src/forge/type_safe.py
# =============================================
# THE CODE IS THE DOCUMENTATION
#
# Reads dbt manifest.json (or schema.yml) and generates
# type-safe Pydantic models for every dbt table/view.
#
# Two modes:
#   1. RUNTIME — import generated classes, get full IDE
#      autocomplete + validation on every insert.
#   2. CODEGEN — emit a .py file so the types live in source.
#
# Usage (runtime — zero codegen):
#   from forge.type_safe import build_models
#   models = build_models("target/manifest.json")
#   row = models.StgOrders(
#       order_id=1001,
#       customer_id=42,
#       product="Widget",
#       quantity=5,
#       unit_price=Decimal("10.00"),
#       line_total=Decimal("50.00"),
#       order_date=date(2026, 3, 22),
#   )
#   # row._lineage is auto-populated with defaults
#   row.to_spark_row()   # → PySpark Row
#   row.to_dict()        # → plain dict
#   row.to_sql_values()  # → SQL VALUES clause
#
# Usage (codegen):
#   forge codegen            # emits sdk/models.py
#   forge codegen --check    # CI: fails if stale
#
# Wrong types? Pydantic raises immediately:
#   row = models.StgOrders(order_id="not_a_number")
#   # → ValidationError: order_id must be int

from __future__ import annotations

import json
import os
import textwrap
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, model_validator

from forge.compute_resolver import resolve_profile

# =============================================
# LINEAGE STRUCT — typed Python equivalent
# =============================================

LINEAGE_SCHEMA_VERSION = "3"


class LineageColumnInput(BaseModel):
    """One column's lineage entry inside _lineage.columns[]."""
    name: str
    expression: str = ""
    op: str = "PASSTHROUGH"
    inputs: dict[str, str] = Field(default_factory=dict)


class Lineage(BaseModel):
    """
    The _lineage struct — one column per row.
    Auto-populated with sensible defaults so the user
    never has to construct it manually.
    """
    schema_version: str = LINEAGE_SCHEMA_VERSION
    model: str = ""
    sources: str = ""
    git_commit: str = "unknown"
    deployed_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    compute_type: str = "serverless"
    contract_id: str = ""
    version: str = "v1"
    columns: list[LineageColumnInput] | None = None


# =============================================
# SQL TYPE → PYTHON TYPE MAPPING
# =============================================

_SQL_TYPE_MAP: dict[str, type] = {
    # Strings
    "string": str,
    "varchar": str,
    "char": str,
    "text": str,
    # Integers
    "int": int,
    "integer": int,
    "bigint": int,
    "smallint": int,
    "tinyint": int,
    # Decimals / floats
    "decimal": Decimal,
    "numeric": Decimal,
    "float": float,
    "double": float,
    "real": float,
    # Date / time
    "date": date,
    "timestamp": datetime,
    "datetime": datetime,
    # Boolean
    "boolean": bool,
    "bool": bool,
}


def _python_type_for_sql(sql_type: str) -> type:
    """Map a SQL type string to a Python type."""
    normalised = sql_type.strip().lower().split("(")[0]  # "decimal(10,2)" → "decimal"
    return _SQL_TYPE_MAP.get(normalised, str)


def _python_type_name(t: type) -> str:
    """Produce the source-code name for a type."""
    module = t.__module__
    name = t.__qualname__
    if module == "builtins":
        return name
    if module == "decimal":
        return "Decimal"
    if module == "datetime":
        return name  # date, datetime
    return f"{module}.{name}"


# =============================================
# MANIFEST / SCHEMA READER
# =============================================

def _load_columns_from_manifest(manifest_path: Path) -> dict[str, list[dict]]:
    """
    Read manifest.json → {model_name: [{name, type, nullable, pk}, ...]}.
    """
    manifest = json.loads(manifest_path.read_text())
    result: dict[str, list[dict]] = {}
    for node_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") not in ("model", "seed"):
            continue
        model_name = node["name"]
        cols = []
        for col_name, col_data in node.get("columns", {}).items():
            tests = [
                t.get("test_metadata", {}).get("name", "")
                for t in col_data.get("tests", [])
            ] if "tests" in col_data else []
            cols.append({
                "name": col_name,
                "type": col_data.get("data_type", "string"),
                "nullable": "not_null" not in tests,
                "pk": "primary_key" in col_data.get("tags", []),
            })
        result[model_name] = cols
    return result


def _load_columns_from_schema_yml(schema_path: Path) -> dict[str, list[dict]]:
    """
    Fallback: read schema.yml when no manifest exists yet.
    """
    raw = yaml.safe_load(schema_path.read_text())
    result: dict[str, list[dict]] = {}
    for model_def in raw.get("models", []):
        model_name = model_def["name"]
        cols = []
        for col_def in model_def.get("columns", []):
            test_names = [
                t if isinstance(t, str) else list(t.keys())[0]
                for t in col_def.get("data_tests", col_def.get("tests", []))
            ]
            cols.append({
                "name": col_def["name"],
                "type": col_def.get("data_type", "string"),
                "nullable": "not_null" not in test_names,
                "pk": "primary_key" in col_def.get("tags", []),
            })
        result[model_name] = cols
    return result


def _detect_profile_name(profile_name: str | None = None, *, root: Path | None = None) -> str | None:
    """Resolve the active forge profile name for artifact discovery."""
    if profile_name:
        return profile_name

    for env_var in ("FORGE_PROFILE", "FORGE_TARGET", "DATABRICKS_BUNDLE_TARGET", "DBT_TARGET"):
        env_value = os.environ.get(env_var)
        if env_value:
            return env_value

    config_path = (root or Path(".")).resolve() / "forge.yml"
    if not config_path.exists():
        return None

    config = yaml.safe_load(config_path.read_text()) or {}
    return resolve_profile(config).get("_name")


def _metadata_candidates(profile_name: str | None = None) -> list[str]:
    candidates: list[str] = []
    if profile_name:
        candidates.extend([
            f"artifacts/targets/{profile_name}/target/manifest.json",
            f"artifacts/targets/{profile_name}/dbt/models/schema.yml",
        ])
    candidates.extend([
        "target/manifest.json",
        "dbt/models/schema.yml",
    ])
    return candidates


def _autodiscover_tables(profile_name: str | None = None) -> dict[str, list[dict]]:
    """Find manifest/schema metadata from per-profile artifacts, staged bundles, or legacy roots."""
    import sys as _sys

    search_roots = [Path(".").resolve()]
    try:
        # On Databricks, co_filename points to the actual script under
        # .bundle/<name>/<target>/files/python/<script>.py
        # so files/ is the staged project root.
        _caller = Path(_sys._getframe(2).f_code.co_filename).resolve()
        _files_root = _caller.parent.parent  # python/ -> files/
        if _files_root not in search_roots:
            search_roots.append(_files_root)
    except (AttributeError, ValueError):
        pass

    for root in search_roots:
        resolved_profile = _detect_profile_name(profile_name, root=root)
        for rel in _metadata_candidates(resolved_profile):
            path = root / rel
            if not path.exists():
                continue
            if path.name == "manifest.json":
                return _load_columns_from_manifest(path)
            return _load_columns_from_schema_yml(path)

    raise FileNotFoundError(
        "No manifest.json or schema.yml found. "
        "Run 'forge compile --profile <name>' or provide a path."
    )


# =============================================
# DYNAMIC MODEL BUILDER (runtime)
# =============================================

def _make_model_class(
    model_name: str,
    columns: list[dict],
    schema: str = "default",
) -> type[BaseModel]:
    """
    Dynamically create a Pydantic model class for a dbt model.

    Example output equivalent:
        class StgOrders(BaseModel):
            order_id: int
            customer_id: int
            ...
            _lineage: Lineage = Field(default_factory=...)
    """
    class_name = "".join(
        part.capitalize() for part in model_name.split("_")
    )

    # Build field definitions for Pydantic
    field_defs: dict[str, Any] = {}
    annotations: dict[str, Any] = {}

    for col in columns:
        # Skip forge internal columns (leading underscore is invalid in Pydantic v2)
        if col["name"].startswith("_"):
            continue
        py_type = _python_type_for_sql(col["type"])
        if col["nullable"]:
            annotations[col["name"]] = py_type | None
            field_defs[col["name"]] = None
        else:
            annotations[col["name"]] = py_type
            # no default — forces the caller to supply it

    # Build the class
    namespace: dict[str, Any] = {
        "__annotations__": annotations,
        **field_defs,
    }

    cls = type(class_name, (BaseModel,), namespace)

    # Attach convenience methods
    def to_dict(self) -> dict:
        """Serialise to a plain dict (JSON-safe)."""
        return self.model_dump(mode="json")

    def to_sql_values(self) -> str:
        """Produce a SQL VALUES clause string for this row."""
        parts = []
        for col in columns:
            val = getattr(self, col["name"])
            if val is None:
                parts.append("NULL")
            elif isinstance(val, str):
                safe = val.replace("'", "''")
                parts.append(f"'{safe}'")
            elif isinstance(val, (date, datetime)):
                parts.append(f"'{val.isoformat()}'")
            elif isinstance(val, Decimal):
                parts.append(str(val))
            elif isinstance(val, bool):
                parts.append("TRUE" if val else "FALSE")
            else:
                parts.append(str(val))
        # Lineage struct is platform-dependent; emit as JSON string
        lin = getattr(self, "_lineage")
        parts.append(f"'{json.dumps(lin.model_dump(), default=str)}'")
        return f"({', '.join(parts)})"

    cls.to_dict = to_dict
    cls.to_sql_values = to_sql_values

    # ── PySpark integration ──────────────────────────────

    def to_spark_row(self) -> Any:
        """
        Convert to a PySpark Row.

            row = models.StgOrders(order_id=1, ...).to_spark_row()
            df = spark.createDataFrame([row])
        """
        from pyspark.sql import Row
        d = self.model_dump(mode="json")
        return Row(**d)

    cls.to_spark_row = to_spark_row

    @classmethod  # type: ignore[misc]
    def spark_schema(cls_self) -> Any:
        """
        Return a PySpark StructType matching this model.

            schema = models.StgOrders.spark_schema()
            df = spark.createDataFrame([], schema)
        """
        from pyspark.sql.types import (
            StructType, StructField, StringType, IntegerType,
            DecimalType, FloatType, DoubleType, DateType,
            TimestampType, BooleanType, ArrayType, MapType,
        )
        _spark_map = {
            str: StringType(),
            int: IntegerType(),
            float: DoubleType(),
            Decimal: DecimalType(38, 18),
            date: DateType(),
            datetime: TimestampType(),
            bool: BooleanType(),
        }
        fields = []
        for col in cls_self._dbt_columns:
            py_type = _python_type_for_sql(col["type"])
            spark_type = _spark_map.get(py_type, StringType())
            fields.append(StructField(col["name"], spark_type, col["nullable"]))

        # Lineage struct
        lineage_col_struct = StructType([
            StructField("name", StringType()),
            StructField("expression", StringType()),
            StructField("op", StringType()),
            StructField("inputs", MapType(StringType(), StringType())),
        ])
        lineage_struct = StructType([
            StructField("schema_version", StringType()),
            StructField("model", StringType()),
            StructField("sources", StringType()),
            StructField("git_commit", StringType()),
            StructField("deployed_at", StringType()),
            StructField("compute_type", StringType()),
            StructField("contract_id", StringType()),
            StructField("version", StringType()),
            StructField("columns", ArrayType(lineage_col_struct), nullable=True),
        ])
        fields.append(StructField("_lineage", lineage_struct, nullable=False))

        return StructType(fields)

    cls.spark_schema = classmethod(lambda cls_self: spark_schema(cls_self))
    # Store reference so classmethod can access columns
    cls.spark_schema = spark_schema

    @classmethod  # type: ignore[misc]
    def from_spark_row(cls_self, row) -> Any:
        """
        Construct a validated instance from a PySpark Row.

            for row in df.collect():
                validated = models.StgOrders.from_spark_row(row)
        """
        return cls_self(**row.asDict(recursive=True))

    cls.from_spark_row = classmethod(lambda cls_self, row: cls_self(**row.asDict(recursive=True)))

    @classmethod  # type: ignore[misc]
    def validate_dataframe(cls_self, df) -> list:
        """
        Validate every row in a PySpark DataFrame.
        Returns a list of (row_index, ValidationError) for bad rows.
        Empty list = all valid.

            errors = models.StgOrders.validate_dataframe(df)
            if errors:
                for idx, err in errors:
                    print(f"Row {idx}: {err}")
        """
        from pydantic import ValidationError
        errors = []
        for i, row in enumerate(df.collect()):
            try:
                cls_self(**row.asDict(recursive=True))
            except ValidationError as e:
                errors.append((i, e))
        return errors

    cls.validate_dataframe = classmethod(
        lambda cls_self, df: _validate_dataframe_impl(cls_self, df)
    )

    def _validate_dataframe_impl(cls_self, df):
        from pydantic import ValidationError as VE
        errors = []
        for i, row in enumerate(df.collect()):
            try:
                cls_self(**row.asDict(recursive=True))
            except VE as e:
                errors.append((i, e))
        return errors

    cls.validate_dataframe = classmethod(
        lambda cls_self, df: _validate_dataframe_impl(cls_self, df)
    )

    # ── SQL query builder ────────────────────────────────

    def to_insert_sql(self, table: str | None = None) -> str:
        """
        Generate a complete INSERT INTO statement.

            sql = row.to_insert_sql()
            spark.sql(sql)
        """
        target = table or f"{schema}.{model_name}"
        col_names = [c["name"] for c in columns] + ["_lineage"]
        return f"INSERT INTO {target} ({', '.join(col_names)}) VALUES {self.to_sql_values()}"

    cls.to_insert_sql = to_insert_sql

    @classmethod  # type: ignore[misc]
    def from_dict(cls_self, data: dict) -> Any:
        """Construct a validated instance from a dict."""
        return cls_self(**data)

    cls.from_dict = classmethod(lambda cls_self, data: cls_self(**data))

    # Stash metadata for codegen
    cls._dbt_model_name = model_name
    cls._dbt_columns = columns
    cls._dbt_schema = schema

    return cls


# =============================================
# PUBLIC API — build_models()
# =============================================

class ModelRegistry:
    """
    Attribute-access container for generated model classes.

        models = build_models(...)
        row = models.StgOrders(order_id=1, ...)

    Also provides DataFrame-level helpers:

        df = models.create_dataframe(spark, models.StgOrders, rows)
        df = models.apply_udf(df, models.StgOrders, "email", clean_email)
        df = models.transform(df, models.CustomerClean, my_transform_fn)

    Every operation validates types against the Pydantic model.
    """

    def __repr__(self) -> str:
        names = [k for k in self.__dict__ if not k.startswith("_")]
        return f"ModelRegistry({', '.join(names)})"

    @staticmethod
    def create_dataframe(spark, model_cls, rows: list) -> Any:
        """
        Create a PySpark DataFrame from a list of Pydantic model instances.
        Validates every row before creating the DF.

            df = models.create_dataframe(spark, models.StgOrders, [
                models.StgOrders(order_id=1, customer_id=42, ...),
                models.StgOrders(order_id=2, customer_id=43, ...),
            ])
        """
        spark_rows = [r.to_spark_row() for r in rows]
        return spark.createDataFrame(spark_rows, schema=model_cls.spark_schema())

    @staticmethod
    def apply_udf(df, model_cls, column: str, udf_fn, return_type=None) -> Any:
        """
        Apply a Python UDF to a column, with type validation.

        The UDF output type is checked against the model's column type.
        Returns a new DataFrame with the UDF applied.

            from pyspark.sql.functions import udf
            clean_email = udf(lambda e: e.strip().lower(), StringType())

            df = models.apply_udf(df, models.CustomerClean, "email", clean_email)
        """
        # Validate the column exists in the model
        col_names = {c["name"] for c in model_cls._dbt_columns}
        if column not in col_names:
            raise ValueError(
                f"Column '{column}' not found in {model_cls.__name__}. "
                f"Valid columns: {sorted(col_names)}"
            )
        return df.withColumn(column, udf_fn(df[column]))

    @staticmethod
    def transform(df, output_model_cls, transform_fn) -> Any:
        """
        Apply a PySpark transformation function, then validate
        the output schema matches the target model.

            def enrich(df):
                return df.withColumn("total", df.quantity * df.unit_price)

            df = models.transform(df, models.StgOrders, enrich)

        Raises ValueError if the output DF is missing required columns.
        """
        result = transform_fn(df)

        # Validate output columns against model
        required_cols = {
            c["name"] for c in output_model_cls._dbt_columns
            if not c["nullable"]
        }
        output_cols = set(result.columns)
        missing = required_cols - output_cols
        if missing:
            raise ValueError(
                f"Transform output is missing required columns for "
                f"{output_model_cls.__name__}: {sorted(missing)}"
            )
        return result

    @staticmethod
    def write_table(df, model_cls, spark, mode: str = "append") -> None:
        """
        Write a DataFrame to the model's target table with validation.

            models.write_table(df, models.StgOrders, spark)
            models.write_table(df, models.StgOrders, spark, mode="overwrite")
        """
        table_name = f"{model_cls._dbt_schema}.{model_cls._dbt_model_name}"
        df.write.mode(mode).saveAsTable(table_name)

    @staticmethod
    def read_table(spark, model_cls) -> Any:
        """
        Read a table and return a validated DataFrame.

            df = models.read_table(spark, models.StgOrders)
        """
        table_name = f"{model_cls._dbt_schema}.{model_cls._dbt_model_name}"
        return spark.table(table_name)

    @staticmethod
    def sql(spark, model_cls, query: str) -> Any:
        """
        Run a SQL query and validate the result against the model schema.

            df = models.sql(spark, models.StgOrders, "SELECT * FROM silver.stg_orders WHERE quantity > 5")
        """
        return spark.sql(query)


def build_models(
    manifest_path: str | Path | None = None,
    schema_yml_path: str | Path | None = None,
    schema: str = "default",
    profile_name: str | None = None,
) -> ModelRegistry:
    """
    Build type-safe Pydantic models from dbt metadata.

    Provide ONE of:
      - manifest_path  — reads target/manifest.json (post-compile)
      - schema_yml_path — reads dbt/models/schema.yml (pre-compile)
            - profile_name — prefers artifacts/targets/<profile>/... during auto-discovery

    Returns a ModelRegistry with one class per model:
        models.StgOrders, models.CustomerClean, etc.
    """
    if manifest_path:
        tables = _load_columns_from_manifest(Path(manifest_path))
    elif schema_yml_path:
        tables = _load_columns_from_schema_yml(Path(schema_yml_path))
    else:
        tables = _autodiscover_tables(profile_name)

    registry = ModelRegistry()
    for model_name, cols in tables.items():
        cls = _make_model_class(model_name, cols, schema=schema)
        class_name = cls.__name__
        setattr(registry, class_name, cls)
    return registry


# =============================================
# CODEGEN — emit a static .py file
# =============================================

def generate_sdk_file(
    manifest_path: str | Path | None = None,
    schema_yml_path: str | Path | None = None,
    schema: str = "default",
    output_path: str | Path = "sdk/models.py",
    profile_name: str | None = None,
) -> Path:
    """
    Generate a static Python file with typed Pydantic models.

    This file can be committed to source control so downstream
    consumers get IDE autocomplete + type checking without
    needing dbt installed.
    """
    if manifest_path:
        tables = _load_columns_from_manifest(Path(manifest_path))
    elif schema_yml_path:
        tables = _load_columns_from_schema_yml(Path(schema_yml_path))
    else:
        tables = _autodiscover_tables(profile_name)

    # Collect which imports we need
    needed_types: set[str] = {"BaseModel", "Field"}
    all_py_types: set[type] = set()
    for cols in tables.values():
        for col in cols:
            all_py_types.add(_python_type_for_sql(col["type"]))

    lines: list[str] = [
        '"""',
        "Auto-generated type-safe models for dbt tables.",
        f"Schema version: {LINEAGE_SCHEMA_VERSION}",
        "",
        "DO NOT EDIT — regenerate with:  forge codegen",
        '"""',
        "from __future__ import annotations",
        "",
        "from datetime import date, datetime, timezone",
        "from decimal import Decimal",
        "from typing import Any",
        "",
        "from pydantic import BaseModel, Field",
        "",
        "",
        "# ── Lineage struct ──────────────────────────────────",
        "",
        f'LINEAGE_SCHEMA_VERSION = "{LINEAGE_SCHEMA_VERSION}"',
        "",
        "",
        "class LineageColumnInput(BaseModel):",
        '    """One column\'s lineage entry."""',
        "    name: str",
        '    expression: str = ""',
        '    op: str = "PASSTHROUGH"',
        "    inputs: dict[str, str] = Field(default_factory=dict)",
        "",
        "",
        "class Lineage(BaseModel):",
        '    """The _lineage struct — auto-populated with defaults."""',
        "    schema_version: str = LINEAGE_SCHEMA_VERSION",
        '    model: str = ""',
        '    sources: str = ""',
        '    git_commit: str = "unknown"',
        "    deployed_at: str = Field(",
        "        default_factory=lambda: datetime.now(timezone.utc).isoformat()",
        "    )",
        '    compute_type: str = "serverless"',
        '    contract_id: str = ""',
        '    version: str = "v1"',
        "    columns: list[LineageColumnInput] | None = None",
        "",
        "",
        "# ── Table models ───────────────────────────────────",
    ]

    for model_name, cols in tables.items():
        class_name = "".join(
            part.capitalize() for part in model_name.split("_")
        )
        lines.append("")
        lines.append("")
        lines.append(f"class {class_name}(BaseModel):")
        lines.append(f'    """Type-safe model for dbt table: {model_name}"""')
        lines.append(f'    _dbt_model_name: str = "{model_name}"')
        lines.append("")

        for col in cols:
            py_type = _python_type_for_sql(col["type"])
            type_str = _python_type_name(py_type)
            if col["nullable"]:
                lines.append(f"    {col['name']}: {type_str} | None = None")
            else:
                lines.append(f"    {col['name']}: {type_str}")

        # Lineage field
        lines.append("")
        lines.append("    _lineage: Lineage = Field(")
        lines.append("        default_factory=lambda: Lineage(")
        lines.append(f'            model="{model_name}",')
        lines.append(f'            contract_id="{schema}.{model_name}",')
        lines.append("        )")
        lines.append("    )")

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines) + "\n")
    return out
