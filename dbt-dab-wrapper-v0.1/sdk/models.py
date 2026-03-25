"""
Auto-generated type-safe models for dbt tables.
Schema version: 3

DO NOT EDIT — regenerate with:  forge codegen
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field


# ── Lineage struct ──────────────────────────────────

LINEAGE_SCHEMA_VERSION = "3"


class LineageColumnInput(BaseModel):
    """One column's lineage entry."""
    name: str
    expression: str = ""
    op: str = "PASSTHROUGH"
    inputs: dict[str, str] = Field(default_factory=dict)


class Lineage(BaseModel):
    """The _lineage struct — auto-populated with defaults."""
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


# ── Table models ───────────────────────────────────


class FileManifest(BaseModel):
    """Type-safe model for dbt table: file_manifest"""
    _dbt_model_name: str = "file_manifest"

    file_path: str
    file_name: str
    model_name: str
    domain: str | None = None
    file_format: str | None = None
    row_count: int | None = None
    file_size_bytes: int | None = None
    checksum: str | None = None
    ingested_at: datetime
    status: str | None = None

    _lineage: Lineage = Field(
        default_factory=lambda: Lineage(
            model="file_manifest",
            contract_id="default.file_manifest",
        )
    )


class RawCustomers(BaseModel):
    """Type-safe model for dbt table: raw_customers"""
    _dbt_model_name: str = "raw_customers"

    customer_id: int | None = None
    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None
    signup_date: date | None = None
    country: str | None = None
    revenue: Decimal | None = None

    _lineage: Lineage = Field(
        default_factory=lambda: Lineage(
            model="raw_customers",
            contract_id="default.raw_customers",
        )
    )


class RawOrders(BaseModel):
    """Type-safe model for dbt table: raw_orders"""
    _dbt_model_name: str = "raw_orders"

    order_id: int | None = None
    customer_id: int | None = None
    product: str | None = None
    quantity: int | None = None
    unit_price: Decimal | None = None
    order_date: date | None = None

    _lineage: Lineage = Field(
        default_factory=lambda: Lineage(
            model="raw_orders",
            contract_id="default.raw_orders",
        )
    )


class CustomerClean(BaseModel):
    """Type-safe model for dbt table: customer_clean"""
    _dbt_model_name: str = "customer_clean"

    customer_id: int
    first_name: str | None = None
    last_name: str | None = None
    email: str
    signup_date: date | None = None
    country: str | None = None
    revenue: Decimal | None = None

    _lineage: Lineage = Field(
        default_factory=lambda: Lineage(
            model="customer_clean",
            contract_id="default.customer_clean",
        )
    )


class CustomerOrders(BaseModel):
    """Type-safe model for dbt table: customer_orders"""
    _dbt_model_name: str = "customer_orders"

    customer_id: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None
    country: str | None = None
    order_id: str
    product: str | None = None
    quantity: str | None = None
    unit_price: str | None = None
    line_total: str | None = None
    order_date: str | None = None

    _lineage: Lineage = Field(
        default_factory=lambda: Lineage(
            model="customer_orders",
            contract_id="default.customer_orders",
        )
    )


class CustomerSummary(BaseModel):
    """Type-safe model for dbt table: customer_summary"""
    _dbt_model_name: str = "customer_summary"

    customer_id: int
    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None
    country: str | None = None
    total_orders: int | None = None
    total_revenue: Decimal | None = None
    first_order_date: date | None = None
    last_order_date: date | None = None
    tier: str | None = None

    _lineage: Lineage = Field(
        default_factory=lambda: Lineage(
            model="customer_summary",
            contract_id="default.customer_summary",
        )
    )


class StgCustomers(BaseModel):
    """Type-safe model for dbt table: stg_customers"""
    _dbt_model_name: str = "stg_customers"

    customer_id: int
    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None
    signup_date: date | None = None
    country: str | None = None
    revenue: Decimal | None = None

    _lineage: Lineage = Field(
        default_factory=lambda: Lineage(
            model="stg_customers",
            contract_id="default.stg_customers",
        )
    )


class StgOrders(BaseModel):
    """Type-safe model for dbt table: stg_orders"""
    _dbt_model_name: str = "stg_orders"

    order_id: int
    customer_id: int
    product: str | None = None
    quantity: int | None = None
    unit_price: Decimal | None = None
    line_total: str | None = None
    order_date: date | None = None

    _lineage: Lineage = Field(
        default_factory=lambda: Lineage(
            model="stg_orders",
            contract_id="default.stg_orders",
        )
    )
