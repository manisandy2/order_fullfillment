import logging
import json
from optparse import Option
from typing import Dict, List, Tuple, Any,Optional
import pyarrow as pa
from pyiceberg.types import (
    BooleanType, LongType, DoubleType, DateType, IntegerType,
    TimestampType, StringType, NestedField, FloatType
)
from datetime import datetime, date
from pyiceberg.schema import Schema


logger = logging.getLogger(__name__)

def schema(record: Dict[str, Any],
            required_fields: List[str],
            field_overrides: Dict[str, tuple]
           ) -> Tuple[Schema, pa.Schema]:

    # Validate required fields
    missing = [f for f in required_fields if f not in record]
    if missing:
        raise ValueError(f"Missing required fields: {missing}")

    iceberg_fields = []
    arrow_fields = []

    # Sort for deterministic field IDs
    sorted_items = sorted(record.items())

    for idx, (name, value) in enumerate(sorted_items, start=1):
        if name in field_overrides:
            ice_type, arrow_type, required = field_overrides[name]
        else:
            required = False

            # Boolean
            if isinstance(value, bool):
                ice_type, arrow_type = BooleanType(), pa.bool_()

            # Integer
            elif isinstance(value, int):
                ice_type, arrow_type = LongType(), pa.int64()

            # Float
            elif isinstance(value, float):
                ice_type, arrow_type = DoubleType(), pa.float64()

            # Date only
            elif isinstance(value, date) and not isinstance(value, datetime):
                ice_type, arrow_type = DateType(), pa.date32()

            # Timestamp
            elif isinstance(value, datetime):
                ice_type, arrow_type = TimestampType(), pa.timestamp("ms")

            # String (default)
            else:
                ice_type, arrow_type = StringType(), pa.string()

        iceberg_fields.append(
            NestedField(field_id=idx, name=name, field_type=ice_type, required=required)
        )
        arrow_fields.append(pa.field(name, arrow_type, nullable=not required))

    iceberg_schema = Schema(*iceberg_fields)
    arrow_schema = pa.schema(arrow_fields)
    return iceberg_schema, arrow_schema

def clean_rows(
    rows: List[Dict[str, Any]],
    boolean_fields: Optional[List[str]] = None,
    timestamps_fields: Optional[List[str]] = None,
    date_fields: Optional[List[str]] = None,
    field_overrides: Optional[Dict[str, tuple]] = None,
) -> List[Dict[str, Any]]:
    """
    Clean and normalize row data for schema compliance
    (MySQL → Arrow → Iceberg safe).
    """

    boolean_fields = set(boolean_fields or [])
    timestamps_fields = set(timestamps_fields or [])
    date_fields = set(date_fields or [])
    field_overrides = field_overrides or {}

    protected_fields = boolean_fields | timestamps_fields | date_fields

    dt_formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d",
    ]

    date_formats = [
        "%Y-%m-%d",
        "%d/%m/%Y",
    ]

    for row in rows:

        # --------------------------------------------------
        # 1️⃣ Boolean fields
        # --------------------------------------------------
        for f in boolean_fields:
            val = row.get(f)

            if val is None:
                row[f] = False
            elif isinstance(val, bool):
                row[f] = val
            elif isinstance(val, (int, float)):
                row[f] = bool(val)
            elif isinstance(val, str):
                row[f] = val.strip().lower() in {"1", "true", "yes", "on"}
            else:
                row[f] = False

        # --------------------------------------------------
        # 2️⃣ Timestamp fields
        # --------------------------------------------------
        for f in timestamps_fields:
            val = row.get(f)

            if val in (None, ""):
                row[f] = datetime.now()
                continue

            if isinstance(val, datetime):
                continue

            parsed = None
            for fmt in dt_formats:
                try:
                    parsed = datetime.strptime(str(val), fmt)
                    break
                except Exception:
                    pass

            row[f] = parsed if parsed else datetime.now()

        # --------------------------------------------------
        # 3️⃣ Date fields
        # --------------------------------------------------
        for f in date_fields:
            val = row.get(f)

            if val in (None, ""):
                row[f] = date.today()
                continue

            if isinstance(val, date) and not isinstance(val, datetime):
                continue

            parsed = None
            for fmt in date_formats:
                try:
                    parsed = datetime.strptime(str(val), fmt).date()
                    break
                except Exception:
                    pass

            row[f] = parsed if parsed else date.today()

        # --------------------------------------------------
        # 4️⃣ Schema-driven normalization (CRITICAL)
        # --------------------------------------------------
        for key, val in row.items():
            if key in protected_fields:
                continue

            if key in field_overrides:
                try:
                    _, arrow_type, is_required = field_overrides[key]
                except ValueError:
                    raise ValueError(
                        f"Invalid override tuple for '{key}', "
                        f"expected (iceberg_type, arrow_type, is_required)"
                    )

                # Handle NULL
                if val is None:
                    row[key] = "" if is_required else None
                    continue

                # 🔥 STRING Arrow columns → FORCE STRING
                if pa.types.is_string(arrow_type):
                    if isinstance(val, bool):
                        row[key] = "true" if val else "false"
                    else:
                        row[key] = str(val)
                    continue

                # Non-string override → keep value
                row[key] = val

            else:
                # No override → safe default
                if isinstance(val, bool):
                    row[key] = "true" if val else "false"
                else:
                    row[key] = str(val) if val is not None else None

    return rows