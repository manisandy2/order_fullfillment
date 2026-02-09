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


# def clean_rows(
#         rows: List[Dict[str, Any]],
#         boolean_fields: Optional[List[str]] = None,
#         timestamps_fields:Optional[List[str]] = None,
#         date_fields: Optional[List[str]] = None,
#         field_overrides: Optional[Dict[str, tuple]] = None
#     ) -> List[Dict[str, Any]]:
#     """
#     Clean and normalize row data for hub_masters schema compliance.
#
#     Args:
#         rows: List of row dictionaries.
#         boolean_fields: List of field names that should be normalized to boolean values.
#         timestamps_fields: List of field names that should be parsed/normalized as timestamps.
#         field_overrides: Mapping of field names to override tuples used to adjust values during cleaning.
#
#     Returns:
#         Cleaned list of row dictionaries.
#     """
#     boolean_fields = boolean_fields or []
#     timestamps_fields = timestamps_fields or []
#     date_fields = date_fields or []
#     field_overrides = field_overrides or {}
#
#     dt_formats = [
#         "%Y-%m-%d %H:%M:%S",
#         "%Y-%m-%dT%H:%M:%S",
#         "%d/%m/%Y %H:%M:%S",
#         "%Y-%m-%d",
#     ]
#
#     date_formats = [
#         "%Y-%m-%d",
#         "%d/%m/%Y",
#     ]
#
#     for row in rows:
#         # 1. Boolean Fields
#         for f in boolean_fields:
#             val = row.get(f)
#             if val is None:
#                 logger.warning(f"Required boolean field {f} is None, defaulting to False")
#                 row[f] = False
#             elif isinstance(val, bool):
#                 row[f] = val
#             elif isinstance(val, int):
#                 row[f] = bool(val)
#             elif isinstance(val, str):
#                 row[f] = val.lower() in ("1", "true", "yes", "on")
#             else:
#                 row[f] = False
#
#         # 2. Timestamp Fields
#         for f in timestamps_fields:
#             val = row.get(f)
#
#             if val is None or val == "":
#                 logger.info(f"Required timestamp {f} is None, using current timestamp")
#                 row[f] = datetime.now()
#                 continue
#
#             if isinstance(val, datetime):
#                 continue
#
#             # Try multiple formats
#             parsed = None
#             for fmt in dt_formats:
#                 try:
#                     parsed = datetime.strptime(val, fmt)
#                     break
#                 except (ValueError, TypeError):
#                     pass
#
#             if parsed is None:
#                 logger.warning(f"Failed to parse timestamp {f}: {val}, using current timestamp")
#                 row[f] = datetime.now()
#             else:
#                 row[f] = parsed
#
#         # 3. Date Fields (date only)
#         for f in date_fields:
#             val = row.get(f)
#
#             if val is None or val == "":
#                 logger.info(f"Date field {f} is None, defaulting to today")
#                 row[f] = date.today()
#                 continue
#
#             if isinstance(val, date) and not isinstance(val, datetime):
#                 continue
#
#             parsed = None
#             for fmt in date_formats:
#                 try:
#                     parsed = datetime.strptime(str(val), fmt).date()
#                     break
#                 except (ValueError, TypeError):
#                     pass
#
#             row[f] = parsed if parsed else date.today()
#
#         # 4. String Fields (Everything else)
#         for key, val in row.items():
#             if key not in boolean_fields + timestamps_fields:
#                 # Check if this field override exists and is required
#                 if key in field_overrides:
#                     _, _, is_required = field_overrides[key]
#                     if val is None:
#                         if is_required:
#                             logger.warning(f"Required string field {key} is None, defaulting to empty string")
#                             row[key] = ""
#                         else:
#                             row[key] = None
#                     else:
#                         row[key] = str(val)
#                 else:
#                     # Generic handling for non-overridden fields
#                     row[key] = str(val) if val is not None else None
#
#     return rows

# def clean_rows(
#     rows: List[Dict[str, Any]],
#     boolean_fields: Optional[List[str]] = None,
#     timestamps_fields: Optional[List[str]] = None,
#     date_fields: Optional[List[str]] = None,
#     field_overrides: Optional[Dict[str, tuple]] = None,
# ) -> List[Dict[str, Any]]:
#     """
#     Clean and normalize row data for schema compliance
#     (MySQL → Arrow → Iceberg safe).
#
#     Args:
#         rows: List of row dictionaries
#         boolean_fields: Columns treated as booleans
#         timestamps_fields: Columns treated as datetime
#         date_fields: Columns treated as date
#         field_overrides: {field: (type, default, is_required)}
#
#     Returns:
#         Cleaned rows
#     """
#
#     # ----------------------------
#     # Defensive initialization
#     # ----------------------------
#     boolean_fields = set(boolean_fields or [])
#     timestamps_fields = set(timestamps_fields or [])
#     date_fields = set(date_fields or [])
#     field_overrides = field_overrides or {}
#
#     assert isinstance(field_overrides, dict), "field_overrides must be dict"
#
#     protected_fields = boolean_fields | timestamps_fields | date_fields
#
#     # ----------------------------
#     # Supported formats
#     # ----------------------------
#     dt_formats = [
#         "%Y-%m-%d %H:%M:%S",
#         "%Y-%m-%dT%H:%M:%S",
#         "%d/%m/%Y %H:%M:%S",
#         "%Y-%m-%d",
#     ]
#
#     date_formats = [
#         "%Y-%m-%d",
#         "%d/%m/%Y",
#     ]
#
#     # ----------------------------
#     # Row processing
#     # ----------------------------
#     for row in rows:
#
#         # 1️⃣ Boolean fields
#         for f in boolean_fields:
#             val = row.get(f)
#
#             if val is None:
#                 # logger.warning(f"{f} is None → False")
#                 row[f] = False
#             elif isinstance(val, bool):
#                 row[f] = val
#             elif isinstance(val, int):
#                 row[f] = bool(val)
#             elif isinstance(val, str):
#                 row[f] = val.strip().lower() in {"1", "true", "yes", "on"}
#             else:
#                 row[f] = False
#
#         # 2️⃣ Timestamp fields
#         for f in timestamps_fields:
#             val = row.get(f)
#
#             if val in (None, ""):
#                 logger.info(f"{f} missing → datetime.now()")
#                 row[f] = datetime.now()
#                 continue
#
#             if isinstance(val, datetime):
#                 continue
#
#             parsed = None
#             for fmt in dt_formats:
#                 try:
#                     parsed = datetime.strptime(str(val), fmt)
#                     break
#                 except (ValueError, TypeError):
#                     pass
#
#             if parsed is None:
#                 logger.warning(f"Invalid timestamp {f}: {val} → now()")
#                 row[f] = datetime.now()
#             else:
#                 row[f] = parsed
#
#         # 3️⃣ Date fields
#         for f in date_fields:
#             val = row.get(f)
#
#             if val in (None, ""):
#                 logger.info(f"{f} missing → date.today()")
#                 row[f] = date.today()
#                 continue
#
#             if isinstance(val, date) and not isinstance(val, datetime):
#                 continue
#
#             parsed = None
#             for fmt in date_formats:
#                 try:
#                     parsed = datetime.strptime(str(val), fmt).date()
#                     break
#                 except (ValueError, TypeError):
#                     pass
#
#             row[f] = parsed if parsed else date.today()
#
#         # 4️⃣ Remaining fields (strings / overrides)
#         for key, val in row.items():
#             if key in protected_fields:
#                 continue
#
#             if key in field_overrides:
#                 try:
#                    iceberg_type, arrow_type, is_required = field_overrides[key]
#                 except ValueError:
#                     raise ValueError(
#                         f"Invalid override tuple for '{key}', expected (_, _, is_required)"
#                     )
#
#                 if val is None:
#                     if is_required:
#                         logger.warning(f"{key} is required → ''")
#                         row[key] = ""
#                     else:
#                         row[key] = None
#                     continue
#
#                 # else:
#                 #     row[key] = str(val)
#                 if pa.types.is_string(arrow_type):
#                     if isinstance(val, bool):
#                         row[key] = "true" if val else "false"
#                     else:
#                         row[key] = str(val)
#                     continue
#                 row[key] = val
#
#             else:
#                 # row[key] = str(val) if val is not None else None
#                 # No override → default safe behavior
#                 if isinstance(val, bool):
#                     row[key] = "true" if val else "false"
#                 else:
#                     row[key] = str(val) if val is not None else None
#     return rows

# def clean_rows(
#     rows: List[Dict[str, Any]],
#     boolean_fields: Optional[List[str]] = None,
#     timestamps_fields: Optional[List[str]] = None,
#     date_fields: Optional[List[str]] = None,
#     field_overrides: Optional[Dict[str, tuple]] = None,
# ) -> List[Dict[str, Any]]:
#     """
#     Clean and normalize row data for schema compliance
#     (MySQL → Arrow → Iceberg safe).
#     """
#
#     boolean_fields = set(boolean_fields or [])
#     timestamps_fields = set(timestamps_fields or [])
#     date_fields = set(date_fields or [])
#     field_overrides = field_overrides or {}
#
#     protected_fields = boolean_fields | timestamps_fields | date_fields
#
#     dt_formats = (
#         "%Y-%m-%d %H:%M:%S",
#         "%Y-%m-%dT%H:%M:%S",
#         "%d/%m/%Y %H:%M:%S",
#         "%Y-%m-%d",
#     )
#
#     date_formats = (
#         "%Y-%m-%d",
#         "%d/%m/%Y",
#     )
#
#     for row in rows:
#         now = datetime.utcnow()
#         today = now.date()
#
#         # 1️⃣ Boolean fields
#         for f in boolean_fields:
#             val = row.get(f)
#
#             if val is None:
#                 row[f] = False
#             elif isinstance(val, bool):
#                 row[f] = val
#             elif isinstance(val, (int, float)):
#                 row[f] = bool(val)
#             elif isinstance(val, str):
#                 row[f] = val.strip().lower() in {"1", "true", "yes", "on"}
#             else:
#                 row[f] = False
#
#         # 2️⃣ Timestamp fields
#         for f in timestamps_fields:
#             val = row.get(f)
#
#             if val in (None, ""):
#                 row[f] = now
#                 continue
#
#             if isinstance(val, datetime):
#                 continue
#
#             parsed = None
#             for fmt in dt_formats:
#                 try:
#                     parsed = datetime.strptime(str(val), fmt)
#                     break
#                 except (ValueError, TypeError):
#                     pass
#
#             row[f] = parsed or now
#
#         # 3️⃣ Date fields
#         for f in date_fields:
#             val = row.get(f)
#
#             if val in (None, ""):
#                 row[f] = today
#                 continue
#
#             if isinstance(val, datetime):
#                 row[f] = val.date()
#                 continue
#
#             if isinstance(val, date):
#                 continue
#
#             parsed = None
#             for fmt in date_formats:
#                 try:
#                     parsed = datetime.strptime(str(val), fmt).date()
#                     break
#                 except (ValueError, TypeError):
#                     pass
#
#             row[f] = parsed or today
#
#         # 4️⃣ Remaining fields + overrides
#         for key, val in row.items():
#             if key in protected_fields:
#                 continue
#
#             if key in field_overrides:
#                 try:
#                     _, default, is_required = field_overrides[key]
#                 except ValueError:
#                     raise ValueError(
#                         f"Invalid override tuple for '{key}', expected (type, default, is_required)"
#                     )
#
#                 if val is None:
#                     row[key] = default if is_required else None
#                 else:
#                     row[key] = str(val)
#
#             else:
#                 if val is None:
#                     row[key] = None
#                 elif isinstance(val, (str, int, float)):
#                     row[key] = str(val)
#                 else:
#                     # leave complex types untouched (JSON, dict, list)
#                     row[key] = val
#
#     return rows

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