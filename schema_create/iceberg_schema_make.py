# import pandas as pd
# import json
# from pyiceberg.schema import NestedField
# from pyiceberg.types import (
#     StringType,
#     IntegerType,
#     LongType,
#     BooleanType,
#     FloatType,
#     DoubleType,
#     TimestampType,
#     BinaryType
# )
# ## iceberg schema
# schema = []
#
# def map_mysql_to_iceberg(mysql_type: str) -> str:
#     mysql_type = mysql_type.lower()
#
#     if mysql_type.startswith("enum"):
#         return "StringType()"
#
#     type_mapping = {
#         "varchar(100)": "StringType()",
#         "varchar(150)": "StringType()",
#         "varchar(200)": "StringType()",
#         "varchar(128)": "StringType()",
#         "varchar(50)": "StringType()",
#         "varchar(20)": "StringType()",
#         "varchar(25)": "StringType()",
#         "varchar(30)": "StringType()",
#         "varchar(32)": "StringType()",
#         "varchar(40)": "StringType()",
#         "varchar(64)": "StringType()",
#         "varchar(255)": "StringType()",
#         "varchar(10)": "StringType()",
#         "json": "StringType()",
#         "text": "StringType()",
#         "int": "IntegerType()",
#         "int(11)": "IntegerType()",
#         "bigint(11)": "IntegerType()",
#         "tinyint(1)": "BooleanType()",
#         "tinyint(4)": "BooleanType()",
#         "blob": "BinaryType()",
#         "bigint": "LongType()",
#         "float": "FloatType()",
#         "double": "DoubleType()",
#         "decimal": "DoubleType()",
#         "decimal(10,6)": "DoubleType()",
#         "boolean": "BooleanType()",
#         "timestamp": "TimestampType()",
#         "datetime": "TimestampType()",
#         "date": "DateType()",
#     }
#
#     return type_mapping.get(mysql_type)
#
#
# path = f"/Users/mac-1/Desktop/order_fulfillment/schema/vehicles.json"
# print(path)
# df = pd.read_json(path, lines=True)
#
# iceberg_fields = []
# field_id = 1
#
# for _, row in df.iterrows():
#     column_name = row["name"]
#     column_type = row["type"].lower()
#     nullable = bool(row["nullable"])
#
#     iceberg_type = map_mysql_to_iceberg(column_type)
#     # iceberg_type = TYPE_MAPPING.get(column_type)
#
#     if iceberg_type is None:
#         raise ValueError(f"Unsupported type: {column_type}")
#
#     print(
#         f'NestedField({field_id}, "{column_name}", {iceberg_type}, required={not nullable}),'
#     )
#
#     field_id += 1
#
# # Result
# for f in iceberg_fields:
#     print(f)

#######################################

import pandas as pd
path = f"/Users/mac-1/Desktop/order_fulfillment/schema/invoice_masters.json"
df = pd.read_json(path,lines=True)

# print(df)
print(path)
TIMESTAMP_FIELDS = df.loc[
    df["type"].str.lower().isin(["timestamp", "datetime"]),
    "name"
].tolist()

BOOLEAN_FIELDS = df.loc[
    df["type"].str.lower().isin(["boolean"]),
    "name"
].tolist()

INTEGER_FIELDS = df.loc[
    df["type"].str.lower().isin(["int","int(11)","integer"]),
    "name"
].tolist()


VARCHAR_FIELDS = df.loc[
    df["type"]
      .str.lower()
      .str.contains(r"varchar|char|text|enum"),
    "name"
].tolist()

REQUIRED_FIELDS = df.loc[
    df["nullable"] == False,   # NOT NULL columns
    "name"
].tolist()

print("REQUIRED_FIELDS =", REQUIRED_FIELDS)

print("TIMESTAMP_FIELDS =", TIMESTAMP_FIELDS)
print("BOOLEAN_FIELDS =", BOOLEAN_FIELDS)
print("INTEGER_FIELDS =", INTEGER_FIELDS)
print("VARCHAR_FIELDS =", VARCHAR_FIELDS)


###########################################

# import pandas as pd
# import pyarrow as pa
# from pyiceberg.types import (
#     BooleanType, LongType, DoubleType, DateType, IntegerType,
#     TimestampType, StringType, NestedField
# )
# path = f"/Users/mac-1/Desktop/order_fulfillment/schema/intransit_shipments.json"
# df = pd.read_json(path,lines=True)
# print(df)
#
# varchar_fields = ["varchar(100)", "varchar(50)", "varchar(255)", "char", "text", "enum"]
# integer_fields = ["int", "int(11)", "integer"]
# timestamp_fields = ["timestamp", "datetime"]
# boolean_fields = ["boolean"]
# float_fields = ["float"]
# double_fields = ["double"]
#
# FIELD_OVERRIDES = {}
#
# for _, row in df.iterrows():
#     name = row["name"]
#     type = row["type"]
#     nullable = bool(row["nullable"])
#     # print(name, type, nullable)
#     if type in varchar_fields:
#         # print("null",nullable)
#
#         if nullable:
#             # Primary / required identifiers
#             FIELD_OVERRIDES[name] = (StringType(), pa.string(), False)
#             # Nullable string fields
#         else:
#             FIELD_OVERRIDES[name] = (StringType(), pa.string(), True)
#     elif type in integer_fields:
#         if nullable:
#             FIELD_OVERRIDES[name] = (LongType(), pa.int64(), False)
#         else:
#             FIELD_OVERRIDES[name] = (LongType(), pa.int64(), True)
#     elif type in float_fields:
#         if nullable:
#             FIELD_OVERRIDES[name] = (DoubleType(), pa.float64(), False)
#         else:
#             FIELD_OVERRIDES[name] = (DoubleType(), pa.float64(), True)
#     elif type == "double":
#         if nullable:
#             FIELD_OVERRIDES[name] = (DoubleType(), pa.float64(), False)
#         else:
#             FIELD_OVERRIDES[name] = (DoubleType(), pa.float64(), True)
#     elif type == "boolean":
#         if nullable:
#             FIELD_OVERRIDES[name] = (BooleanType(), pa.bool_(), False)
#         else:
#             FIELD_OVERRIDES[name] = (BooleanType(), pa.bool_(), True)
#     elif type == "timestamp" or type == "datetime":
#         if nullable:
#             FIELD_OVERRIDES[name] = (TimestampType(), pa.timestamp("ms"), False)
#         else:
#             FIELD_OVERRIDES[name] = (TimestampType(), pa.timestamp("ms"), True)
#     elif type == "date":
#         if nullable:
#             FIELD_OVERRIDES[name] = (DateType(), pa.date32(), False)
#         else:
#             FIELD_OVERRIDES[name] = (DateType(), pa.date32(), True)
#
# print(FIELD_OVERRIDES)