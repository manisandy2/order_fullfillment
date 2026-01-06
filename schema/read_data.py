from pathlib import Path
import pandas as pd

schema_path = Path("/Users/mac-1/Desktop/order_fulfillment/original schema")

existing_tables = [
    "masterorders",
    "masterorders_w",
    "pickup_delivery_items",
    "pickup_delivery_items_w",
    "orderlineitems",
    "status_events"
]
for file_path in schema_path.iterdir():

    # Skip directories
    if not file_path.is_file():
        continue

    # Only JSON files
    if file_path.suffix.lower() != ".json":
        continue

    table_name = file_path.stem  # SAFE way

    if table_name in existing_tables:
        continue

    print("#" * 100)
    print(f"📄 New schema found: {table_name}")

    try:
        df = pd.read_json(file_path)
        print(df)

    except ValueError as e:
        print(f"❌ Invalid JSON → {file_path.name}: {e}")

    except ParserError as e:
        print(f"❌ Parsing error → {file_path.name}: {e}")

    except Exception as e:
        print(f"❌ Unexpected error → {file_path.name}: {e}")


