
from last_value.utility import get_last_date_value,insert_last_value
import json

with open("table_list.json", "r") as f:
    data = json.load(f)

namespace = data["namespace"]
tables = data["table_name"]
column = data["column"]

print("Tables:", tables)

for table in tables:
    result = insert_last_value(namespace, table, column)
    print(result)