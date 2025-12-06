import csv
import json

csv_file = "json_backup/masterorders_202512032101.csv"
json_file = "json_backup/masterorders_202512032101_copy.json"

data = []

with open(csv_file, encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        data.append(row)

with open(json_file, "w", encoding='utf-8') as f:
    json.dump(data, f, indent=4)

print("CSV converted to JSON successfully!")

# import json
# import csv
#
# json_file = "json_backup/masterorders_202512032101.json"
# csv_file = "json_backup/masterorders_202512032101.csv"
#
# with open(json_file, "r", encoding="utf-8") as f:
#     data = json.load(f)
#
# # Case 1: JSON is a list of objects
# if isinstance(data, list):
#     rows = data
#
# # Case 2: JSON has nested key (like {"records": [...]})
# elif isinstance(data, dict):
#     # get first list inside the dict
#     for value in data.values():
#         if isinstance(value, list):
#             rows = value
#             break
#     else:
#         raise ValueError("JSON does not contain a list of rows")
#
# # Now write CSV
# with open(csv_file, "w", newline="", encoding="utf-8") as f:
#     writer = csv.DictWriter(f, fieldnames=rows[0].keys())
#     writer.writeheader()
#     writer.writerows(rows)
#
# print("JSON → CSV converted successfully!")