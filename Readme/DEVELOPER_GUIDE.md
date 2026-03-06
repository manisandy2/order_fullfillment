Developer Guide
Introduction

This guide explains how developers can work with the Order Fulfillment Data Ingestion System.

The project ingests operational data from MySQL into Apache Iceberg tables using a reusable ingestion framework.

Developers can easily:

Add new table pipelines

Modify schemas

Extend ingestion utilities

Debug ingestion jobs

Development Environment Setup
1. Clone the Repository
git clone https://github.com/manisandy2/order_fullfillment.git
cd order_fullfillment
2. Create Virtual Environment
python -m venv venv
source venv/bin/activate

Windows:

venv\Scripts\activate
3. Install Dependencies
pip install -r requirements.txt

Main dependencies:

Library	Purpose
mysql-connector-python	MySQL connectivity
pyarrow	Arrow table conversion
pyiceberg	Iceberg table operations
boto3	S3 object storage
python-dotenv	Environment variables
Environment Configuration

Create a .env file:

HOST=
USERNAME=
PASSWORD=
DATABASE=
PORT=3306

ACCESS_KEY_ID=
SECRET_ACCESS_KEY=
ENDPOINT=

These values configure:

MySQL connection

S3 object storage

Iceberg catalog access

Project Structure
order_fullfillment/

core/
    between_date.py
    MySQL connection & fetch logic

date_between/
    utility.py
    Data cleaning utilities

schemas/
    columns/
        MySQL column lists
    iceberg/
        Iceberg schema definitions

jobs/
    Table-specific ingestion scripts

logs/
    Error logs for failed ingestion
How the Pipeline Works

Each ingestion job performs:

Identify the last ingested timestamp

Fetch new rows from MySQL

Clean data

Convert to Arrow tables

Append to Iceberg

Log failures

Pipeline example:

rows = fetch_mysql_date_range(...)
clean_rows(rows)
arrow_table = process_chunk(...)
tbl.append(arrow_table)
Running an Ingestion Job

Example job:

python bluedart_zone_masters_between_date.py

Example output:

Initial Memory: 180 MB

{
  "rows_fetched": 404,
  "chunks_total": 1,
  "chunks_failed": 0,
  "status": "COMPLETED"
}

Final Memory: 210 MB
Adding a New Table Pipeline

To ingest a new MySQL table, follow these steps.

Step 1 — Define Column List

Create a new file:

schemas/columns/new_table_columns.py

Example:

new_table_columns = [
    "id",
    "name",
    "created_at",
    "updated_at"
]

Column lists ensure:

predictable query behavior

schema compatibility

Step 2 — Define Iceberg Schema

Create file:

schemas/iceberg/new_table_schema.py

Example:

FIELD_OVERRIDES = {
    "id": (LongType(), pa.int64(), True),
    "name": (StringType(), pa.string(), False),
    "created_at": (TimestampType(), pa.timestamp("ms"), True),
}
Step 3 — Add MySQL Fetch Function

Edit:

core/between_date.py

Add a method:

def get_new_table_date_between(self, table_name, start_date, end_date):
    return self._fetch_date_range(
        table_name,
        start_date,
        end_date,
        db_columns.new_table_columns,
        "created_at",
        "id",
    )
Step 4 — Create Job File

Create:

jobs/new_table_between_date.py

Example:

def run():
    return new_table_between_date()

if __name__ == "__main__":
    print(run())
Coding Standards

Developers should follow these guidelines:

Naming
Item	Convention
Files	snake_case
Functions	snake_case
Classes	PascalCase

Example:

bluedart_zone_masters_between_date()
Logging

Always log errors clearly:

logger.error("Failed to append chunk")
Memory Safety

Large datasets must be processed in chunks.

Example:

chunks = [rows[i:i+1000] for i in range(0, len(rows), 1000)]
Error Handling

If a chunk fails:

the chunk data is saved

error metadata is recorded

pipeline continues processing

Example stored fields:

Field	Description
chunk_index	failed chunk number
chunk_data	original data
error	error message

Logs location:

logs/iceberg_upload/
Debugging Pipelines

Common issues:

Arrow Type Error

Example:

ArrowInvalid: Could not convert string to int

Solution:

Check FIELD_OVERRIDES.

Timestamp Parsing Errors

Fix using:

clean_rows()
Memory Errors

Reduce chunk size:

chunk_size = 500
Best Practices

Developers should follow these practices:

Use explicit column lists

Avoid SELECT *

Process large datasets in chunks

Validate schemas before ingestion

Keep pipelines modular

Contribution Workflow

Create a new branch

git checkout -b feature/new-pipeline

Implement changes

Run ingestion tests

Create pull request

Future Improvements

Possible improvements developers may work on:

dynamic schema detection

table registry configuration

streaming ingestion

Airflow orchestration

monitoring dashboards

Maintainer

Manikandan R
Backend / Data Engineering