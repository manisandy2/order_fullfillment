System Architecture
Overview

The Order Fulfillment Data Pipeline is designed to ingest operational data from MySQL transactional systems into Apache Iceberg tables for analytical workloads and long-term storage.

The system performs:

Incremental data ingestion

Data cleaning and normalization

Schema enforcement

Chunk-based ingestion for memory safety

Error tracking and retry capability

The architecture ensures scalable, fault-tolerant ingestion for 40+ tables.

High-Level Architecture
Operational MySQL Database
        │
        │  Incremental Queries
        ▼
MySQL Catalog Layer
        │
        ▼
Data Cleaning Layer
        │
        ▼
Chunk Processing Engine
        │
        ▼
PyArrow Conversion
        │
        ▼
Apache Iceberg Tables
        │
        ▼
Object Storage (S3 Compatible)
System Components
1. MySQL Source Layer

The source system is an operational MySQL database containing order fulfillment data.

Example tables:

masterorders

orderlineitems

pickup_delivery_items

drivers

shipments

manifests

Data is extracted using date-range queries.

Example:

SELECT *
FROM masterorders
WHERE created_at BETWEEN start_date AND end_date
ORDER BY order_id ASC;
2. MySQL Catalog Layer

Location:

core/between_date.py

Responsibilities:

Manage MySQL connections

Execute date-range queries

Stream rows using batch fetching

Provide table-specific fetch functions

Example:

with MysqlCatalog() as mysql:
    rows = mysql.get_table_date_between(
        table_name,
        start_date,
        end_date
    )

Key features:

Connection management

Automatic reconnection

Streaming support

Configurable batch size

3. Data Cleaning Layer

Location:

date_between/utility.py

Before ingestion, raw MySQL rows are normalized.

Typical issues handled:

Problem	Example
Invalid timestamps	"2026-01-01"
Boolean strings	"0" / "1"
Empty values	" "
Incorrect types	"123" instead of integer

Cleaning function:

clean_rows(rows, boolean_fields, timestamps_fields, field_overrides)

This ensures the data matches the Iceberg schema requirements.

4. Chunk Processing Engine

Large datasets are split into chunks to prevent memory overload.

Example:

chunks = [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]

Each chunk is processed independently.

Benefits:

Memory safety

Parallelizable processing

Failure isolation

5. Arrow Conversion Layer

Location:

process_chunk()

Rows are converted into PyArrow tables.

Example:

arrow_table = process_chunk(chunk, arrow_schema)

Advantages:

Efficient columnar data format

Compatible with Apache Iceberg

High performance serialization

6. Apache Iceberg Storage Layer

Destination tables are stored as Apache Iceberg tables.

Example append operation:

tbl.append(arrow_table)

Iceberg provides:

Schema evolution

Partitioning

Snapshot isolation

Time travel queries

7. Object Storage Layer

Iceberg tables are stored in S3-compatible object storage.

Example configuration:

endpoint_url
ACCESS_KEY_ID
SECRET_ACCESS_KEY

Benefits:

Scalable storage

Low cost

High availability

Incremental Ingestion Strategy

The system avoids full table scans by tracking the last ingested timestamp.

Process:

Retrieve last ingested value

get_last_date_value()

Fetch new records

WHERE created_at BETWEEN start_date AND end_date

Append to Iceberg table

This ensures:

No duplicate ingestion

Faster pipelines

Efficient storage usage

Error Handling

If a chunk fails during ingestion:

The chunk is recorded

Error details are logged

Failed data can be reprocessed

Example error metadata:

chunk_index
chunk_data
error_message

Error logs are stored in:

logs/iceberg_upload
Memory Management

The pipeline includes safeguards to prevent memory exhaustion.

Techniques used:

check_memory_limit()
gc.collect()

These are especially important when running on:

Docker containers

Cloud Run

Kubernetes

Pipeline Execution Flow

Example pipeline:

bluedart_zone_masters_between_date()

Execution steps:

Get last timestamp
        │
        ▼
Fetch MySQL rows
        │
        ▼
Clean rows
        │
        ▼
Create Arrow schema
        │
        ▼
Split into chunks
        │
        ▼
Convert to Arrow tables
        │
        ▼
Append to Iceberg
        │
        ▼
Handle failed chunks
Scalability Design

The system is designed to scale for dozens of tables and large datasets.

Key design principles:

Modular ingestion pipelines

Config-driven table ingestion

Streaming MySQL fetch

Chunk-based processing

Schema-driven transformation

Future Architecture Improvements

Planned enhancements:

Table registry configuration

Dynamic schema generation

Parallel ingestion workers

Airflow orchestration

Kubernetes job scheduling

Observability and metrics

Summary

The system provides a robust ingestion framework for operational data.

Key capabilities:

Incremental ingestion

Schema enforcement

Memory-safe processing

Scalable architecture

Iceberg-compatible storage

This architecture allows the platform to efficiently ingest and maintain order fulfillment data pipelines at scale.