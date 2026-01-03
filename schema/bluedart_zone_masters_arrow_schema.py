"""PyArrow schema for bluedart_zone_masters table."""

import pyarrow as pa

bluedart_zone_masters_schema = pa.schema([
    pa.field("id", pa.uint64()),
    pa.field("cpincode", pa.string()),
    pa.field("cpindesc", pa.string()),
    pa.field("city", pa.string()),
    pa.field("bdsc", pa.string()),
    pa.field("state", pa.string()),
    pa.field("carea", pa.string()),
    pa.field("cecomzn", pa.string()),
    pa.field("region", pa.string()),
    pa.field("created_at", pa.timestamp('us')),
    pa.field("updated_at", pa.timestamp('us')),
    pa.field("created_by", pa.string(), nullable=True),
    pa.field("updated_by", pa.string(), nullable=True),
    pa.field("cscrcd", pa.string(), nullable=True),
])

