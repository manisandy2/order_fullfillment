"""
Usage examples for bluedart_zone_masters schemas.
Demonstrates how to use both PyArrow and PyIceberg schemas.
"""

from bluedart_zone_masters_schema import (
    bluedart_zone_masters_arrow_schema,
    bluedart_zone_masters_iceberg_schema,
    get_arrow_schema,
    get_iceberg_schema,
)
import pyarrow as pa
from datetime import datetime


# ============================================================================
# EXAMPLE 1: Create PyArrow Table from Data
# ============================================================================

def example_create_arrow_table():
    """Create an Arrow table from sample data."""
    print("=" * 80)
    print("EXAMPLE 1: Create PyArrow Table")
    print("=" * 80)
    
    # Sample data
    data = [
        {
            "id": 1,
            "cpincode": "110001",
            "cpindesc": "Connaught Place",
            "city": "New Delhi",
            "bdsc": "DEL",
            "state": "Delhi",
            "carea": "Central Delhi, Connaught Place Area",
            "cecomzn": "North Zone",
            "region": "North",
            "created_at": datetime(2025, 1, 1, 10, 0, 0),
            "updated_at": datetime(2025, 1, 2, 15, 30, 0),
            "created_by": "admin",
            "updated_by": "system",
            "cscrcd": "DEL-CP-001",
        },
        {
            "id": 2,
            "cpincode": "400001",
            "cpindesc": "Fort",
            "city": "Mumbai",
            "bdsc": "BOM",
            "state": "Maharashtra",
            "carea": "South Mumbai, Fort Area",
            "cecomzn": "West Zone",
            "region": "West",
            "created_at": datetime(2025, 1, 1, 11, 0, 0),
            "updated_at": datetime(2025, 1, 2, 16, 0, 0),
            "created_by": "admin",
            "updated_by": None,
            "cscrcd": None,
        }
    ]
    
    # Create Arrow table
    arrow_table = pa.Table.from_pylist(data, schema=bluedart_zone_masters_arrow_schema)
    
    print(f"✓ Created Arrow table with {arrow_table.num_rows} rows")
    print(f"✓ Schema: {arrow_table.schema}")
    print()
    
    return arrow_table


# ============================================================================
# EXAMPLE 2: Create Iceberg Table
# ============================================================================

def example_create_iceberg_table():
    """Create an Iceberg table using the schema."""
    print("=" * 80)
    print("EXAMPLE 2: Create Iceberg Table")
    print("=" * 80)
    
    namespace = "order_fulfillment"
    table_name = "bluedart_zone_masters"
    table_identifier = f"{namespace}.{table_name}"
    
    try:
        catalog = get_catalog_client()
        
        # Create table
        table = catalog.create_table(
            table_identifier,
            schema=bluedart_zone_masters_iceberg_schema
        )
        
        print(f"✓ Created Iceberg table: {table_identifier}")
        print(f"✓ Schema: {table.schema()}")
        print()
        
        return table
        
    except Exception as e:
        print(f"✗ Error creating table: {str(e)}")
        print("  (Table may already exist)")
        print()
        return None


# ============================================================================
# EXAMPLE 3: Load Existing Iceberg Table and Append Data
# ============================================================================

def example_append_to_iceberg():
    """Load existing Iceberg table and append Arrow data."""
    print("=" * 80)
    print("EXAMPLE 3: Append Data to Iceberg Table")
    print("=" * 80)
    
    namespace = "order_fulfillment"
    table_name = "bluedart_zone_masters"
    table_identifier = f"{namespace}.{table_name}"
    
    try:
        catalog = get_catalog_client()
        
        # Load existing table
        table = catalog.load_table(table_identifier)
        print(f"✓ Loaded table: {table_identifier}")
        
        # Create sample data
        data = [{
            "id": 3,
            "cpincode": "560001",
            "cpindesc": "Bangalore GPO",
            "city": "Bangalore",
            "bdsc": "BLR",
            "state": "Karnataka",
            "carea": "Central Bangalore",
            "cecomzn": "South Zone",
            "region": "South",
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "created_by": "api",
            "updated_by": "api",
            "cscrcd": "BLR-GPO-001",
        }]
        
        # Create Arrow table
        arrow_table = pa.Table.from_pylist(data, schema=bluedart_zone_masters_arrow_schema)
        
        # Append to Iceberg
        table.append(arrow_table)
        print(f"✓ Appended {arrow_table.num_rows} rows to Iceberg table")
        print()
        
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        print()


# ============================================================================
# EXAMPLE 4: Query Iceberg Table
# ============================================================================

def example_query_iceberg():
    """Query data from Iceberg table."""
    print("=" * 80)
    print("EXAMPLE 4: Query Iceberg Table")
    print("=" * 80)
    
    namespace = "order_fulfillment"
    table_name = "bluedart_zone_masters"
    table_identifier = f"{namespace}.{table_name}"
    
    try:
        catalog = get_catalog_client()
        table = catalog.load_table(table_identifier)
        
        # Scan table
        scan = table.scan(
            selected_fields=["id", "cpincode", "city", "state", "region"]
        )
        
        # Convert to Arrow and then Pandas
        arrow_table = scan.to_arrow()
        df = arrow_table.to_pandas()
        
        print(f"✓ Retrieved {len(df)} rows")
        print(f"\nSample data:")
        print(df.head())
        print()
        
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        print()


# ============================================================================
# EXAMPLE 5: MySQL to Iceberg Migration
# ============================================================================

def example_mysql_to_iceberg_migration():
    """Example of migrating data from MySQL to Iceberg."""
    print("=" * 80)
    print("EXAMPLE 5: MySQL to Iceberg Migration")
    print("=" * 80)
    
    from core.mysql_client import MysqlCatalog
    
    try:
        # Connect to MySQL
        mysql_client = MysqlCatalog()
        
        # Fetch data from MySQL
        rows = mysql_client.get_all_value("bluedart_zone_masters")
        print(f"✓ Fetched {len(rows)} rows from MySQL")
        
        # Convert to Arrow table
        arrow_table = pa.Table.from_pylist(rows, schema=bluedart_zone_masters_arrow_schema)
        print(f"✓ Converted to Arrow table")
        
        # Load Iceberg table
        catalog = get_catalog_client()
        table = catalog.load_table("order_fulfillment.bluedart_zone_masters")
        
        # Append to Iceberg
        table.append(arrow_table)
        print(f"✓ Appended {arrow_table.num_rows} rows to Iceberg")
        print()
        
        # Close MySQL connection
        mysql_client.close()
        
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        print()


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 20 + "BLUEDART ZONE MASTERS - USAGE EXAMPLES" + " " * 20 + "║")
    print("╚" + "=" * 78 + "╝")
    print()
    
    # Run examples
    example_create_arrow_table()
    
    # Uncomment to run other examples:
    # example_create_iceberg_table()
    # example_append_to_iceberg()
    # example_query_iceberg()
    # example_mysql_to_iceberg_migration()
    
    print("=" * 80)
    print("EXAMPLES COMPLETED")
    print("=" * 80)
    print()
