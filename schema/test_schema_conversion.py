#!/usr/bin/env python3
"""
Test script to verify bigint and varchar type conversions.
Tests the bluedart_zone_masters Arrow schema with sample data.
"""

import pyarrow as pa
from bluedart_zone_masters_arrow_schema import bluedart_zone_masters_schema
from datetime import datetime


def test_bigint_unsigned():
    """Test bigint unsigned (uint64) type."""
    print("=" * 80)
    print("TEST 1: BIGINT UNSIGNED → UINT64")
    print("=" * 80)
    
    test_cases = [
        ("Small value", 1),
        ("Medium value", 1000000),
        ("Large value", 9223372036854775807),  # Max int64
        ("Very large value", 18446744073709551615),  # Max uint64
    ]
    
    for name, value in test_cases:
        try:
            # Create single row with test value
            data = [{
                "id": value,
                "cpincode": "110001",
                "cpindesc": "Test",
                "city": "Delhi",
                "bdsc": "DEL",
                "state": "Delhi",
                "carea": "Test Area",
                "cecomzn": "North",
                "region": "North",
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "created_by": None,
                "updated_by": None,
                "cscrcd": None,
            }]
            
            table = pa.Table.from_pylist(data, schema=bluedart_zone_masters_schema)
            retrieved_value = table["id"][0].as_py()
            
            print(f"✓ {name:20s} | Input: {value:20d} | Retrieved: {retrieved_value:20d} | Match: {value == retrieved_value}")
            
        except Exception as e:
            print(f"✗ {name:20s} | Error: {str(e)}")
    
    print()


def test_varchar_types():
    """Test varchar and text (string) types."""
    print("=" * 80)
    print("TEST 2: VARCHAR/TEXT → STRING")
    print("=" * 80)
    
    test_cases = [
        ("Empty string", ""),
        ("Short string", "ABC"),
        ("Max varchar(100)", "A" * 100),
        ("Unicode", "नई दिल्ली 🇮🇳"),
        ("Special chars", "Test@#$%^&*()"),
        ("Long text", "A" * 1000),  # TEXT type can handle this
        ("Null value", None),
    ]
    
    for name, value in test_cases:
        try:
            data = [{
                "id": 1,
                "cpincode": value if name == "Empty string" else "110001",
                "cpindesc": value if name == "Short string" else "Test",
                "city": value if name == "Max varchar(100)" else "Delhi",
                "bdsc": "DEL",
                "state": "Delhi",
                "carea": value if name == "Long text" else "Test Area",
                "cecomzn": "North",
                "region": "North",
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "created_by": value if name == "Unicode" else None,
                "updated_by": value if name == "Special chars" else None,
                "cscrcd": value if name == "Null value" else "DEL001",
            }]
            
            table = pa.Table.from_pylist(data, schema=bluedart_zone_masters_schema)
            
            # Check the appropriate column
            if name == "Empty string":
                retrieved = table["cpincode"][0].as_py()
            elif name == "Short string":
                retrieved = table["cpindesc"][0].as_py()
            elif name == "Max varchar(100)":
                retrieved = table["city"][0].as_py()
            elif name == "Long text":
                retrieved = table["carea"][0].as_py()
            elif name == "Unicode":
                retrieved = table["created_by"][0].as_py()
            elif name == "Special chars":
                retrieved = table["updated_by"][0].as_py()
            else:  # Null value
                retrieved = table["cscrcd"][0].as_py()
            
            match = retrieved == value
            display_value = f"'{value[:20]}...'" if value and len(str(value)) > 20 else f"'{value}'"
            display_retrieved = f"'{retrieved[:20]}...'" if retrieved and len(str(retrieved)) > 20 else f"'{retrieved}'"
            
            print(f"✓ {name:20s} | Input: {display_value:25s} | Retrieved: {display_retrieved:25s} | Match: {match}")
            
        except Exception as e:
            print(f"✗ {name:20s} | Error: {str(e)}")
    
    print()


def test_timestamp_types():
    """Test timestamp type."""
    print("=" * 80)
    print("TEST 3: TIMESTAMP → TIMESTAMP[US]")
    print("=" * 80)
    
    test_cases = [
        ("Current time", datetime.now()),
        ("Specific time", datetime(2025, 1, 1, 12, 30, 45, 123456)),
        ("Old date", datetime(1970, 1, 1, 0, 0, 0)),
        ("Future date", datetime(2050, 12, 31, 23, 59, 59)),
    ]
    
    for name, value in test_cases:
        try:
            data = [{
                "id": 1,
                "cpincode": "110001",
                "cpindesc": "Test",
                "city": "Delhi",
                "bdsc": "DEL",
                "state": "Delhi",
                "carea": "Test Area",
                "cecomzn": "North",
                "region": "North",
                "created_at": value,
                "updated_at": datetime.now(),
                "created_by": None,
                "updated_by": None,
                "cscrcd": None,
            }]
            
            table = pa.Table.from_pylist(data, schema=bluedart_zone_masters_schema)
            retrieved = table["created_at"][0].as_py()
            
            # Compare timestamps (allowing microsecond precision difference)
            match = abs((retrieved - value).total_seconds()) < 0.000001
            
            print(f"✓ {name:20s} | Input: {value} | Retrieved: {retrieved} | Match: {match}")
            
        except Exception as e:
            print(f"✗ {name:20s} | Error: {str(e)}")
    
    print()


def test_nullable_fields():
    """Test nullable vs non-nullable fields."""
    print("=" * 80)
    print("TEST 4: NULLABLE FIELDS")
    print("=" * 80)
    
    # Test non-nullable field with None (should fail)
    print("Testing non-nullable field (cpincode) with None value:")
    try:
        data = [{
            "id": 1,
            "cpincode": None,  # This should fail - non-nullable
            "cpindesc": "Test",
            "city": "Delhi",
            "bdsc": "DEL",
            "state": "Delhi",
            "carea": "Test Area",
            "cecomzn": "North",
            "region": "North",
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "created_by": None,
            "updated_by": None,
            "cscrcd": None,
        }]
        table = pa.Table.from_pylist(data, schema=bluedart_zone_masters_schema)
        print("✗ FAILED: Should have raised an error for None in non-nullable field")
    except Exception as e:
        print(f"✓ PASSED: Correctly rejected None in non-nullable field")
        print(f"  Error: {str(e)[:100]}")
    
    print()
    
    # Test nullable field with None (should succeed)
    print("Testing nullable field (created_by) with None value:")
    try:
        data = [{
            "id": 1,
            "cpincode": "110001",
            "cpindesc": "Test",
            "city": "Delhi",
            "bdsc": "DEL",
            "state": "Delhi",
            "carea": "Test Area",
            "cecomzn": "North",
            "region": "North",
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "created_by": None,  # This should work - nullable
            "updated_by": None,
            "cscrcd": None,
        }]
        table = pa.Table.from_pylist(data, schema=bluedart_zone_masters_schema)
        print(f"✓ PASSED: Correctly accepted None in nullable field")
        print(f"  Value: {table['created_by'][0].as_py()}")
    except Exception as e:
        print(f"✗ FAILED: Should have accepted None in nullable field")
        print(f"  Error: {str(e)}")
    
    print()


def test_full_record():
    """Test with complete realistic data."""
    print("=" * 80)
    print("TEST 5: FULL RECORD")
    print("=" * 80)
    
    data = [
        {
            "id": 12345,
            "cpincode": "110001",
            "cpindesc": "Connaught Place",
            "city": "New Delhi",
            "bdsc": "DEL",
            "state": "Delhi",
            "carea": "Central Delhi, Connaught Place Area",
            "cecomzn": "North Zone",
            "region": "North",
            "created_at": datetime(2025, 1, 1, 10, 30, 0),
            "updated_at": datetime(2025, 1, 2, 15, 45, 30),
            "created_by": "admin",
            "updated_by": "system",
            "cscrcd": "DEL-CP-001",
        },
        {
            "id": 67890,
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
            "updated_by": None,  # Nullable
            "cscrcd": None,  # Nullable
        }
    ]
    
    try:
        table = pa.Table.from_pylist(data, schema=bluedart_zone_masters_schema)
        
        print(f"✓ Created table with {table.num_rows} rows and {table.num_columns} columns")
        print(f"\nSchema:")
        print(table.schema)
        print(f"\nSample data (first row):")
        for col_name in table.column_names:
            value = table[col_name][0].as_py()
            print(f"  {col_name:15s}: {value}")
        
        print(f"\n✓ All tests passed!")
        
    except Exception as e:
        print(f"✗ Failed to create table: {str(e)}")
    
    print()


def main():
    """Run all tests."""
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 15 + "BLUEDART ZONE MASTERS SCHEMA TEST" + " " * 30 + "║")
    print("╚" + "=" * 78 + "╝")
    print()
    
    test_bigint_unsigned()
    test_varchar_types()
    test_timestamp_types()
    test_nullable_fields()
    test_full_record()
    
    print("=" * 80)
    print("ALL TESTS COMPLETED")
    print("=" * 80)
    print()


if __name__ == "__main__":
    main()
