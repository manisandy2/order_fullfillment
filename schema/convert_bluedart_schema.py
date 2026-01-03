#!/usr/bin/env python3
"""
Convert MySQL schema JSON to PyArrow schema for bluedart_zone_masters table.
"""

import json
import pyarrow as pa
from pathlib import Path


def mysql_type_to_arrow(mysql_type: str, nullable: bool) -> pa.DataType:
    """
    Convert MySQL type to PyArrow type.
    
    Args:
        mysql_type: MySQL column type (e.g., 'varchar(100)', 'bigint(20) unsigned')
        nullable: Whether the column is nullable
        
    Returns:
        PyArrow DataType
    """
    # Normalize type string
    mysql_type = mysql_type.lower().strip()
    
    # Integer types
    if 'bigint' in mysql_type:
        if 'unsigned' in mysql_type:
            return pa.uint64()
        return pa.int64()
    elif 'int' in mysql_type:
        if 'unsigned' in mysql_type:
            return pa.uint32()
        return pa.int32()
    elif 'tinyint' in mysql_type:
        if 'unsigned' in mysql_type:
            return pa.uint8()
        return pa.int8()
    elif 'smallint' in mysql_type:
        if 'unsigned' in mysql_type:
            return pa.uint16()
        return pa.int16()
    
    # String types
    elif mysql_type.startswith('varchar') or mysql_type.startswith('char'):
        return pa.string()
    elif mysql_type == 'text' or mysql_type == 'longtext' or mysql_type == 'mediumtext':
        return pa.string()
    
    # Date/Time types
    elif mysql_type == 'timestamp' or mysql_type == 'datetime':
        return pa.timestamp('us')  # microsecond precision
    elif mysql_type == 'date':
        return pa.date32()
    elif mysql_type == 'time':
        return pa.time64('us')
    
    # Numeric types
    elif mysql_type.startswith('decimal') or mysql_type.startswith('numeric'):
        # Extract precision and scale if available
        # For now, use double as a safe default
        return pa.float64()
    elif mysql_type == 'float':
        return pa.float32()
    elif mysql_type == 'double':
        return pa.float64()
    
    # Boolean
    elif mysql_type == 'boolean' or mysql_type == 'bool':
        return pa.bool_()
    
    # Binary types
    elif mysql_type.startswith('blob') or mysql_type.startswith('binary'):
        return pa.binary()
    
    # JSON
    elif mysql_type == 'json':
        return pa.string()  # Store as string, parse as needed
    
    # Default fallback
    else:
        print(f"Warning: Unknown MySQL type '{mysql_type}', defaulting to string")
        return pa.string()


def convert_schema(json_file_path: str) -> pa.Schema:
    """
    Convert MySQL schema JSON file to PyArrow schema.
    
    Args:
        json_file_path: Path to the JSON file with MySQL schema
        
    Returns:
        PyArrow Schema object
    """
    fields = []
    
    with open(json_file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            # Parse JSON line
            column = json.loads(line)
            
            name = column['name']
            mysql_type = column['type']
            nullable = column['nullable']
            
            # Convert to Arrow type
            arrow_type = mysql_type_to_arrow(mysql_type, nullable)
            
            # Create field
            field = pa.field(name, arrow_type, nullable=nullable)
            fields.append(field)
            
            print(f"✓ {name:20s} {mysql_type:25s} → {arrow_type} (nullable={nullable})")
    
    return pa.schema(fields)


def main():
    """Main conversion function."""
    # Input file
    schema_file = Path(__file__).parent / "bluedart_zone_masters.json"
    
    print("=" * 80)
    print("MySQL to PyArrow Schema Conversion")
    print("=" * 80)
    print(f"Input file: {schema_file}")
    print()
    
    # Convert schema
    arrow_schema = convert_schema(schema_file)
    
    print()
    print("=" * 80)
    print("PyArrow Schema:")
    print("=" * 80)
    print(arrow_schema)
    
    print()
    print("=" * 80)
    print("Python Code to Use:")
    print("=" * 80)
    print()
    print("import pyarrow as pa")
    print()
    print("bluedart_zone_masters_schema = pa.schema([")
    for field in arrow_schema:
        nullable_str = f", nullable={field.nullable}" if field.nullable else ""
        print(f"    pa.field('{field.name}', pa.{field.type}{nullable_str}),")
    print("])")
    print()
    
    # Save to Python file
    output_file = Path(__file__).parent / "bluedart_zone_masters_arrow_schema.py"
    with open(output_file, 'w') as f:
        f.write('"""PyArrow schema for bluedart_zone_masters table."""\n\n')
        f.write('import pyarrow as pa\n\n')
        f.write('bluedart_zone_masters_schema = pa.schema([\n')
        for field in arrow_schema:
            nullable_str = f", nullable={field.nullable}" if field.nullable else ""
            f.write(f'    pa.field("{field.name}", pa.{field.type}{nullable_str}),\n')
        f.write('])\n')
    
    print(f"✓ Schema saved to: {output_file}")
    print()


if __name__ == "__main__":
    main()
