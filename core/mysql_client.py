import mysql.connector
from dotenv import load_dotenv
import os
from mysql.connector import Error

load_dotenv()

def mysql_connect():
    """Connect to MySQL database using environment variables.
    
    Returns:
        mysql.connector.MySQLConnection if successful, None otherwise
    """
    print("Connecting to MySQL database...")
    # print("Host",os.getenv("HOST"))
    # print("USERNAME",os.getenv("USERNAME"))
    # print(os.getenv("PASSWORD"))
    # print(os.getenv("DATABASE"))
    try:
        conn = mysql.connector.connect(
            host=os.getenv("HOST"),
            user=os.getenv("USERNAME"),
            password=os.getenv("PASSWORD"),
            database=os.getenv("DATABASE"),
            port=int(os.getenv("PORT", 3306))
        )
        if conn.is_connected():
            print("MySQL connection established")
            return conn
        else:
            raise ConnectionError("MySQL connection could not be established")
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None



class MysqlCatalog:
    """MySQL database catalog for querying table information."""
    
    def __init__(self) -> None:
        """Initialize MySQL catalog connection."""
        self.conn = mysql_connect()
        if self.conn is None:
            raise ConnectionError("Failed to connect to MySQL database")
        self.cursor = self.conn.cursor(dictionary=True)


    def get_all_value(self, table_name: str) -> list:
        """Retrieve all values from a table.
        
        Args:
            table_name: Name of the table to query
            
        Returns:
            List of all rows from the table
        """
        query = f"SELECT * FROM `{table_name}`"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_count(self, table_name: str) -> int:
        """Get total count of rows in a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Total number of rows in the table
        """
        query = f"SELECT COUNT(*) AS total FROM `{table_name}`"
        self.cursor.execute(query)
        row = self.cursor.fetchone()
        return int(row["total"])


    def get_describe(self, table_name: str):
        """Get table structure/schema description.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of column descriptions
        """
        query = f"DESCRIBE `{table_name}`"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_range(self, table_name: str, start: int, end: int):
        """Retrieve a range of rows from a table.
        
        Args:
            table_name: Name of the table
            start: Starting offset
            end: Ending offset (exclusive)
            
        Returns:
            List of rows within the specified range
        """
        query = f"SELECT * FROM `{table_name}` LIMIT %s, %s"
        self.cursor.execute(query, (start, end - start))
        return self.cursor.fetchall()
    
    def get_range_ph_bi(self, table_name: str, start: int, end: int) -> list:
        """Retrieve a range of order records with specific columns.
        
        Args:
            table_name: Name of the table to query
            start: Starting offset
            end: Ending offset (exclusive)
            
        Returns:
            List of order records within the specified range, or empty list on error
        """
        try:
            query = f"""
                SELECT
                    *
                FROM `{table_name}`
                ORDER BY order_id ASC
                LIMIT %s, %s
            """
            self.cursor.execute(query, (start, end - start))
            return self.cursor.fetchall()
        except Exception as e:
            print(f"MySQL fetch error in get_range_ph_bi: {e}")
            return []

    def get_schema(self, table_name: str) -> list:
        """Get the schema/structure of a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of dictionaries containing column information
        """
        query = f"SHOW COLUMNS FROM `{table_name}`"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        
        schema = []
        for col in rows:
            # print(col)
            schema.append({
                "name": col["Field"],
                "type": col["Type"],
                "nullable": col["Null"] == "YES",
                "key": col["Key"],  # PRI, MUL, UNI
                "default": col["Default"],
                "extra": col["Extra"],  # auto_increment, etc.
            })
        return schema

    def close(self) -> None:
        """Close database connection and cursor."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


ss = MysqlCatalog()
print(ss.get_count(table_name="orderlineitems"))
print(ss.get_schema(table_name="orderlineitems"))