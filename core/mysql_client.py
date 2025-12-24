import mysql.connector
from dotenv import load_dotenv
import os
from mysql.connector import Error
from .db_colums import pickup_delivery_columns, masterorder_columns,orderlineitems_columns,status_events_columns
# from db_colums import pickup_delivery_columns, masterorder_columns,orderlineitems_columns,status_events_columns

load_dotenv()

def mysql_connect():
    """Connect to MySQL database using environment variables.
    
    Returns:
        mysql.connector.MySQLConnection if successful, None otherwise
    """
    print("Connecting to MySQL database...")
    
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
        # query = f"SELECT COUNT(*) AS total FROM `{table_name}`"
        query = f"SELECT COUNT(*) AS total FROM `{table_name}` WHERE oms_data_migration_status = 1"
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
    
    def get_master_order_w(self, table_name: str, start: int, end: int, stop_date :str) -> list:
        """Retrieve a range of order records with specific columns.
        
        Args:
            table_name: Name of the table to query
            start: Starting offset
            end: Ending offset (exclusive)
            
        Returns:
            List of order records within the specified range, or empty list on error
        """
        try:
            columns = ", ".join(masterorder_columns)
            query = f"""
                SELECT
                    {columns}
                FROM `{table_name}`
                where oms_data_migration_status = 0
                    and created_at < %s
                ORDER BY order_id ASC
                LIMIT %s, %s
            """
            limit = end - start

            self.cursor.execute(query, (stop_date,start,limit))
            return self.cursor.fetchall()
        except Exception as e:
            print(f"MySQL fetch error in master_order_w: {e}")
            return []
    
    def get_master_order(self, table_name: str, start: int, end: int, stop_date :str) -> list:
        """Retrieve a range of order records with specific columns.
        
        Args:
            table_name: Name of the table to query
            start: Starting offset
            end: Ending offset (exclusive)
            # where oms_data_migration_status = 1
        Returns:
            List of order records within the specified range, or empty list on error
        """
        try:
            columns = ", ".join(masterorder_columns)
            query = f"""
                SELECT {columns}
                FROM `{table_name}`
                where created_at < %s
                ORDER BY order_id ASC
                LIMIT %s, %s
            """
            limit = end - start

            self.cursor.execute(query, (stop_date, start ,limit))
            return self.cursor.fetchall()
        except Exception as e:
            print(f"MySQL fetch error in master_order: {e}")
            return []

    def get_master_order_date_range(
            self,
            table_name: str,
            start_date: str,
            end_date: str
    ) -> list:
        """
        Retrieve master order records within a date range.

        Args:
            table_name: Name of the table to query
            start_date: Start datetime (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
            end_date: End datetime (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)

        Returns:
            List of order records, or empty list on error
        """
        try:
            # columns = ", ".join(masterorder_columns)
            # columns = ", ".join(pickup_delivery_columns)
            # columns = ", ".join(status_events_columns)
            columns = ", ".join(orderlineitems_columns)

            query = f"""
                SELECT {columns}
                FROM `{table_name}`
                WHERE
                    created_at >= %s
                    AND created_at <= %s
                ORDER BY line_item_id ASC
            """

            self.cursor.execute(
                query,
                (start_date, end_date)
            )

            return self.cursor.fetchall()

        except Exception as e:
            print(f"MySQL fetch error in get_master_order: {e}")
            return []
        
    def get_pickup_delivery_items(self, table_name: str, start: int, end: int, stop_date :str) -> list:
        """Retrieve a range of order records with specific columns.
        
        Args:
            table_name: Name of the table to query
            start: Starting offset
            end: Ending offset (exclusive)
            
        Returns:
            List of order records within the specified range, or empty list on error
        """
        try:
            columns = ", ".join(pickup_delivery_columns)
            query = f"""
                SELECT {columns}
                FROM `{table_name}`
                where oms_data_migration_status = 1
                    and invoice_date < %s
                ORDER BY pickup_delivery_req_item_id ASC
                LIMIT %s, %s
            """
            limit = end - start
            self.cursor.execute(query, (stop_date,start,limit))
            return self.cursor.fetchall()
        except Exception as e:
            print(f"MySQL fetch error in pickup_delivery_items: {e}")
            return []

    def get_pickup_delivery_items_w(self, table_name: str, start: int, end: int,stop_date :str) -> list:
        """Retrieve a range of pickup/delivery item records with specific columns.
        
        Args:
            table_name: Name of the table to query
            start: Starting offset
            end: Ending offset (exclusive)
            
        Returns:
            List of pickup/delivery item records within the specified range, or empty list on error
        """
        try:
            # Convert list of column names to comma-separated string
            columns = ", ".join(pickup_delivery_columns)
            
            query = f"""
                SELECT {columns}
                FROM `{table_name}`
                WHERE oms_data_migration_status = 0
                    and invoice_date < %s
                ORDER BY pickup_delivery_req_item_id ASC
                LIMIT %s, %s
            """
            limit = end - start

            self.cursor.execute(query, (stop_date, start,limit))
            return self.cursor.fetchall()
        except Exception as e:
            print(f"MySQL fetch error in get_pickup_delivery_items_w: {e}")
            return []
    
    def get_status_events(self, table_name: str, start: int, end: int,stop_date :str) -> list:
        """Retrieve a range of order records with specific columns.
        
        Args:
            table_name: Name of the table to query
            start: Starting offset
            end: Ending offset (exclusive)
            
        Returns:
            List of order records within the specified range, or empty list on error
        """
        try:
            columns = ", ".join(status_events_columns)
            query = f"""
                SELECT {columns}
                FROM `{table_name}`
                where oms_data_migration_status = 0
                    and invoice_date < %s
                ORDER BY status_event_id ASC
                LIMIT %s, %s
            """
            limit = end - start
            self.cursor.execute(query, (stop_date,start,limit))
            return self.cursor.fetchall()
        except Exception as e:
            print(f"MySQL fetch error in status_events: {e}")
            return []
        
    def get_orderlineitems(self, table_name: str, start: int, end: int, stop_date: str) -> list:
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
                SELECT *
                FROM `{table_name}`
                where oms_data_migration_status = 0
                    and created_at < %s
                ORDER BY line_item_id ASC
                LIMIT %s, %s
            """
            limit = end - start

            self.cursor.execute(query, (stop_date, start,limit))
            return self.cursor.fetchall()
        except Exception as e:
            print(f"MySQL fetch error in get_range_ph_bi: {e}")
            return []

    def get_orderlineitems_test(self, table_name: str, start: int, end: int) -> list:
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
                    line_item_id,
                    order_line_item_id,
                    master_order_id,
                    master_sale_order_id,
                    delivery_from,
                    customer_status,
                    inventory_status,
                    erp_item_code,
                    created_at
                FROM `{table_name}`
                where oms_data_migration_status = 1
                ORDER BY line_item_id ASC
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

    from datetime import datetime

    def get_orderlineitems_incremental(
            self,
            table_name: str,
            last_date: datetime,
            current_date: datetime,

    ) -> list:

        """
        Retrieve order line items created between last_date and current_date for incremental migration.
        
        Args:
            table_name: Name of the table to query
            last_date: Starting datetime (records with created_at > this)
            current_date: Ending datetime boundary (records with created_at <= this)
            limit: Maximum number of records to retrieve
            
        Returns:
            List of order line item records created between last_date and current_date, or empty list on error
        """
        print("Last_date",last_date,"current_date",current_date)
        try:
            query = f"""
                SELECT *
                FROM `{table_name}`
                WHERE
                    oms_data_migration_status = 0
                    AND created_at > %s
                    AND created_at < %s
                ORDER BY created_at ASC
                
            """
            self.cursor.execute(
                query,
                (last_date, current_date)
            )
            return self.cursor.fetchall()

        except Exception as e:
            print(f"MySQL fetch error in get_orderlineitems_incremental: {e}")
            return []

    def close(self) -> None:
        """Close database connection and cursor."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


# if __name__ == "__main__":
#     # Test code - run with: python -m core.mysql_client
#     ss = MysqlCatalog()
#     print("Testing get_pickup_delivery_items_w...")
#     results = ss.get_pickup_delivery_items_w(table_name="pickup_delivery_items_w", start=0, end=10)
#     print(results)
#     print(f"Retrieved {len(results)} records")
#     if results:
#         print(f"First record has {len(results[0])} columns")
#     ss.close()

# Other test examples (commented out):
# from datetime import datetime
#
# ss = MysqlCatalog()
# last_date = datetime(2024, 12, 1,0,0,0)
# current_date = datetime(2025, 12, 18,0,0,0)
# results = ss.get_orderlineitems_incremental(
#     table_name="orderlineitems",
#     last_date=last_date,
#     current_date=current_date
#
# )
# print(f"Retrieved {len(results)} records between {last_date} and {current_date}")
# if results:
#     print("First record:")
#     print(f"First record: {results[0]}")
#     print("Last record:")
#     print(f"Last record: {results[-1]}")
# ss.close()

# print("masterorders",ss.get_count(table_name="masterorders"))
# print("pickup_delivery_items",ss.get_count(table_name="pickup_delivery_items"))
# print("status_events",ss.get_count(table_name="status_events"))
# print("orderlineitems",ss.get_count(table_name="orderlineitems"))
# print("masterorders_w",ss.get_count(table_name="masterorders_w"))
# print("pickup_delivery_items_w",ss.get_count(table_name="pickup_delivery_items_w"))
# print("masterorders_view",ss.get_count(table_name="masterorders_view"))
# print(ss.get_schema(table_name="pickup_delivery_items"))
# print("#"*100)
# print(ss.get_schema(table_name="pickup_delivery_items_w"))


# ss = MysqlCatalog()
# print(ss.get_master_order_date_range(table_name="orderlineitems", start_date="2025-12-12 00:00:00", end_date="2025-12-23 23:59:59"))
