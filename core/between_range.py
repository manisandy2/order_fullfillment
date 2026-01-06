from .mysql_client import mysql_connect
from .db_colums import bluedart_zone_masters_columns,courier_masters_columns

class MydatabaseRange:
    def __init__(self):
        self.db = mysql_connect()
        if self.db is None:
            raise RuntimeError("Database connection failed")
        self.cursor = self.db.cursor(dictionary=True)

    def get_bluedart_zone_masters(self, table_name, start: int, end: int):
        try:

            columns = ", ".join(bluedart_zone_masters_columns)
            query = f"""
                SELECT {columns}
                FROM `{table_name}`
                ORDER BY id ASC
                LIMIT %s, %s
            """
            limit = end - start

            self.cursor.execute(query, (start, limit))
            return self.cursor.fetchall()

        except Exception as e:
            print(f"MySQL fetch error in master_order: {e}")
            return []

    def get_courier_masters(self, table_name, start: int, end: int):
        try:

            columns = ", ".join(courier_masters_columns)
            query = f"""
                SELECT {columns}
                FROM `{table_name}`
                ORDER BY code ASC
                LIMIT %s, %s
            """
            limit = end - start

            self.cursor.execute(query, (start, limit))
            return self.cursor.fetchall()

        except Exception as e:
            print(f"MySQL fetch error in master_order: {e}")
            return []