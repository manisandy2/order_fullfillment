from .mysql_client import mysql_connect
from .db_colums import *

class MydatabaseRange:
    def __init__(self):
        self.db = mysql_connect()
        if self.db is None:
            raise RuntimeError("Database connection failed")
        self.cursor = self.db.cursor(dictionary=True)

    # bluedart_zone_masters
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
    # courier_masters
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
    # drivers
    def get_drivers(self, table_name, start: int, end: int):
        try:

            columns = ", ".join(drivers_columns)
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
    # exchange_informations
    def get_exchange_informations(self, table_name, start: int, end: int):
        try:

            columns = ", ".join(exchange_informations_columns)
            query = f"""
                SELECT {columns}
                FROM `{table_name}`
                ORDER BY order_id ASC
                LIMIT %s, %s
            """
            limit = end - start

            self.cursor.execute(query, (start, limit))
            return self.cursor.fetchall()

        except Exception as e:
            print(f"MySQL fetch error in master_order: {e}")
            return []
    # exchange_masterorders
    def get_exchange_masterorders(self, table_name, start: int, end: int):
        try:

            columns = ", ".join(exchange_masterorders_columns)
            query = f"""
                SELECT {columns}
                FROM `{table_name}`
                ORDER BY order_id ASC
                LIMIT %s, %s
            """
            limit = end - start

            self.cursor.execute(query, (start, limit))
            return self.cursor.fetchall()

        except Exception as e:
            print(f"MySQL fetch error in master_order: {e}")
            return []
    # exchange_masterorders_w
    def get_exchange_masterorders_w(self, table_name, start: int, end: int):
        try:

            columns = ", ".join(exchange_masterorders_columns)
            query = f"""
                SELECT {columns}
                FROM `{table_name}`
                ORDER BY order_id ASC
                LIMIT %s, %s
            """
            limit = end - start

            self.cursor.execute(query, (start, limit))
            return self.cursor.fetchall()

        except Exception as e:
            print(f"MySQL fetch error in master_order: {e}")
            return []
    # exchange_orderlineitems
    def get_exchange_orderlineitems(self, table_name, start: int, end: int):
        try:
            columns = ", ".join(exchange_orderlineitems_columns)
            query = f"""
                SELECT {columns}
                FROM `{table_name}`
                ORDER BY line_item_id ASC
                LIMIT %s, %s
            """
            limit = end - start

            self.cursor.execute(query, (start, limit))
            return self.cursor.fetchall()

        except Exception as e:
            print(f"MySQL fetch error in exchange_orderlineitems: {e}")
            return []

    def get_externalcalllogs(self, table_name, start: int, end: int):
        try:
            columns = ", ".join(externalcalllogs_columns)
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
            print(f"MySQL fetch error in {table_name}: {e}")
            return []