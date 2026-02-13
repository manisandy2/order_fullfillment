import mysql.connector
from dotenv import load_dotenv
import os
import logging
from mysql.connector import Error
from . import db_colums

load_dotenv()

# Setup logger
logger = logging.getLogger(__name__)

def mysql_connect():

    logger.info("Connecting to MySQL database...")

    try:
        conn = mysql.connector.connect(
            host=os.getenv("HOST"),
            user=os.getenv("USERNAME"),
            password=os.getenv("PASSWORD"),
            database=os.getenv("DATABASE"),
            port=int(os.getenv("PORT", 3306))
        )
        if conn.is_connected():
            logger.info("MySQL connection established")
            return conn
        else:
            raise ConnectionError("MySQL connection could not be established")
    except Error as e:
        logger.error(f"Error connecting to MySQL: {e}")
        return None


class MysqlCatalog:

    def __init__(self) -> None:
        self.conn = mysql_connect()
        if self.conn is None:
            raise ConnectionError("Failed to connect to MySQL database")
        self.cursor = self.conn.cursor(dictionary=True)

    def _fetch_date_range(self, table_name: str, start_date: str, end_date: str, columns: list, date_col: str, sort_col: str) -> list:
        """Generic method to fetch data within a date range."""
        try:
            cols_str = ", ".join(columns)
            query = f"""
                SELECT {cols_str}
                FROM `{table_name}`
                WHERE {date_col} BETWEEN %s AND %s
                ORDER BY {sort_col} ASC
            """

            self.cursor.execute(query, (start_date, end_date))
            return self.cursor.fetchall()

        except Exception as e:
            logger.exception(f"MySQL fetch failed | table={table_name} | range=({start_date},{end_date}) | error={e}")
            raise e

    # --- Refactored Methods ---

    def get_master_order_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.masterorder_columns, "created_at", "order_id")

    def get_master_order_w_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.masterorder_w_columns, "created_at", "order_id")

    def get_pickup_delivery_items_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.pickup_delivery_columns, "row_added_dt", "pickup_delivery_req_item_id")

    def get_pickup_delivery_items_w_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.pickup_delivery_items_w_columns, "row_added_dt", "pickup_delivery_req_item_id")

    def get_orderlineitems_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.orderlineitems_columns, "created_at", "line_item_id")

    def get_status_event_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.status_events_columns, "row_added_dttm", "status_event_id")

    def get_bluedart_zone_masters_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.bluedart_zone_masters_columns, "created_at", "id")

    def get_courier_masters_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.courier_masters_columns, "created_at", "code")

    def get_drivers_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.drivers_columns, "created_at", "id")

    def get_drivers_dob_error_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.drivers_dob_error_columns, "created_at", "id")

    def get_exchange_informations_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.exchange_informations_columns, "created_at", "order_id")

    def get_exchange_masterorders_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.exchange_masterorders_columns, "created_at", "order_id")

    def get_exchange_masterorders_w_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.exchange_masterorders_columns_w, "created_at", "order_id")

    def get_exchange_orderlineitems_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.exchange_orderlineitems_columns, "created_at", "line_item_id")

    def get_externalcalllogs_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.externalcalllogs_columns, "created_at", "id")

    def get_hub_masters_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.hub_masters_columns, "created_at", "id")

    def get_installation_services_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.installation_services_columns, "created_at", "id")

    def get_intransit_manifests_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.intransit_manifests_columns, "created_at", "t_manifest_id")

    def get_intransit_pickup_delivery_items_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.intransit_pickup_delivery_items_columns, "row_added_dt", "intransit_pickupdelivery_id")

    def get_intransit_shipments_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.intransit_shipments_columns, "created_at", "t_shipment_id")

    def get_invoice_masters_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.invoice_masters_columns, "created_at", "id")

    def get_manifests_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.manifests_columns, "created_at", "time_sorted_id")

    def get_pick_lists_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.pick_lists_columns, "created_at", "picklist_id")

    def get_pickup_deliveries_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.pickup_deliveries_columns, "row_added_dttm", "pickup_delivery_req_id")

    def get_reason_messages_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.reason_messages_columns, "created_at", "id")

    def get_return_masterorders_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.return_masterorders_columns, "created_at", "order_id")

    def get_return_masterorders_w_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.return_masterorders_w_columns, "created_at", "order_id")

    def get_return_orderlineitems_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.return_orderlineitems_columns, "created_at", "line_item_id")

    def get_roles_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.roles_columns, "created_at", "time_sorted_id")

    def get_scheduler_retention_log_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.scheduler_retention_log_columns, "created_at", "id")

    def get_schedulers_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.schedulers_columns, "created_at", "job_id")

    def get_schedulers_w_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.schedulers_w_columns, "created_at", "job_id")

    def get_service_history_c_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.service_history_c_columns, "created_at", "id")

    def get_service_history_h_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.service_history_h_columns, "created_at", "id")

    def get_service_master_c_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.service_master_c_columns, "created_at", "service_id")

    def get_service_master_h_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.service_master_h_columns, "created_at", "service_id")

    def get_shipments_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.shipments_columns, "created_at", "shipment_id")

    def get_uploadloggers_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.uploadloggers_columns, "created_at", "time_sorted_id")

    def get_users_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.users_columns, "created_at", "id")

    def get_vehicles_date_between(self, table_name: str, start_date: str, end_date: str) -> list:
        return self._fetch_date_range(table_name, start_date, end_date, db_colums.vehicles_columns, "created_at", "id")


    def close(self) -> None:
        """Close database connection and cursor."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


