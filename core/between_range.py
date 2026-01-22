from .mysql_client import mysql_connect
from .db_colums import *
import logging

# Configure logger
logger = logging.getLogger(__name__)

class MydatabaseRange:
    def __init__(self):
        self.db = mysql_connect()
        if self.db is None:
            raise RuntimeError("Database connection failed")
        self.cursor = self.db.cursor(dictionary=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Close the cursor and database connection."""
        if self.cursor:
            try:
                self.cursor.close()
            except Exception as e:
                logger.error(f"Error closing cursor: {e}")
        if self.db:
            try:
                self.db.close()
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")

    def _fetch_rows(self, table_name: str, columns: list, start: int, end: int, order_by: str) -> list:
        """
        Generic helper method to fetch rows from the database with pagination.
        """
        try:
            columns_str = ", ".join(columns)
            # Note: table_name cannot be parameterized in MySQL, so we must rely on proper input validation upstream.
            query = f"""
                SELECT {columns_str}
                FROM `{table_name}`
                ORDER BY {order_by} ASC
                LIMIT %s, %s
            """
            limit = end - start
            self.cursor.execute(query, (start, limit))
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"MySQL fetch error in {table_name}: {e}")
            return []

    # bluedart_zone_masters
    def get_bluedart_zone_masters(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, bluedart_zone_masters_columns, start, end, "id")

    # courier_masters
    def get_courier_masters(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, courier_masters_columns, start, end, "code")

    # drivers
    def get_drivers(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, drivers_columns, start, end, "id")

    # exchange_informations
    def get_exchange_informations(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, exchange_informations_columns, start, end, "order_id")

    # exchange_masterorders
    def get_exchange_masterorders(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, exchange_masterorders_columns, start, end, "order_id")

    # exchange_masterorders_w
    def get_exchange_masterorders_w(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, exchange_masterorders_columns, start, end, "order_id")

    # exchange_orderlineitems
    def get_exchange_orderlineitems(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, exchange_orderlineitems_columns, start, end, "line_item_id")

    def get_externalcalllogs(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, externalcalllogs_columns, start, end, "id")

    def get_hub_masters(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, hub_masters_columns, start, end, "id")

    def get_installation_services(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, installation_services_columns, start, end, "id")

    def get_intransit_manifests(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, intransit_manifests_columns, start, end, "t_manifest_id")

    def get_intransit_pickup_delivery_items(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, intransit_pickup_delivery_items_columns, start, end, "intransit_pickupdelivery_id")

    def get_intransit_shipments(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, intransit_shipments_columns, start, end, "t_shipment_id")

    def get_invoice_masters(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, invoice_masters_columns, start, end, "id")

    def get_manifests(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, manifests_columns, start, end, "time_sorted_id")

    def get_master_order(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, masterorder_columns, start, end, "order_id")

    def get_master_order_w(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, masterorder_columns, start, end, "order_id")

    def get_pick_lists(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, pick_lists_columns, start, end, "picklist_id")

    def get_pickup_deliveries(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, pickup_deliveries_columns, start, end, "pickup_delivery_req_id")

    def get_reason_messages(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, reason_messages_columns, start, end, "id")

    def get_return_masterorders(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, return_masterorders_columns, start, end, "order_id")

    def get_return_masterorders_w(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, return_masterorders_w_columns, start, end, "order_id")

    def get_return_orderlineitems(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, return_orderlineitems_columns, start, end, "line_item_id")

    def get_roles(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, roles_columns, start, end, "time_sorted_id")

    def get_scheduler_retention_log(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, scheduler_retention_log_columns, start, end, "id")

    def get_schedulers(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, schedulers_columns, start, end, "job_id")

    def get_schedulers_w(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, schedulers_w_columns, start, end, "job_id")

    def get_service_history_c(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, service_history_c_columns, start, end, "id")

    def get_service_history_h(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, service_history_h_columns, start, end, "id")

    def get_service_master_c(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, service_master_c_columns, start, end, "service_id")

    def get_service_master_h(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, service_master_h_columns, start, end, "service_id")

    def get_shipments(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, shipments_columns, start, end, "shipment_id")

    def get_uploadloggers(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, uploadloggers_columns, start, end, "time_sorted_id")

    def get_users(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, users_columns, start, end, "id")

    def get_vehicles(self, table_name: str, start: int, end: int) -> list:
        return self._fetch_rows(table_name, vehicles_columns, start, end, "id")
