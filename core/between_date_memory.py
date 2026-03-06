import mysql.connector
from dotenv import load_dotenv
import os
import logging
from mysql.connector import Error
from . import db_colums

load_dotenv()
logger = logging.getLogger(__name__)

# ===============================
# MYSQL CONNECTION
# ===============================

def mysql_connect():
    try:
        conn = mysql.connector.connect(
            host=os.getenv("HOST"),
            user=os.getenv("USERNAME"),
            password=os.getenv("PASSWORD"),
            database=os.getenv("DATABASE"),
            port=int(os.getenv("PORT", 3306)),
            autocommit = False,
            connection_timeout=30,
        )
        return conn

    except Error as e:
        logger.error(f"Error connecting to MySQL: {e}")
        raise

# ===============================
# MYSQL CATALOG
# ===============================

class MysqlCatalog:

    BATCH_SIZE = 5000

    TABLE_CONFIG = {
        "bluedart_zone_masters":{
            "columns": db_colums.bluedart_zone_masters_columns,
            "date_col": "created_at",
            "sort_col": "id",
        },
        "courier_masters": {
            "columns": db_colums.courier_masters_columns,
            "date_col": "created_at",
            "sort_col": "code",
        },
        "drivers": {
            "columns": db_colums.drivers_columns,
            "date_col": "created_at",
            "sort_col": "id",
        },
        "drivers_dob_error": {
            "columns": db_colums.drivers_dob_error_columns,
            "date_col": "created_at",
            "sort_col": "id",
        },
        "exchange_informations": {
            "columns": db_colums.exchange_informations_columns,
            "date_col": "created_at",
            "sort_col": "order_id",
        },
        "exchange_masterorders": {
            "columns": db_colums.exchange_masterorders_columns,
            "date_col": "created_at",
            "sort_col": "order_id",
        },
        "exchange_masterorders_w": {
            "columns": db_colums.exchange_masterorders_columns_w,
            "date_col": "created_at",
            "sort_col": "order_id",
        },
        "exchange_orderlineitems": {
            "columns": db_colums.exchange_orderlineitems_columns,
            "date_col": "created_at",
            "sort_col": "line_item_id",
        },
        "externalcalllogs": {
            "columns": db_colums.externalcalllogs_columns,
            "date_col": "created_at",
            "sort_col": "id",
        },
        "hub_masters": {
            "columns": db_colums.hub_masters_columns,
            "date_col": "created_at",
            "sort_col": "id",
        },
        "installation_services": {
            "columns": db_colums.installation_services_columns,
            "date_col": "created_at",
            "sort_col": "id",
        },
        "intransit_manifests": {
            "columns": db_colums.intransit_manifests_columns,
            "date_col": "created_at",
            "sort_col": "t_manifest_id",
        },
        "intransit_pickup_delivery_items": {
            "columns": db_colums.intransit_pickup_delivery_items_columns,
            "date_col": "row_added_dt",
            "sort_col": "intransit_pickupdelivery_id",
        },
        "intransit_shipments": {
            "columns": db_colums.intransit_shipments_columns,
            "date_col": "created_at",
            "sort_col": "t_shipment_id",
        },
        "invoice_masters": {
            "columns": db_colums.invoice_masters_columns,
            "date_col": "created_at",
            "sort_col": "id",
        },
        "manifests": {
            "columns": db_colums.manifests_columns,
            "date_col": "created_at",
            "sort_col": "time_sorted_id",
        },
        "masterorders": {
            "columns": db_colums.masterorder_columns,
            "date_col": "created_at",
            "sort_col": "order_id",
        },
        "masterorders_w": {
            "columns": db_colums.masterorder_w_columns,
            "date_col": "created_at",
            "sort_col": "order_id",
        },
        "orderlineitems": {
            "columns": db_colums.orderlineitems_columns,
            "date_col": "created_at",
            "sort_col": "line_item_id",
        },
        "pick_lists": {
            "columns": db_colums.pick_lists_columns,
            "date_col": "created_at",
            "sort_col": "picklist_id",
        },
        "pickup_deliveries": {
            "columns": db_colums.pickup_deliveries_columns,
            "date_col": "row_added_dttm",
            "sort_col": "pickup_delivery_req_id",
        },
        "pickup_delivery_items": {
            "columns": db_colums.pickup_delivery_items_columns,
            "date_col": "row_added_dt",
            "sort_col": "pickup_delivery_req_item_id",
        },
        "pickup_delivery_items_w": {
            "columns": db_colums.pickup_delivery_items_w_columns,
            "date_col": "row_added_dt",
            "sort_col": "pickup_delivery_req_item_id",
        },
        "reason_messages":{
            "columns": db_colums.reason_messages_columns,
            "date_col": "created_at",
            "sort_col": "id",
        },
        "return_masterorders": {
            "columns": db_colums.return_masterorders_columns,
            "date_col": "created_at",
            "sort_col": "order_id",
        },
        "return_masterorders_w": {
            "columns": db_colums.return_masterorders_w_columns,
            "date_col": "created_at",
            "sort_col": "order_id",
        },
        "return_orderlineitems": {
            "columns": db_colums.return_orderlineitems_columns,
            "date_col": "created_at",
            "sort_col": "line_item_id",
        },
        "roles": {
            "columns": db_colums.roles_columns,
            "date_col": "created_at",
            "sort_col": "time_sorted_id",
        },
        "scheduler_retention_log": {
            "columns": db_colums.scheduler_retention_log_columns,
            "date_col": "created_at",
            "sort_col": "id",
        },

        "schedulers": {
            "columns": db_colums.schedulers_columns,
            "date_col": "created_at",
            "sort_col": "job_id",
        },
        "schedulers_w": {
            "columns": db_colums.schedulers_w_columns,
            "date_col": "created_at",
            "sort_col": "job_id",
        },
        "service_history_c": {
            "columns": db_colums.service_history_c_columns,
            "date_col": "created_at",
            "sort_col": "id",
        },
        "service_history_h": {
            "columns": db_colums.service_history_h_columns,
            "date_col": "created_at",
            "sort_col": "id",
        },
        "service_master_c": {
            "columns": db_colums.service_master_c_columns,
            "date_col": "created_at",
            "sort_col": "service_id",
        },
        "service_master_h": {
            "columns": db_colums.service_master_h_columns,
            "date_col": "created_at",
            "sort_col": "service_id",
        },
        "shipments": {
            "columns": db_colums.shipments_columns,
            "date_col": "created_at",
            "sort_col": "shipment_id",
        },
        "status_events": {
            "columns": db_colums.status_events_columns,
            "date_col": "row_added_dttm",
            "sort_col": "status_event_id",
        },
        "uploadloggers": {
            "columns": db_colums.uploadloggers_columns,
            "date_col": "created_at",
            "sort_col": "time_sorted_id",
        },
        "users": {
            "columns": db_colums.users_columns,
            "date_col": "created_at",
            "sort_col": "id",
        },
        "vehicles": {
            "columns": db_colums.vehicles_columns,
            "date_col": "created_at",
            "sort_col": "id",
        },
    }

    def __init__(self):
        self.conn = mysql_connect()
        self.cursor = self.conn.cursor(dictionary=True)
        self.cursor.arraysize = self.BATCH_SIZE

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        try:
            if self.cursor:
                self.cursor.close()
        finally:
            if self.conn and self.conn.is_connected():
                self.conn.close()

    # ===============================
    # GENERIC DATE RANGE FETCH
    # ===============================
    def get_table_date_between(
            self,
            table_name: str,
            start_date: str,
            end_date: str,
    ):
        """
        Generic streaming fetch.
        Returns generator (memory safe).
        """

        if table_name not in self.TABLE_CONFIG:
            raise ValueError(f"No TABLE_CONFIG found for table: {table_name}")

        config = self.TABLE_CONFIG[table_name]

        return self._fetch_date_range(
            table_name=table_name,
            start_date=start_date,
            end_date=end_date,
            columns=config["columns"],
            date_col=config["date_col"],
            sort_col=config["sort_col"],
        )

    # ===============================
    # STREAMING FETCH CORE
    # ===============================
    def _fetch_date_range(
            self,
            table_name: str,
            start_date: str,
            end_date: str,
            columns: list,
            date_col: str,
            sort_col: str
    ):

        cols_str = ", ".join(columns)

        query = f"""
            SELECT {cols_str}
            FROM `{table_name}`
            WHERE {date_col} BETWEEN %s AND %s
            ORDER BY {sort_col} ASC
        """

        if not self.conn.is_connected():
            self.conn.reconnect()

        self.cursor.execute(query, (start_date, end_date))

        while True:
            rows = self.cursor.fetchmany(5000)  # batch size
            if not rows:
                break
            yield rows  # 🔥 THIS makes it streaming