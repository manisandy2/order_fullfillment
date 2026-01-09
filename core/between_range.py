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

    def get_hub_masters(self, table_name, start: int, end: int):
        try:
            columns = ", ".join(hub_masters_columns)
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

    def get_installation_services(self, table_name, start: int, end: int):
        try:
            columns = ", ".join(installation_services_columns)
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

    def get_intransit_manifests(self, table_name, start: int, end: int):
        try:
            columns = ", ".join(intransit_manifests_columns)
            query = f"""
                SELECT {columns}
                FROM `{table_name}`
                ORDER BY t_manifest_id ASC
                LIMIT %s, %s
            """
            limit = end - start

            self.cursor.execute(query, (start, limit))
            return self.cursor.fetchall()

        except Exception as e:
            print(f"MySQL fetch error in {table_name}: {e}")
            return []

    def get_intransit_pickup_delivery_items(self, table_name, start: int, end: int):
        try:
            columns = ", ".join(intransit_pickup_delivery_items_columns)
            query = f"""
                SELECT {columns}
                FROM `{table_name}`
                ORDER BY intransit_pickupdelivery_id ASC
                LIMIT %s, %s
            """
            limit = end - start

            self.cursor.execute(query, (start, limit))
            return self.cursor.fetchall()

        except Exception as e:
            print(f"MySQL fetch error in {table_name}: {e}")
            return []
    ################## test joint column
    def get_intransit_shipments(self, table_name, start: int, end: int):
        try:
            columns = ", ".join(intransit_shipments_columns)
            query = f"""
                SELECT {columns}
                FROM `{table_name}`
                ORDER BY t_shipment_id ASC
                LIMIT %s, %s
            """
            limit = end - start

            self.cursor.execute(query, (start, limit))
            return self.cursor.fetchall()

        except Exception as e:
            print(f"MySQL fetch error in {table_name}: {e}")
            return []

    def get_invoice_masters(self, table_name, start: int, end: int):
        try:
            columns = ", ".join(invoice_masters_columns)
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

    def get_manifests(self, table_name, start: int, end: int):
        try:
            columns = ", ".join(manifests_columns)
            query = f"""
                SELECT {columns}
                FROM `{table_name}`
                ORDER BY time_sorted_id ASC
                LIMIT %s, %s
            """
            limit = end - start

            self.cursor.execute(query, (start, limit))
            return self.cursor.fetchall()

        except Exception as e:
            print(f"MySQL fetch error in {table_name}: {e}")
            return []

    def get_pick_lists(self, table_name, start: int, end: int):
        try:
            columns = ", ".join(pick_lists_columns)
            query = f"""
                SELECT {columns}
                FROM `{table_name}`
                ORDER BY picklist_id ASC
                LIMIT %s, %s
            """
            limit = end - start

            self.cursor.execute(query, (start, limit))
            return self.cursor.fetchall()

        except Exception as e:
            print(f"MySQL fetch error in {table_name}: {e}")
            return []

    def get_pickup_deliveries(self, table_name, start: int, end: int):
        try:
            columns = ", ".join(pickup_deliveries_columns)
            query = f"""
                SELECT {columns}
                FROM `{table_name}`
                ORDER BY pickup_delivery_req_id ASC
                LIMIT %s, %s
            """
            limit = end - start

            self.cursor.execute(query, (start, limit))
            return self.cursor.fetchall()

        except Exception as e:
            print(f"MySQL fetch error in {table_name}: {e}")
            return []

    def get_reason_messages(self, table_name, start: int, end: int):
        try:
            columns = ", ".join(reason_messages_columns)
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

    def get_return_masterorders(self, table_name, start: int, end: int):
        try:
            columns = ", ".join(return_masterorders_columns)
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
            print(f"MySQL fetch error in {table_name}: {e}")
            return []

    def get_return_masterorders_w(self, table_name, start: int, end: int):
        try:
            columns = ", ".join(return_masterorders_w_columns)
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
            print(f"MySQL fetch error in {table_name}: {e}")
            return []

    def get_return_orderlineitems(self, table_name, start: int, end: int):
        try:
            columns = ", ".join(return_orderlineitems_columns)
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
            print(f"MySQL fetch error in {table_name}: {e}")
            return []

    def get_roles(self, table_name, start: int, end: int):
        try:
            columns = ", ".join(roles_columns)
            query = f"""
                SELECT {columns}
                FROM `{table_name}`
                ORDER BY time_sorted_id ASC
                LIMIT %s, %s
            """
            limit = end - start

            self.cursor.execute(query, (start, limit))
            return self.cursor.fetchall()

        except Exception as e:
            print(f"MySQL fetch error in {table_name}: {e}")
            return []

    def get_scheduler_retention_log(self, table_name, start: int, end: int):
        try:
            columns = ", ".join(scheduler_retention_log_columns)
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

    def get_schedulers(self, table_name, start: int, end: int):
        try:
            columns = ", ".join(schedulers_columns)
            query = f"""
                SELECT {columns}
                FROM `{table_name}`
                ORDER BY job_id ASC
                LIMIT %s, %s
            """
            limit = end - start

            self.cursor.execute(query, (start, limit))
            return self.cursor.fetchall()

        except Exception as e:
            print(f"MySQL fetch error in {table_name}: {e}")
            return []

    def get_schedulers_w(self, table_name, start: int, end: int):
        try:
            columns = ", ".join(schedulers_w_columns)
            query = f"""
                SELECT {columns}
                FROM `{table_name}`
                ORDER BY job_id ASC
                LIMIT %s, %s
            """
            limit = end - start

            self.cursor.execute(query, (start, limit))
            return self.cursor.fetchall()

        except Exception as e:
            print(f"MySQL fetch error in {table_name}: {e}")
            return []

    def get_service_history_c(self, table_name, start: int, end: int):
        try:
            columns = ", ".join(service_history_c_columns)
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

    def get_service_history_h(self, table_name, start: int,
                                end: int):
        try:
            columns = ", ".join(
                service_history_h_columns)
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

    def get_service_master_c(self, table_name, start: int,
                                end: int):
        try:
            columns = ", ".join(
                service_master_c_columns)
            query = f"""
                SELECT {columns}
                FROM `{table_name}`
                ORDER BY service_id ASC
                LIMIT %s, %s
            """
            limit = end - start

            self.cursor.execute(query, (start, limit))
            return self.cursor.fetchall()

        except Exception as e:
            print(f"MySQL fetch error in {table_name}: {e}")
            return []

    def get_service_master_h(self, table_name,
                                start: int, end: int):
        try:
            columns = ", ".join(
                service_master_h_columns)
            query = f"""
                SELECT {columns}
                FROM `{table_name}`
                ORDER BY service_id ASC
                LIMIT %s, %s
            """
            limit = end - start

            self.cursor.execute(query, (start, limit))
            return self.cursor.fetchall()

        except Exception as e:
            print(f"MySQL fetch error in {table_name}: {e}")
            return []

    def get_shipments(self, table_name,
                                start: int, end: int):
        try:
            columns = ", ".join(
                shipments_columns)
            query = f"""
                SELECT {columns}
                FROM `{table_name}`
                ORDER BY shipment_id ASC
                LIMIT %s, %s
            """
            limit = end - start

            self.cursor.execute(query, (start, limit))
            return self.cursor.fetchall()

        except Exception as e:
            print(
                f"MySQL fetch error in {table_name}: {e}")
            return []

    def get_uploadloggers(self, table_name,
                      start: int, end: int):
        try:
            columns = ", ".join(
                uploadloggers_columns)
            query = f"""
                SELECT {columns}
                FROM `{table_name}`
                ORDER BY time_sorted_id ASC
                LIMIT %s, %s
            """
            limit = end - start

            self.cursor.execute(query, (start, limit))
            return self.cursor.fetchall()

        except Exception as e:
            print(
                f"MySQL fetch error in {table_name}: {e}")
            return []

    def get_users(self, table_name,
                      start: int, end: int):
        try:
            columns = ", ".join(
                users_columns)
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
            print(
                f"MySQL fetch error in {table_name}: {e}")
            return []

    def get_vehicles(self, table_name,
                      start: int, end: int):
        try:
            columns = ", ".join(
                vehicles_columns)
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
            print(
                f"MySQL fetch error in {table_name}: {e}")
            return []



