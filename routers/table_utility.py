


TABLE_LIST = [
    "masterorders",
    "masterorders_w",
    "pickup_delivery_items",
    "pickup_delivery_items_w",
    "orderlineitems",
    "status_events",
    "bluedart_zone_masters",
    "courier_masters",
    "drivers",
    "exchange_informations"
]


def table_identifier(table_name: str) -> str:
    namespace = "order_fulfillment"

    return f"{namespace}.{table_name}"