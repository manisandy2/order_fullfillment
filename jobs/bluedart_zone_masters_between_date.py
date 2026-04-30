from core.between_date import MysqlCatalog
from pipelines.between_date_pipeline import run_between_date_pipeline


def run():

    return run_between_date_pipeline(
        namespace="order_fulfillment",
        table_name="bluedart_zone_masters",
        dbname="bluedart_zone_masters",
        date_column="created_at",
        fetch_fn=MysqlCatalog.get_bluedart_zone_masters_date_between,

    )


if __name__ == "__main__":
    print(run())