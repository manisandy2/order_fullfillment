# import trino
# from trino.auth import BasicAuthentication
import time
import trino
# from mysql_client import MysqlCatalog

# ss = MysqlCatalog()

start_time = time.time()
conn = trino.dbapi.connect(
    host="trino-connector.poorvika.com",        # Trino host
    port=443,               # Trino port
    user="admin",            # Username
    catalog="order-fulfillment",       # Catalog name
    schema="order_fulfillment",        # Schema / namespace
    http_scheme="https"       # or "https"
)
print("Total time",time.time() - start_time)
cursor = conn.cursor()
# cursor.execute("SELECT COUNT(*) FROM order_fulfillment.orderlineitems")
cursor.execute("SELECT * FROM order_fulfillment.orderlineitems")
print("Total time",time.time() - start_time)
rows = cursor.fetchall()
print(rows)

# cursor = conn.cursor()
# cursor.execute("SELECT * FROM masterorders where  created_at < %s ORDER BY order_id ASC LIMIT %s, %s
# """)
#
# rows = cursor.fetchall()
# print(rows)
# cursor = conn.cursor()

# sql = """
# SELECT *
# FROM masterorders
# WHERE
#     created_at >= ?
#     AND created_at <= ?
# ORDER BY order_id ASC
#
# """
#
# start_date = "2025-11-01 00:00:00"
# end_date   = "2025-12-01 00:00:00"
# offset = 0
# limit = 1000
#
# cursor.execute(sql, (start_date, end_date))
#
# rows = cursor.fetchall()
# print(rows)
# sql = """
# SELECT *
# FROM orderlineitems
# WHERE
#     created_at >= CAST(? AS TIMESTAMP)
#     AND created_at <= CAST(? AS TIMESTAMP)
#     ORDER BY line_item_id ASC
#
# """
#
# start_date = "2025-12-01 00:00:00"
# end_date   = "2025-12-15 00:00:00"
#
#
#
# cursor.execute(sql, (start_date, end_date))
# rows = cursor.fetchall()
# print(len(rows))


# conn = trino.dbapi.connect(
#     host="trino.yourdomain.com",
#     port=443,
#     user="admin",
#     http_scheme="https",
#     auth=BasicAuthentication("admin", "PASSWORD"),
#     catalog="iceberg",
#     schema="order_fulfillment"
# )
#
# cur = conn.cursor()
# cur.execute("SHOW TABLES")
# print(cur.fetchall())