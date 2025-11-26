import mysql.connector
from dotenv import load_dotenv
import os
from mysql.connector import Error
# import mysql

load_dotenv()
# print(os.getenv("HOST"))

def mysql_connect():
    print("Connecting to MySQL database...")
    # print("Host",os.getenv("HOST"))
    # print("USERNAME",os.getenv("USERNAME"))
    # print(os.getenv("PASSWORD"))
    # print(os.getenv("DATABASE"))
    try:
        conn = mysql.connector.connect(
            host=os.getenv("HOST"),
            user=os.getenv("USERNAME"),
            password=os.getenv("PASSWORD"),
            database=os.getenv("DATABASE"),
            port=os.getenv("PORT")
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
    def __init__(self):
        self.conn = mysql_connect()
        # self.cursor = self.conn.cursor()
        self.cursor = self.conn.cursor(dictionary=True)


    def get_all_value(self,table_name):

        self.cursor.execute(f"SELECT * FROM {table_name}")
        return self.cursor.fetchall()

    def get_count(self,table_name:str):
        query = f"SELECT COUNT(*) AS total FROM `{table_name}`"
        self.cursor.execute(query)
        row = self.cursor.fetchone()
        count = int(row[0])
        return count


    def get_describe(self,table_name:str):

        self.cursor.execute(f"DESCRIBE {table_name}")
        # print("Describe:",self.cursor.fetchall())
        # return self.cursor.fetchall()
        result = self.cursor.fetchall()
        # print("Describe:", result)
        return result

    def get_range(self,table_name:str, start: int, end: int):

        self.cursor.execute(f"SELECT * FROM {table_name} LIMIT {start}, {end - start}")
        return self.cursor.fetchall()

    def get_schema(self, table_name: str):
        # Validate table name before using it in SQL (for safety)

        query = f"SHOW COLUMNS FROM `{table_name}`"
        self.cursor.execute(query)

        rows = self.cursor.fetchall()
        # print(rows)
        # If using dictionary cursor
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

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


# ss = MysqlCatalog()
# print(ss.get_count(table_name="masterorders"))
# print(ss.get_schema(table_name="masterorders"))