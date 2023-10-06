import snowflake.connector


class SnowflakeAccessManager:

    def __init__(self, conn_params):
        self.conn_params = conn_params

    def __enter__(self):
        conn = snowflake.connector.connect(
            user=self.conn_params.get("user_name"),
            password=self.conn_params.get("password"),
            account=self.conn_params.get("account"),
            warehouse=self.conn_params.get("warehouse", "COMPUTE_WH"),
            role=self.conn_params.get("role", "ACCOUNTADMIN"),
            database=self.conn_params.get("database", "DEV_DB"),
            # client_session_keep_alive=True,
        )
        # Create a cursor object.
        self.cur = conn.cursor()
        return self.cur

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cur.close()