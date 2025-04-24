import snowflake.connector

class SnowflakeSchemaManager:
    """Manages schema setup in Snowflake (without creating database & schema)."""

    def __init__(self):
        """Initialize SnowflakeSchemaManager with a connection."""
        self.conn = self.get_connection()

    def get_connection(self):
        """Establish a connection to Snowflake."""
        from utils.snowflake_connector import SnowflakeConnector
        sf_connector = SnowflakeConnector()
        return sf_connector.get_connection()

    def use_existing_schema(self):
        """Set the active database and schema (since they already exist)."""
        cur = self.conn.cursor()

        # ✅ Use the existing database & schema
        cur.execute("USE DATABASE HOUSING_MARKET_ANALYSIS;")
        cur.execute("USE SCHEMA PUBLIC;")
        cur.execute("USE WAREHOUSE HOUSING_COMPUTE_WH;")

        print("✅ Using existing Snowflake database, schema, and warehouse.")
        cur.close()

    def close_connection(self):
        """Close the Snowflake connection."""
        self.conn.close()