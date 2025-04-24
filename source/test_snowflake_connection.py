from utils.snowflake_connector import SnowflakeConnector

def test_connection():
    try:
        sf = SnowflakeConnector()
        conn = sf.get_connection()
        cur = conn.cursor()

        # Run a simple test query
        cur.execute("SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_REGION();")
        result = cur.fetchone()

        print("✅ Connection successful!")
        print(f"User: {result[0]}, Account: {result[1]}, Region: {result[2]}")

        cur.close()
        conn.close()

    except Exception as e:
        print("❌ Connection failed.")
        print(f"Error: {e}")

if __name__ == "__main__":
    test_connection()