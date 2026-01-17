
import sqlite3
import pandas as pd

DB_PATH = "data/tempest.db"

def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query("SELECT * FROM collector_heartbeat", conn)
        print("Collector Heartbeat Status:")
        print(df.to_string())
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
