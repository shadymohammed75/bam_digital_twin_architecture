import os
import sqlite3

class SQLiteStorage:
    def __init__(self, db_path=r"C:\Users\dell\PycharmProjects\bam-aas-timeseries\data\sensors.db"):
        # Ensure the 'data' folder exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self._init_db()

    def _init_db(self):
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sensor_data (
                sensor_id TEXT,
                timestamp_iso TEXT,
                epoch_ms INTEGER,
                value REAL,
                measurement_type TEXT
            )
        ''')
        self.conn.commit()
        print("[INFO] SQLite table 'sensor_data' initialized.")

    def append_records(self, sensor_id, records):
        cursor = self.conn.cursor()
        try:
            for record in records:
                cursor.execute('''
                    INSERT INTO sensor_data (sensor_id, timestamp_iso, epoch_ms, value, measurement_type)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    sensor_id,
                    record["timestamp_iso"],
                    record["epoch_ms"],
                    record["value"],
                    record["measurement_type"]
                ))
            self.conn.commit()
            print(f"[INFO] Inserted {len(records)} records for sensor '{sensor_id}'.")
        except Exception as e:
            print(f"[ERROR] Failed to insert records for '{sensor_id}': {e}")

    def close(self):
        self.conn.close()
        print("[INFO] SQLite connection closed.")

if __name__ == "__main__":
    storage = SQLiteStorage()
    sample_records = [{
        "timestamp_iso": "2024-01-01T12:00:00Z",
        "epoch_ms": 1704100800000,
        "value": 42.5,
        "measurement_type": "temperature"
    }]
    storage.append_records("sensor_01", sample_records)
    storage.close()
