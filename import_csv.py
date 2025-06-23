import pandas as pd
from influxdb_client import InfluxDBClient, Point
# *** THÊM DÒNG NÀY ***
from influxdb_client.client.write_api import SYNCHRONOUS
import os

# --- Cấu hình ---
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "9m598lxZxRoqIEd1ja0kWSKXXVZG1y9lUbjYfFuuktWLcGvWsxlWE67Evmqf7JJLfdo6N_pAQe_UsQ5_sx8IVQ=="
INFLUXDB_ORG = "my-org"
INFLUXDB_BUCKET = "history_weather"
CSV_FILE_PATH = "weatherHistory.csv"

# --- TÙY CHỈNH THEO FILE CSV CỦA BẠN ---
COLUMN_TIMESTAMP = "Formatted Date"
COLUMN_SUMMARY = "Summary"
COLUMN_PRECIP_TYPE = "Precip Type"
COLUMN_TEMP = "Temperature (C)"
COLUMN_APPARENT_TEMP = "Apparent Temperature (C)"
COLUMN_HUMIDITY = "Humidity"
COLUMN_WIND_SPEED = "Wind Speed (km/h)"
COLUMN_WIND_BEARING = "Wind Bearing (degrees)"
COLUMN_VISIBILITY = "Visibility (km)"
COLUMN_LOUD_COVER = "Loud Cover"
COLUMN_PRESSURE = "Pressure (millibars)"
COLUMN_DAILY_SUMMARY = "Daily Summary"

MEASUREMENT_NAME = "weather_archive_full"

def import_data_from_csv():
    """Đọc toàn bộ dữ liệu từ file CSV và ghi vào InfluxDB."""

    print(f"Reading data from {CSV_FILE_PATH}...")
    try:
        df = pd.read_csv(CSV_FILE_PATH, parse_dates=[COLUMN_TIMESTAMP])
    except FileNotFoundError:
        print(f"Error: File not found at {CSV_FILE_PATH}")
        return

    with InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG) as client:
        # *** SỬA DÒNG NÀY: Chuyển sang chế độ ghi đồng bộ ***
        write_api = client.write_api(write_options=SYNCHRONOUS)

        print(f"Preparing to write {len(df)} points to bucket '{INFLUXDB_BUCKET}'...")

        points = []
        for index, row in df.iterrows():
            try:
                timestamp = pd.to_datetime(row[COLUMN_TIMESTAMP], utc=True)
                point = Point(MEASUREMENT_NAME)
                
                if pd.notna(row[COLUMN_PRECIP_TYPE]):
                    point = point.tag(COLUMN_PRECIP_TYPE, row[COLUMN_PRECIP_TYPE])

                point = point.field(COLUMN_SUMMARY, row[COLUMN_SUMMARY]) \
                             .field(COLUMN_TEMP, float(row[COLUMN_TEMP])) \
                             .field(COLUMN_APPARENT_TEMP, float(row[COLUMN_APPARENT_TEMP])) \
                             .field(COLUMN_HUMIDITY, float(row[COLUMN_HUMIDITY])) \
                             .field(COLUMN_WIND_SPEED, float(row[COLUMN_WIND_SPEED])) \
                             .field(COLUMN_WIND_BEARING, float(row[COLUMN_WIND_BEARING])) \
                             .field(COLUMN_VISIBILITY, float(row[COLUMN_VISIBILITY])) \
                             .field(COLUMN_LOUD_COVER, float(row[COLUMN_LOUD_COVER])) \
                             .field(COLUMN_PRESSURE, float(row[COLUMN_PRESSURE])) \
                             .field(COLUMN_DAILY_SUMMARY, row[COLUMN_DAILY_SUMMARY]) \
                             .time(timestamp)

                # Với chế độ SYNCHRONOUS, bạn có thể ghi từng điểm hoặc theo lô
                # Ghi theo lô vẫn hiệu quả hơn
                points.append(point)

                if len(points) % 5000 == 0:
                    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=points)
                    points = []
                    print(f"Wrote batch of 5000 points... (total written: {index + 1})")

            except Exception as e:
                print(f"Skipping row {index + 2} due to error: {e}")

        if points:
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=points)
            print(f"Wrote final batch of {len(points)} points.")

        print("--- Data import completed! ---")

if __name__ == "__main__":
    import_data_from_csv()