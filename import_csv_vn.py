import pandas as pd
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# --- Cấu hình ---
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "9m598lxZxRoqIEd1ja0kWSKXXVZG1y9lUbjYfFuuktWLcGvWsxlWE67Evmqf7JJLfdo6N_pAQe_UsQ5_sx8IVQ=="  # Thay bằng token thật của bạn
INFLUXDB_ORG = "my-org"
INFLUXDB_BUCKET = "weather_bucket"
CSV_FILE_PATH = "weather.csv"

# --- Tên đo lường ---
MEASUREMENT_NAME = "weather_data_vn"

def import_data_from_csv():
    """Nhập dữ liệu thời tiết từ CSV vào InfluxDB."""
    try:
        df = pd.read_csv(CSV_FILE_PATH, parse_dates=["date"])
    except FileNotFoundError:
        print(f"Không tìm thấy file: {CSV_FILE_PATH}")
        return

    with InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        print(f"Đang ghi {len(df)} dòng vào bucket '{INFLUXDB_BUCKET}'...")

        points = []
        for index, row in df.iterrows():
            try:
                point = Point(MEASUREMENT_NAME)
                point = point.tag("province", row["province"]) \
                             .tag("wind_direction", row["wind_d"]) \
                             .field("temp_max", int(row["max"])) \
                             .field("temp_min", int(row["min"])) \
                             .field("wind_speed", int(row["wind"])) \
                             .field("rain", float(row["rain"])) \
                             .field("humidity", int(row["humidi"])) \
                             .field("cloud", int(row["cloud"])) \
                             .field("pressure", int(row["pressure"])) \
                             .time(pd.to_datetime(row["date"], utc=True))

                points.append(point)

                if len(points) % 5000 == 0:
                    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=points)
                    print(f"Đã ghi {len(points)} điểm...")
                    points = []

            except Exception as e:
                print(f"Lỗi dòng {index + 2}: {e}")

        if points:
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=points)
            print(f"Đã ghi lô cuối gồm {len(points)} điểm.")

        print("--- Nhập dữ liệu hoàn tất! ---")

if __name__ == "__main__":
    import_data_from_csv()
