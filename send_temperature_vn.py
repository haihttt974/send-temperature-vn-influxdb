from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import requests
import time
import csv
import os
from datetime import datetime, timezone
import pytz

# ====== Thông tin kết nối InfluxDB ======
token = "mytoken123"
org = "my-org"
bucket = "temperature"
url_influx = "http://localhost:8086"

client = InfluxDBClient(url=url_influx, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

# ====== Thông tin kết nối API thời tiết ======
API_KEY = "04ac04c6c5dfbc0ed71a2f734493c394"
CITY = "Ho Chi Minh"
URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"

# ====== Chuẩn bị file CSV ======
csv_file = "result.csv"
file_exists = os.path.exists(csv_file)

if not file_exists:
    with open(csv_file, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["time", "city", "message"])  # Tiêu đề

# ====== Gửi dữ liệu mỗi 5 giây ======
while True:
    try:
        res = requests.get(URL)
        data = res.json()

        if "main" in data:
            nhiet_do = data["main"]["temp"]

            # Thời gian hiện tại theo UTC
            local_time = datetime.now()
            utc_time = pytz.timezone("Asia/Ho_Chi_Minh").localize(local_time).astimezone(pytz.utc)

            # Gửi vào InfluxDB với tag city
            point = Point("temperature_data")\
                .tag("city", CITY)\
                .field("value", nhiet_do)\
                .time(utc_time)

            write_api.write(bucket=bucket, org=org, record=point)
            print(f"✅ Gửi: {local_time.strftime('%Y-%m-%d %H:%M:%S')} - {nhiet_do}°C")

            # Ghi vào CSV
            with open("result.csv", mode='a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([local_time.strftime("%Y-%m-%d %H:%M:%S"), CITY, f"{nhiet_do}"])

        else:
            print("❌ Không lấy được dữ liệu từ API.")

    except Exception as e:
        print(f"❌ Lỗi: {e}")

    time.sleep(5)
