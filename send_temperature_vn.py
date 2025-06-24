from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import requests
import time
import csv
import os
from datetime import datetime


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

# ====== Gửi dữ liệu nhiệt độ thực tế mỗi 5 giây ======
while True:
    try:
        res = requests.get(URL)
        data = res.json()
        #print(data)  # ✅ In toàn bộ dữ liệu để bạn dễ debug

        if "main" in data:
            nhiet_do = data["main"]["temp"]
            point = Point("temperature_data").field("value", nhiet_do)
            write_api.write(bucket=bucket, org=org, record=point)
            print(f"✅ Gửi nhiệt độ thực tế: {nhiet_do}°C")
            
                # Ghi vào file result.csv
            with open("result.csv", mode='a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if not os.path.exists("result.csv") or os.path.getsize("result.csv") == 0:
                    writer.writerow(["time", "city", "message"])  # tiêu đề nếu chưa có
                writer.writerow([now, CITY, f"{nhiet_do}"])

        else:
            print("❌ Lỗi khi gọi API: 'main'")
            print("Không lấy được dữ liệu. Bỏ qua lần này.")
    except Exception as e:
        print(f"❌ Lỗi: {e}")
    
    time.sleep(5)
