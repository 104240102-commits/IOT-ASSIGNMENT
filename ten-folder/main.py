import csv
import time
import json
import math
import random
from datetime import datetime, timezone
from azure.iot.device import IoTHubDeviceClient, Message

# ---------- CẤU HÌNH ----------
CONNECTION_STRING = "HostName=your-hub.azure-devices.net;DeviceId=your-device;SharedAccessKey=xxxxxx"
CSV_FILE = "output.csv"  # File tọa độ bạn đã tải về
SEND_INTERVAL = 5        # Giây giữa mỗi lần gửi
# -----------------------------

def calculate_distance(lat1, lon1, lat2, lon2):
    """Tính khoảng cách giữa 2 tọa độ (km) bằng công thức Haversine."""
    R = 6371  # Bán kính Trái Đất (km)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

def generate_elevation(lat, lon):
    """Tạo độ cao giả lập dựa trên tọa độ (m)."""
    # Mô phỏng địa hình: dao động 0-500m dựa trên lat/lon
    return 100 + 50 * math.sin(lat) + 30 * math.cos(lon)

def load_coordinates(csv_path):
    """Đọc danh sách tọa độ từ file CSV."""
    coords = []
    with open(csv_path, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) >= 2:
                lat = float(row[0].strip())
                lon = float(row[1].strip())
                coords.append((lat, lon))
    return coords

def main():
    # 1. Đọc tọa độ
    coords = load_coordinates(CSV_FILE)
    if len(coords) < 2:
        print("Cần ít nhất 2 tọa độ để tính tốc độ!")
        return
    print(f"Đã tải {len(coords)} điểm tọa độ.")

    # 2. Kết nối IoT Hub
    try:
        client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)
        client.connect()
        print("Đã kết nối IoT Hub.")
    except Exception as e:
        print(f"Lỗi kết nối IoT Hub: {e}")
        return

    # 3. Mô phỏng di chuyển qua từng điểm
    prev_lat, prev_lon = coords[0]
    prev_time = time.time()

    for i, (lat, lon) in enumerate(coords):
        # Tính thời gian UTC hiện tại
        now_utc = datetime.now(timezone.utc)
        timestamp = now_utc.isoformat().replace('+00:00', 'Z')
        date_str = now_utc.strftime("%d%m%y")

        # Tính tốc độ (km/h) dựa trên khoảng cách và thời gian thực giữa các lần gửi
        distance_km = calculate_distance(prev_lat, prev_lon, lat, lon)
        time_elapsed = time.time() - prev_time
        speed_kmh = (distance_km / time_elapsed) * 3600 if time_elapsed > 0 else 0.0

        # Tạo độ cao giả lập
        altitude = generate_elevation(lat, lon)

        # Tạo dữ liệu telemetry với các trường mở rộng
        telemetry = {
            "latitude": lat,
            "longitude": lon,
            "altitude_m": round(altitude, 1),
            "speed_kmh": round(speed_kmh, 2),
            "utc_time": timestamp,
            "date": date_str,
            "satellites": random.randint(6, 12)  # Số vệ tinh giả lập
        }

        # In ra console để kiểm tra
        print(f"\n[{i+1}/{len(coords)}] Gửi dữ liệu:")
        print(json.dumps(telemetry, indent=2))

        # Gửi lên IoT Hub
        msg = Message(json.dumps(telemetry))
        msg.content_encoding = "utf-8"
        msg.content_type = "application/json"
        client.send_message(msg)
        print("Đã gửi thành công.")

        # Cập nhật cho lần sau
        prev_lat, prev_lon = lat, lon
        prev_time = time.time()

        # Chờ đến lần gửi tiếp theo
        if i < len(coords) - 1:
            time.sleep(SEND_INTERVAL)

    # 4. Ngắt kết nối
    client.disconnect()
    print("\nHoàn thành mô phỏng.")

if __name__ == "__main__":
    main()