# Hướng Dẫn Vận Hành Hệ Thống Predictive Maintenance (PHM IEEE 2012)

Tài liệu này hướng dẫn bạn cách khởi chạy và giám sát toàn trình (End-to-End) luồng Big Data MLOps dành riêng cho dataset vòng bi **PHM IEEE 2012**. Hệ thống sử dụng mô hình **XGBoost (Pure Tabular)** để dự đoán RUL (Remaining Useful Life).

---

## 1. Cơ Chế Hoạt Động Của Pipeline

1. **Edge Simulator (`replay_phm_mqtt.py`)**: Tính toán tức thời **26 Đặc trưng** (RMS, Kurtosis, Peak Frequency...) và bắn bản tin JSON lên `EMQX (MQTT)`.
2. **Ingest Node (`mqtt_to_kafka_bridge_phm.py`)**: Hứng JSON từ MQTT và đẩy vào Kafka topic `pdm.phm.raw`.
3. **Data Lakehouse (MinIO & Apache Spark)**:
   - `stream_bronze_telemetry_phm.py`: Ép kiểu dữ liệu đổ vào kho Bronze.
   - `stream_silver_gold_phm.py`: Trích xuất batch mới nhất, áp dụng `RobustScaler`, tải model XGBoost từ `MLflow` để dự đoán RUL. Chấm điểm rủi ro (Risk Score) và đẩy kết quả ra kho Gold.
4. **Dashboard Sink (`gold-sync`)**: Đồng bộ dữ liệu Gold từ MinIO sang PostgreSQL. Từ đây Grafana sẽ truy vấn để vẽ Dashboard biểu đồ cảnh báo.

---

## 2. Kích Hoạt Hệ Thống (Thứ tự chuẩn)

Tất cả các lệnh điều khiển đều thông qua script `run.ps1`. Hãy mở PowerShell tại thư mục gốc của dự án:

### Bước 1: Huấn luyện Mô hình AI (Training)
Chạy lệnh này để hệ thống đọc dữ liệu tập `Learning_set`, trích xuất đặc trưng và train mô hình XGBoost.
```powershell
./run.ps1 -Action train-phm
```
*Kết quả:* Mô hình sẽ được đăng ký lên MLflow với tên `PHM_XGBoost_Model` (Alias: `production`).

### Bước 2: Bật toàn bộ Hệ thống (Kafka, Spark, Database, Grafana)
Bật các service lõi, Data Lake, luồng xử lý Spark Streaming và Dashboard.
```powershell
./run.ps1 -Action up-phm
./run.ps1 -Action up-dashboard
```

### Bước 3: Đẩy dữ liệu Sensor giả lập (Replay / Streaming)
Bắt đầu bắn dữ liệu của tập `Full_Test_Set` vào hệ thống để quan sát.
```powershell
./run.ps1 -Action replay-phm
```
Lúc này, hãy mở trình duyệt vào **Grafana (`http://localhost:3000` - User: admin / Pass: admin)**, mở Dashboard **PHM Gold Overview** để xem kết quả RUL và Cảnh báo cập nhật theo thời gian thực (real-time).

---

## 3. Cách Reset Hệ Thống (Demo lại từ đầu)

Vì đây là môi trường giả lập (Replay), mỗi khi bạn muốn thuyết trình và biểu diễn lại biểu đồ vòng bi từ trạng thái "khỏe mạnh 100%" (chu kỳ 0), bạn **phải xóa sạch dữ liệu cũ** theo quy trình 4 bước sau:

1. **Dừng luồng hiện tại:**
   ```powershell
   ./run.ps1 -Action down-phm
   ```
2. **Xóa toàn bộ Database, Kafka Topic và MinIO Delta Lake:**
   ```powershell
   ./run.ps1 -Action clean-phm
   ```
3. **Khởi động lại luồng (Tạo bảng mới tinh):**
   ```powershell
   ./run.ps1 -Action up-phm
   ```
4. **Bắn lại giả lập dữ liệu:**
   ```powershell
   ./run.ps1 -Action replay-phm
   ```

---

## 4. Các Trạm Giám Sát (Troubleshooting)

Nếu hệ thống không lên số liệu, hãy kiểm tra lần lượt:

1. **Trạm Model (MLflow) - `http://localhost:5000`**: Đảm bảo tab Models có `PHM_XGBoost_Model`.
2. **Trạm Dữ Liệu Thô (Kafka UI) - `http://localhost:8080`**: Đảm bảo Topic `pdm.phm.raw` có tin nhắn đang bay vào liên tục.
3. **Trạm Data Lake (MinIO) - `http://localhost:9001`**: Đảm bảo bucket `lakehouse` có thư mục `silver/phm_stream_clean` và `gold/...`
4. **Trạm Cảnh Báo (Grafana) - `http://localhost:3000`**: Panel phải hiển thị đồ thị của Bearing 1_3, Bearing 1_4, Bearing 2_6...

Chúc bạn bảo vệ đồ án thành công xuất sắc!
