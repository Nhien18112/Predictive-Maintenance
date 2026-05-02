# Hướng Dẫn Vận Hành Hệ Thống Predictive Maintenance (PHM IEEE 2012)

Tài liệu này hướng dẫn bạn cách khởi chạy và giám sát toàn trình (End-to-End) luồng Big Data MLOps dành riêng cho dataset ổ dĩa cơ học **PHM IEEE 2012**. Hệ thống được thiết kế theo tư duy kiến trúc **Cấp Độ 2 (Level 2 Architecture: Time-Domain + Frequency-Domain Edge Computing)**.

---

## 1. Cơ Chế Hoạt Động Của Pipeline

Trước khi gõ lệnh, hãy nhìn lướt qua cách dòng chảy dữ liệu di chuyển để dễ dàng debug nếu có sự cố:

1. **Edge Simulator (`replay_phm_mqtt.py`)**: Đóng vai trò là máy tính nhúng IoT (Raspberry Pi/PLC) đính trên vòng bi. Thay vì gửi hàng chục ngàn dòng gia tốc thô, nó tính toán tức thời **20 Đặc trưng** (10 đặc trưng thống kê như RMS, Kurtosis + 10 đặc trưng biên độ tần số sóng FFT). Bản tin JSON 20 biến siêu nhẹ được bắn lên `EMQX (MQTT)`.
2. **Ingest Node (`mqtt_to_kafka_bridge_phm.py`)**: Hứng JSON từ MQTT, Validate lược đồ schema (đảm bảo không bị thiếu field), và đổ vào vòi `Kafka`.
3. **Data Lakehouse (MinIO & Apache Spark)**:
   - `stream_bronze_telemetry_phm.py`: Ép kiểu dữ liệu đổ vào kho Bronze raw.
   - `stream_silver_gold_phm.py`: Trích xuất micro-batch gần nhất (chống lụt RAM), áp dụng Pre-trained Scaler, gọi model từ nhánh Production của `MLflow` để chạy Inference (Dự đoán RUL = Remaining Useful Life). Kết quả nhả ra kho Gold đính kèm *Symptom Score* và *Alert Level* (Có chống nảy Hysteresis).
4. **Dashboard Sink (`sync_gold_to_warehouse.py`)**: Dùng `DuckDB` đọc bảng Gold Delta từ MinIO, đẩy vào View của `PostgreSQL`. Từ đây Grafana và Superset sẽ đọc để vẽ Dashboard.

---

## 2. Chuẩn Bị Môi Trường (Data Science Host)

Vì hệ thống Machine Learning sẽ sử dụng tài nguyên Máy tính Thật (Local Host) của bạn để chạy đồ thị TensorFlow và XGBoost thay vì nhét chung vào Docker (nhằm giảm tải bộ nhớ Docker), bạn cần thiết lập một môi trường Python ảo (`venv`) và cài đặt các thư viện cần thiết.

Mở PowerShell tại thư mục gốc của dự án (`D:\BK_Document\252\DOAN\Predictive_Maintenance`) và thực hiện:

### Bước 1: Tạo và kích hoạt Môi trường Ảo (Venv)
```powershell
# Tạo môi trường ảo có tên venv
python -m venv venv

# Kích hoạt môi trường ảo (Nếu báo lỗi UnauthorizedAccess, chạy lệnh Set-ExecutionPolicy trước)
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\venv\Scripts\Activate.ps1
```
*(Nếu kích hoạt thành công, bạn sẽ thấy chữ `(venv)` hiện ở đầu dòng lệnh PowerShell).*

### Bước 2: Cài đặt tập Thư viện (Requirements)
Hệ thống đã gom toàn bộ các thư viện AI & Data Science (MLflow, TensorFlow, Scikit-learn...) vào một file `requirements.txt` nằm ở thư mục gốc. Bạn chỉ cần chạy:
```powershell
powershell ./run.ps1 -Action install
```
*(Lệnh này tương đương với `pip install -r requirements.txt`).*

---

## 3. Kích Hoạt Hệ Thống

Tất cả đã được mapping vào công cụ tự động `run.ps1` bằng PowerShell.

### Bước 1: Huấn luyện Mô hình Độc lập (Model Training)
Đầu tiên, hệ thống cần "học" cách ổ bi hao mòn để sinh ra Model LSTM.
```powershell
powershell ./run.ps1 -Action train-phm
```
> [!NOTE]
> **Điều gì đang xảy ra?**
> MinIO và MLflow Server sẽ được bật lên. Dữ liệu trong `Learning_set` được lôi ra tính toán 20 đặc trưng y hệt Edge Simulator. Code `train_phm_model.py` sẽ train một mô hình **Pure LSTM** và Đăng ký bản ghi lên `http://localhost:5000` với tên `PHM_LSTM_Model` (Alias: `production`).

### Bước 2: Kích hoạt toàn bộ Trạm Xử Lý (Core & Big Data)
Khởi động Kafka, MQTT, Spark Streaming (Bronze + Silver/Gold).
```powershell
powershell ./run.ps1 -Action up-phm
```
> [!TIP]
> Hãy đợi khoảng 15-30 giây để Apache Spark khởi chạy xong các Excecution Node tải Model từ MLflow về trước khi sang bước 3. 

### Bước 3: Đẩy dữ liệu Sensor giả lập (IoT Streaming)
Gửi dữ liệu kiểm thử (Ví dụ ổ bi `Bearing1_4` từ tập `Test_set`) vào hệ thống.
```powershell
powershell ./run.ps1 -Action replay-phm
```
> [!IMPORTANT]
> Script này sẽ quét liên tục các file CSV `acc_*.csv`, tính toán FFT và push lên MQTT mỗi 1 giây. Lúc này nhà máy của bạn đã chính thức **"Go-live"**.

### Bước 4: Khởi động hệ thống Dashboard (Tùy chọn)
Nếu bạn muốn ngắm dữ liệu trên đồ thị.
```powershell
powershell ./run.ps1 -Action up-dashboard
```

---

## 3. Cách Verify (Kiểm thử sức khỏe toàn hệ thống)

Bạn không cần mở code ra xem, hãy dùng các trạm gác (Port/Web UI) sau để xác nhận Pipeline "Sống hay Chết".

### 1. Trạm kiểm định Mô Hình (MLflow)
* Trực cập: `http://localhost:5000`
* Trong tab **Models**, bạn phải thấy `PHM_LSTM_Model` được gắn tag `production`. Nếu không có, bước 1 (Train) đã thất bại.

### 2. Trạm kiểm định Dây Cáp (Kafka UI)
* Truy cập: `http://localhost:8080`
* Kiểm tra Data Explorer của Topic `pdm.phm.raw`. Các bản tin phải đang trôi liên tục mang theo các thông số `fft_x_1`. Nếu topic này rỗng, Simulator đang bị nghẽn ở Bridge.

### 3. Trạm kiểm định Cục Lưu Trữ (MinIO Console)
* Truy cập: `http://localhost:9001` (User/Pass: `minioadmin` / `minioadmin123`)
* Vào bucket `lakehouse`, bạn phải thấy các thư mục `bronze/phm_raw` và `gold/prediction_current_phm` được cập nhật liên tục (Delta log đẻ ra các file json mới).

### 4. Bảng điều khiển Ops (Grafana / Superset)
Sau khi chạy *Bước 4*, kiểm tra Database PostgresSQL của bạn. Nếu các Views tên `v_phm_grafana_entrypoint` đã hiện diện, bạn có thể vào `http://localhost:3000` (Grafana) kết nối với Table này để giám sát cảnh báo "đỏ" thời gian thực.

Chúc bạn trình diễn (demo) đồ án của mình một cách hoàn hảo và mượt mà nhất!
