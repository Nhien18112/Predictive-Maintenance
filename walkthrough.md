# Hoàn Tất Tích Hợp IEEE PHM 2012 & Nâng Cấp Kiến Trúc MLOps

Quá trình tách biệt luồng dữ liệu mới không những tôn trọng hoàn toàn mã nguồn CMAPSS hiện tại mà còn tích hợp thành công hai trụ cột thiết kế lớn để sẵn sàng cho Production. Dưới đây là những gì đã được cài đặt:

## 1. MLOps bằng MLflow & PySpark Pandas UDF 🚀

Thay vì tải trực tiếp các tệp `.keras` tĩnh như trước, hệ thống đã được "tiến hóa" với giải pháp chuẩn mực:

- **MLflow Tracking Server**: Được thêm vào qua file `docker-compose.yml`. MLflow sẽ sử dụng SQLite làm Metadata Store và MinIO (s3://lakehouse/mlflow) là Artifact Store. Truy cập tại `http://localhost:5000`.
- **Huấn Luyện (Inference Engine) Độc Lập**: Tôi đã build sẵn file `scripts/train_phm_model.py`. Khác với dummy data, script này **đọc thẳng** vào hàng vạn file CSV có thật trong folder `Learning_set/`, thực hiện Feature Extraction (RMS, Kurtosis, Skewness, P2P..) trực tiếp ra Pandas DataFrame.
  - Sau đó train LSTM và **log model tự động lên MLflow** dưới trạng thái (alias) là `production`.
- **Inference Tốc Độ Cao Bằng Arrow/Pandas UDF**: Dưới file `stream_silver_gold_phm.py`, Spark được tối ưu hóa thông qua cơ chế `groupBy(...).applyInPandas()`. Spark sẽ tạo một lô Apache Arrow lớn, nạp từ Model tải từ `mlflow:5000` theo đường dẫn register URI, tránh hiện trạng rò rỉ bộ nhớ (memory leaks) khi dự đoán dữ liệu streaming.

## 2. Hoạch Định Kiến Trúc Dữ Liệu Song Song (PHM vs CMAPSS)

Hệ thống cũ không bị thay thế, IEEE PHM được kết nối như một Data Lakehouse Node thứ 2:

* **Edge IoT Simulator**: `simulator/replay_phm_mqtt.py` đóng vai trò "cụm biên trích xuất". Tức là, mỗi giây nó thay vì gửi 25.600 array gia tốc thì nó xử lý *Edge Computing* ngay lập tức, tóm gọn về 10 "thống kê tĩnh" (RMS, đỉnh, độ lệch...). Qua đó topic `factory/pdm/phm/raw` vô cùng "nhẹ nhàng".
* **MQTT to Kafka Bridge**: File `mqtt_to_kafka_bridge_phm.py` tiếp nhận JSON vừa tính theo thời gian thực để gửi vào Kafka.
* **Medallion Spark Jobs**: `stream_bronze_telemetry_phm.py` hứng dữ liệu raw, tạo checkpoint riêng. Và Silver->Gold tính RUL.

## Hướng Dẫn Kích Hoạt Hệ Thống

Thiết lập mới đã được map thẳng vào script PowerShell với các flag chuẩn. 

> [!IMPORTANT]
> **Khởi động luồng mới**
> 1. Chạy `powershell ./run.ps1 -Action train-phm` để huấn luyện thực tế trên data `phm-ieee-2012`. MLflow và MinIO sẽ được bật lên, LSTM model sẽ học trên máy bạn và lưu vào MLflow Registry. Thời gian train phụ thuộc CPU của bạn (tầm 1~3 phút).
> 2. Sau khi Model "Ready", khởi động cụm Pipeline Realtime bằng lệnh: `powershell ./run.ps1 -Action up-phm`
> 3. Mô phỏng tín hiệu: Lệnh `powershell ./run.ps1 -Action replay-phm` sẽ gọi file `simulator/replay_phm_mqtt.py` giả lập đọc `Bearing1_4` trong Test_set và đẩy lên MQTT. Bảng Gold của PHM sẽ hiện trong Dashboard.

Chúc mừng bạn đã tạo nên một kiến trúc Industrial Predictive Maintenance thực thụ!
