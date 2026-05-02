# Hướng Dẫn Vận Hành Toàn Trình (End-to-End) Pipeline Predictive Maintenance

Tài liệu này là "Sổ Tay Vận Hành" dành cho kỹ sư MLOps, Data Engineer hoặc đội nhóm dự án. Tài liệu bao quát cách thiết lập, chạy, theo dõi và gỡ lỗi toàn bộ hệ thống cho **CẢ 2 LUỒNG DỮ LIỆU**: 
1. **NASA Turbofan FD001**
2. **PHM IEEE 2012 Bearing**

Toàn bộ hệ thống chạy trên nền tảng Big Data (Kafka, Spark), lưu trữ Medallion Lakehouse (MinIO Delta) và phục vụ BI/Ops (Postgres + Grafana/Superset) bằng Docker Compose, được điều phối tập trung qua script `run.ps1`.

---

## 1. Chuẩn Bị Môi Trường Của Host (Máy Mẹ)

Hệ thống phân tách rõ ràng: Database, Pipeline (Kafka/Spark), Storage (MinIO) chạy trên Docker; còn AI Inference (TensorFlow/MLflow) sẽ sử dụng tài nguyên của máy Host để tránh Overhead. Do đó Host cần:

1. **Python 3.10 hoặc 3.11 (khuyến nghị mạnh: 3.11)**
2. **Tạo và kích hoạt môi trường ảo (venv)**:
   Mở **PowerShell (Run as Administrator lần đầu để set policy)** tại thư mục gốc:
   ```powershell
   Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
   python -m venv venv
   .\venv\Scripts\Activate.ps1
   ```
3. **Cài đặt thư viện bắt buộc**:
   Hệ thống có file `run.ps1` làm nhiệm vụ điều phối và Automation tất cả các lệnh.
   ```powershell
   powershell ./run.ps1 -Action install
   ```

**Lưu ý quan trọng về PowerShell (tránh lỗi "not digitally signed")**

Nếu máy bạn chặn script chưa ký số, hãy dùng cú pháp này cho mọi lệnh:
```powershell
powershell -ExecutionPolicy Bypass -File ./run.ps1 -Action <ten-action>
```

Ví dụ:
```powershell
powershell -ExecutionPolicy Bypass -File ./run.ps1 -Action up-core
```

Ở lần khởi động đầu tiên, Kafka có thể cần thêm một nhịp để đồng bộ metadata topic. Nếu lần 1 báo lỗi topic chưa tồn tại, chạy lại đúng lệnh thêm 1 lần.

---

## 2. LUỒNG 1: Khởi Chạy NASA Turbofan FD001

Dữ liệu FD001 có đặc thù là đọc trực tiếp giá trị cảm biến của các chu kỳ bay (time cycles).

**Bước 2.1: Chạy Job Build Silver/Gold & Training**
Chạy Job Spark độc lập để convert dữ liệu lịch sử trên file CSV thành cấu trúc Delta (Silver/Gold) và chuẩn bị cho Model.
```powershell
powershell ./run.ps1 -Action build-train-silver-gold
```

**Bước 2.2: Bật Mạng Lưới Big Data Core + Inference**
Bạn có thể bật từng phần hoặc bật hàng loạt bằng lệnh gom (tùy vào tài nguyên máy):
```powershell
powershell ./run.ps1 -Action up-core
powershell ./run.ps1 -Action up-ingest
powershell ./run.ps1 -Action up-bronze
powershell ./run.ps1 -Action up-ops
```

*Hoặc đơn giản hơn (nếu chạy tất cả luồng NASA):*
```powershell
powershell ./run.ps1 -Action up
```
* Broker (Kafka, EMQX) sẽ thức dậy.
* Spark Streaming (Bronze) lắng nghe Raw Topic.
* Spark Streaming (Silver/Gold) load Model Keras và chạy Inference theo lô (micro-batch) thời gian thực.

**Bước 2.3: Bật Mô Phỏng (IoT Edge Sensor Simulator)**
Mở **1 cửa sổ PowerShell MỚI**, nhớ kích hoạt `venv` và chạy lệnh giả lập đẩy dữ liệu từ CSV lên hệ thống qua MQTT:
```powershell
powershell ./run.ps1 -Action replay
```

---

## 3. LUỒNG 2: Khởi Chạy PHM IEEE 2012 (Dữ Liệu Vòng Bi)

Dữ liệu PHM là sóng gia tốc. Dữ liệu thô được thiết bị Edge tính toán ra đặc trưng FFT & Stats trước khi bắn lên Cloud.

**Bước 3.1: Train model PHM & Kích hoạt MLflow Registry**
Trích xuất đặc trưng từ tập học (`Learning_set`) và chỉ dùng tập này để huấn luyện model **TCN** (Temporal Convolutional Network), sau đó đăng ký tự động lên MLflow.
`Full_Test_Set` không dùng để train; tập này chỉ dùng cho replay streaming khi demo.
Mô hình sử dụng thêm các feature phổ tần (spectral entropy, band energy, peak frequency) và chia train/val theo bearing để tránh leakage.
Chạy 2 lệnh docker để build trước:
docker compose build mlflow 
docker compose up -d mlflow
```powershell
powershell -ExecutionPolicy Bypass -File ./run.ps1 -Action train-phm
```

> **Ghi chú ổn định trên Windows:** 
> - `train-phm` có retry tự động (3 lần) và ép cấu hình CPU-safe (TF_ENABLE_ONEDNN_OPTS=0)
> - MLflow health check: Script chờ MLflow sẵn sàng tại port 5000 (max 30s) trước khi bắt đầu huấn luyện
> - **Metrics Output**: Huấn luyện in ra từng epoch:
>   - Loss, Validation Loss, MAE, Validation MAE theo real-time
>   - RMSE, MAPE
>   - PHM Score (val)
>   - Chỉ báo ✓ khi validation loss cải thiện
>   - Tóm tắt cuối cùng với % cải thiện
> - **Fallback Mode**: Nếu MLflow không available, training vẫn chạy ở chế độ local (model lưu tại file)

**Bước 3.2: Bật mạng lưới Big Data cho Luồng PHM**
Luồng này tái sử dụng Kafka/MinIO nhưng chạy các container Data Pipeline và AI Inference riêng biệt. Kích hoạt toàn bộ luồng PHM bằng lệnh:
```powershell
powershell -ExecutionPolicy Bypass -File ./run.ps1 -Action up-phm
```
docker compose --profile dashboard restart gold-sync
powershell -ExecutionPolicy Bypass -File ./run.ps1 -Action refresh-superset
**Bước 3.3: Bật Mô Phỏng (Edge Computing)**
Mở **1 cửa sổ PowerShell MỚI**, kích hoạt `venv` và chạy:
```powershell
powershell -ExecutionPolicy Bypass -File ./run.ps1 -Action replay-phm
```
Lệnh này giả lập thiết bị Edge, đọc file gia tốc, ép logic Fourier Transform (FFT) và publish lên EMQX.
Mỗi lần replay sẽ sinh một `run_id` (timestamp) để nhóm dữ liệu theo phiên chạy.

**Lưu ý quan trọng:** replay PHM nên trỏ vào `Data/phm-ieee-2012-data-challenge-dataset-master/Full_Test_Set`. Không replay bằng `Learning_set`, vì đó là tập train.

**Bước 3.4: (Tuỳ chọn) Replay DLQ cho PHM**
Nếu có bản tin lỗi nằm trong `pdm.phm.raw.dlq`, bạn có thể chạy lại sau khi đã sửa dữ liệu nguồn:
```powershell
powershell -ExecutionPolicy Bypass -File ./run.ps1 -Action replay-phm-dlq
```
Lệnh này sẽ validate lại payload và bắn sang `pdm.phm.raw` nếu hợp lệ.

---

## 4. Chạy Giám Sát Và Dashboard Hình Ảnh (Áp Dụng Chung)

Sau khi dữ liệu đã tuôn trào thành công vào các Lakehouse (MinIO), bạn cần mở cửa ngõ cho Kỹ Sư Quan Sát:

**Bước 4.1: Bật Cơ Sở Dữ Liệu Dashboard & BI Tools**
```powershell
powershell -ExecutionPolicy Bypass -File ./run.ps1 -Action up-dashboard
```
* Container `dashboard-db` (Postgres) sẽ được mount lên.
* Job `gold-sync` sẽ tự động đọc Delta Lake và đồng bộ cho cả bảng CMAPSS và bảng PHM.
* Grafana (`http://localhost:3000`) và Superset (`http://localhost:8088`) thức dậy.

**Bước 4.2: Refresh Metadata cho Superset**
Do Superset cần định nghĩa lại Database Schema sau khi `gold-sync` chạy, hãy chạy thêm lệnh cập nhật (sau khi warehouse đã có dữ liệu):
```powershell
powershell -ExecutionPolicy Bypass -File ./run.ps1 -Action refresh-superset
```

**Bước 4.3: Truy Cập Và Sử Dụng**
1. **Grafana (Live Ops / Command Center)**
   * **URL:** `http://localhost:3000` (User/Pass: `admin`/`admin`)
   * **Sử dụng:** Vào Dashboards, bạn sẽ thấy 2 Dashboard (Tự động nạp sẵn):
     - `PDM Gold Overview` (NASA Turbofan)
     - `PHM Gold Overview` (Trục bi PHM IEEE 2012)
2. **Superset (Luồng Điều Tra & Phân Rã Lỗi Ngành Sâu)**
   * **URL:** `http://localhost:8088` (User/Pass: `admin`/`admin`)
   * **Sử dụng:** Quan sát Detail Analytics như Risk Components hay Sensor Breakdown của từng thiết bị độc lập!

---

## 5. Gỡ Lỗi & Troubleshooting

### 5.1 Lỗi MLflow Connection: "Connection aborted by the software in your host machine"

**Nguyên nhân:** MLflow server tại `http://localhost:5000` không phản hồi

**Giải pháp:**
1. **Kiểm tra MLflow container đang chạy không:**
   ```powershell
   docker ps | findstr mlflow
   ```
   Nếu không có, hãy khởi động thủ công: `docker compose --profile train up -d mlflow`

2. **Kiểm tra log MLflow:**
   ```powershell
   docker logs $(docker ps -a -q -f name=mlflow) --tail 50
   ```

3. **Kiểm tra port 5000 có bị chiếm chưa:**
   ```powershell
   netstat -ano | findstr :5000
   ```

4. **Chờ MLflow khởi động:** Bạn có thể tạo script chờ:
   ```powershell
   # Chờ tới 30 giây cho MLflow sẵn sàng
   for ($i = 0; $i -lt 30; $i++) {
       try {
           $response = Invoke-WebRequest -Uri "http://localhost:5000/health" -ErrorAction SilentlyContinue
           if ($response.StatusCode -eq 200) {
               Write-Host "✓ MLflow ready"
               break
           }
       }
       catch { Start-Sleep -Seconds 1 }
   }
   ```

**Lưu ý:** Script `train-phm` đã tích hợp sẵn MLflow health check (chờ max 30 giây). Nếu MLflow không available, training sẽ chạy ở chế độ local (mô hình lưu tại file).

---

## 6. PHM Gold Tables (bổ sung)

Ngoài các bảng hiện có, luồng PHM hiện ghi thêm:
- `prediction_history_phm`
- `alert_history_phm`
- `pipeline_quality_phm`

Mỗi bản ghi PHM có thêm `run_id` để phân tách các lần replay. Dashboard sẽ mặc định hiển thị **run_id mới nhất**.

Các bảng này phục vụ phân tích lịch sử, theo dõi chất lượng pipeline và hiển thị trend trên dashboard.

---

### 5.2 Metrics Output During Training

**Đầu ra ước vọng (Expected Output):**

Khi chạy `train-phm`, bạn sẽ thấy:

```
======================================================================
🚀 Starting PHM IEEE 2012 Model Training Pipeline...
======================================================================

📊 PHASE 1: Feature Extraction
----------------------------------------------------------------------
Extracting 2803 files for Bearing1_1...
Extracting 871 files for Bearing1_2...
...
✓ Saved extracted features to Data/train_history_phm.csv
  Total samples: 7534, Features: 23

📈 PHASE 2: Data Preprocessing & Normalization
----------------------------------------------------------------------
✓ Normalized 20 features using MinMaxScaler
✓ Clipped RUL to max 200.0 hours

🔄 PHASE 3: Sequence Generation
----------------------------------------------------------------------
✓ Training sequences generated:
  X shape: (7354, 30, 20) (sequences, timesteps, features)
  y shape: (7354,)
  RUL range: [0.00, 200.00] hours

🏗️ PHASE 4: Model Architecture
----------------------------------------------------------------------
✓ LSTM Model created:
  - 2 LSTM layers (64 + 32 units)
  - Dropout: 0.2
  - Optimizer: Adam (lr=0.001)
  - Loss: MSE, Metrics: MAE
  - Total parameters: 27,777

🚂 PHASE 5: Model Training
----------------------------------------------------------------------
✓ MLflow server ready at http://localhost:5000
  Epoch  1/15 | Loss: 0.453216 | Val Loss: 0.312448 | MAE: 0.532810 | Val MAE: 0.442189 ✓
  Epoch  2/15 | Loss: 0.312458 | Val Loss: 0.287392 | MAE: 0.442893 | Val MAE: 0.412503 ✓
  ...
  Epoch 15/15 | Loss: 0.245812 | Val Loss: 0.268734 | MAE: 0.381256 | Val MAE: 0.389201

======================================================================
✓ TRAINING COMPLETED!
======================================================================
📊 FINAL METRICS SUMMARY:
----------------------------------------------------------------------
Training Loss:      0.245812
Validation Loss:    0.268734
Training MAE:       0.381256
Validation MAE:     0.389201
----------------------------------------------------------------------
Loss Improvement:   45.76%
Val Loss Improvement: 14.02%
======================================================================
```

**Giải thích Metrics:**
- **Loss:** Mean Squared Error (MSE) trên tập training/validation
- **MAE:** Mean Absolute Error - độ lệch trung bình dự đoán RUL
- **✓:** Chỉ báo validation loss đã cải thiện so với epoch trước
- **Improvement:** % cải thiện từ epoch 1 tới epoch cuối

---

## 6. Teardown & Kiểm Tra Sức Khỏe (Status Check)

**Dọn dẹp hệ thống rác (Tắt toàn bộ)**
Nếu muốn làm mới hoặc tắt dịch vụ đi ngủ:
```powershell
powershell ./run.ps1 -Action down-all
```
*(Lệnh này huỷ toàn bộ Docker Container và Networks).*

**Để bắt bệnh nếu hệ thống gặp trục trặc:**
- Xem trạng thái sống/chết của các container:
  ```powershell
  powershell ./run.ps1 -Action status
  ```
- Kiểm tra sức khỏe dịch vụ:
  ```powershell
  powershell ./run.ps1 -Action health
  ```
- Nơi coi trực tiếp dữ liệu (No-code):
  * **Kafka UI:** `http://localhost:8080` (Check dòng dữ liệu Ingest)
  * **MinIO Console:** `http://localhost:9001` (User: `minioadmin`, Pass: `minioadmin123`) - Xem storage file Delta parquet.
  * **MLflow Model Registry:** `http://localhost:5000` - Quản lý version model AI. 

> **Mẹo:** Đừng quên chạy `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` mỗi khi bạn vô tình mở terminal PowerShell mới!
