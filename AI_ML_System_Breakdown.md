# AI/ML System Architecture Breakdown

This document provides a detailed technical breakdown of the Artificial Intelligence and Machine Learning systems within the Predictive Maintenance project.

## Overview

The project features a **dual-stream architecture** running two separate predictive maintenance pipelines in parallel:
1. **PHM IEEE 2012 Data Challenge Stream:** Focuses on bearing degradation using vibration/accelerometer data.
2. **NASA Turbofan CMAPSS Stream:** Focuses on engine degradation using multivariate sensor telemetry.

Both streams share a unified "Lakehouse" paradigm (Bronze -> Silver -> Gold), but they differ significantly in their machine learning approaches (XGBoost vs. LSTM).

## Data Splitting Strategy

Based on the actual implementation in the training pipelines, the data splitting strategies are handled differently for each stream:

**1. PHM IEEE 2012 Stream (XGBoost)**
- **Split Ratio:** The `Learning_set` data is split into **85% Training** and **15% Validation** (`test_size=0.15`). The `Full_Test_Set` is held out entirely for testing/streaming.
- **Splitting Method:** A standard random split is used (`sklearn.model_selection.train_test_split(random_state=42)`). The data is **not** grouped chronologically or by Machine ID (`unit_nr`) prior to splitting. This means random cycles from all bearings are mixed into both sets, which introduces a risk of data leakage (time-series interpolation) because adjacent cycles from the same bearing can appear in both training and validation sets.
- **Cross-Validation:** No K-Fold or specific time-series cross-validation techniques are employed. Early stopping relies on the randomly sampled 15% validation set.

**2. NASA Turbofan CMAPSS Stream (LSTM)**
- **Split Ratio:** **100% Training**. No explicit validation split is performed during the `model.fit()` phase.
- **Splitting Method:** All loaded data from the Gold Delta table is passed as training data (`X_train, y_train`). The Keras `EarlyStopping` and `ModelCheckpoint` callbacks are configured to monitor the internal **training loss** (`monitor='loss'`) rather than validation loss. The Test set is entirely separated and reserved for the streaming inference stage.
- **Cross-Validation:** No cross-validation is used.

---

## 1. PHM IEEE 2012 Stream (Bearing Vibration)

### Model Training
- **Algorithm:** XGBoost Regressor (`reg:squarederror`).
- **Frameworks:** XGBoost, Scikit-Learn, MLflow.
- **Hyperparameters:** `max_depth: 6`, `learning_rate: 0.05`, `n_estimators: 500`, `subsample: 0.8`, `colsample_bytree: 0.8`.
- **Target Variable (RUL):** Remaining Useful Life is modeled as a piecewise linear function. It remains flat at `130.0` (healthy state) and then decays linearly as the bearing degrades.
- **Model Registry:** The trained XGBoost model is logged and registered to an MLflow server with the `production` alias.

### Datasets (Train & Validate)
- **Source:** PHM IEEE 2012 Data Challenge (`Learning_set`).
- **Ingestion:** Raw CSV files containing high-frequency accelerometer readings (`Acc_x`, `Acc_y`).
- **Preprocessing & Feature Engineering:**
  - Extracts statistical features: Root Mean Square (RMS), Kurtosis, Peak-to-Peak, Crest Factor, Skewness.
  - Extracts frequency-domain features: Fast Fourier Transform (FFT) amplitudes, Spectral Entropy, Band Energy, Peak Frequency.
  - Normalization: Features are scaled using `RobustScaler` and clipped to `±5` to handle outliers.
  - Temporal features: Calculates Z-scores relative to a bearing's baseline (first 50 cycles) and rate of change (slope over the last 10 cycles).

### Inference / Application
- **Streaming Context:** Processed via PySpark Structured Streaming (`stream_silver_gold_phm.py`).
- **Batch Inference:** Uses Spark's `applyInPandas` (Pandas UDF) to distribute inference across the cluster.
- **Execution:** 
  - Gathers the last 30 cycles (`SEQUENCE_LENGTH = 30`) for a given machine.
  - Calculates features on-the-fly and scales them using the `RobustScaler` fitted during training.
  - Loads the production XGBoost model dynamically from MLflow.
  - Outputs a predicted RUL. It also calculates a heuristic "Symptom Score" and "Trend Score" to gauge immediate degradation speed.

---

## 2. NASA Turbofan CMAPSS Stream (Engine Telemetry)

### Model Training
- **Algorithm:** Long Short-Term Memory (LSTM) Neural Network.
- **Frameworks:** TensorFlow / Keras.
- **Hyperparameters:** Trained with `EarlyStopping` (patience=5 on loss) and `ModelCheckpoint` to save the best weights. Batch size and epochs are managed via `src.config`.
- **Target Variable:** Piecewise linear RUL (clipped at `125.0`).

### Datasets (Train & Validate)
- **Source:** NASA CMAPSS (Commercial Modular Aero-Propulsion System Simulation) datasets.
- **Preprocessing:**
  - Feature Selection: Drops constant or non-informative columns (e.g., `setting_3`, `s_1`, `s_5`, etc.), leaving 17 active features.
  - Smoothing: Applies Exponential Weighted Moving Average (EWMA) with `alpha=0.1` to reduce noise.
  - Scaling: Uses `MinMaxScaler` fitted on the training data.
  - Windowing: Formats data into 3D tensors of shape `(batch_size, SEQUENCE_LENGTH=25, num_features)` for the LSTM.

### Inference / Application
- **Streaming Context:** Processed via PySpark Structured Streaming (`stream_silver_gold_inference_alert.py`).
- **Execution:**
  - A `foreachBatch` loop collects valid data for active engines.
  - Uses an `InferenceEngine` wrapper class to load the saved `.keras` model.
  - Prepares the 25-cycle sequence (smoothing and min-max scaling).
  - Passes the `(1, 25, 17)` tensor into the LSTM to retrieve the RUL prediction.

---

## 3. End-to-End Pipeline Architecture

The system utilizes a Medallion Architecture (Bronze -> Silver -> Gold) over a Delta Lake.

### 1. Data Extraction & Ingestion (Bronze)
- Simulators publish JSON payloads to **MQTT/Kafka** topics.
- Spark Streaming jobs (`stream_bronze_telemetry...`) read raw strings from Kafka.
- Schemas are applied, and raw payloads are dumped into append-only **Bronze Delta tables** (`telemetry_raw`), with invalid formats routed to Dead Letter Queues (DLQ).

### 2. Data Transformation (Silver)
- Streams read from Bronze.
- Data is type-casted, validated (e.g., rejecting negative cycles or missing features), and flattened.
- Cross-batch deduplication ensures exactly-once semantics per `unit_nr` and `time_cycle`.
- Clean data is saved to **Silver Delta tables** (`stream_clean`).

### 3. Model Inference & Alerting (Gold)
- Streams aggregate the Silver data into windows (25 or 30 cycles depending on the stream).
- The models (XGBoost/LSTM) are invoked to predict the Remaining Useful Life (RUL).
- **Alert Logic:** 
  - Computes a blended "Risk Score" derived from the predicted RUL, Symptom Score (deviation from baseline), and Trend Score (slope of degradation).
  - Categorizes risk into states: `Normal`, `Watch`, `Warning`, `Critical`.
  - **Hysteresis:** Applies anti-flicker logic, requiring a state change to persist for a certain number of cycles (`HYSTERESIS_UP=2`, `HYSTERESIS_DOWN=3`) before confirming the new alert level.
- Outputs are written to **Gold Delta tables**: `prediction_current`, `prediction_history`, `alert_current`, `alert_history`.

### 4. Application Serving
- The Gold `_current` tables represent the materialized state of all active machines.
- A Backend/API layer queries these tables (using DuckDB or Spark SQL).
- A Frontend UI (Dashboard) continuously polls or receives pushed updates, rendering real-time RUL gauges, trend charts, and alerting banners to end-users.

---

## Evaluation Metrics

- **XGBoost (PHM IEEE 2012 Stream):** The evaluation metrics are explicitly defined and calculated within `scripts/train_phm_model.py`. The model evaluates predictions using **RMSE (Root Mean Squared Error)** and **MAE (Mean Absolute Error)**. These are logged directly to the MLflow tracking server.
- **LSTM (NASA CMAPSS Stream):** The neural network is compiled with a standard **Mean Squared Error (`mse`)** loss function, while explicitly tracking Keras's **`root_mean_squared_error`** as a metric (defined in `NASA-Turbofan-Predictive-Modeling/src/model.py`).

## Infrastructure & Compute

- **Deployment Environment:** The entire pipeline is containerized and orchestrated locally via **Docker Compose** (`docker-compose.yml`). The data streaming layers (Bronze, Silver, Gold) are executed by local **Apache Spark** containers (`apache/spark:3.5.1`).
- **Compute Resources:** The deep learning models (LSTM) and the XGBoost models are currently running exclusively on **CPUs**. There are no GPU pass-through configurations (e.g., Nvidia runtime bindings) allocated to the Spark executor containers or the training pipelines within the Docker Compose setup.

## Model Retraining Strategy

- **Retraining Automation:** There is **no automated mechanism** (such as Apache Airflow or Prefect DAGs) configured to trigger retraining periodically or based on data drift. 
- **Execution:** Model training is currently an entirely **offline, manual process**. The training workflows are executed by manually running scripts (like `scripts/train_phm_model.py`) or invoking the `train` profile in Docker Compose to build the training datasets (`build_train_silver_gold.py`).

## Known Limitations & Future Improvements (Technical Debt)

Based on the current state of the ML pipelines, the following critical improvements should be prioritized to prevent false model confidence and improve production robustness:

**1. Data Leakage in XGBoost Training**
- *Issue:* The PHM stream utilizes a standard random `train_test_split`. Because the data consists of highly correlated sequential time-series cycles, mixing random cycles from the same bearing (`unit_nr`) into both the Train and Validation sets allows the model to "cheat" via interpolation, leading to artificially inflated validation scores.
- *Solution:* Implement a **Group-based Split** (e.g., `GroupShuffleSplit` or `GroupKFold` in scikit-learn) partitioned by `unit_nr` to ensure that entire bearings are strictly held out for validation.

**2. Missing Validation Split in LSTM Training**
- *Issue:* In `NASA-Turbofan-Predictive-Modeling/train.py`, `model.fit()` is executed on 100% of the training data without a `validation_data` or `validation_split` parameter. Consequently, the Keras `EarlyStopping` callback is monitoring *training loss* (`monitor='loss'`) instead of validation loss, which risks severe overfitting.
- *Solution:* Introduce an explicit validation split (e.g., grouped by engine ID) and update Keras callbacks to monitor `val_loss`.

**3. Orchestration & Retraining**
- *Issue:* Models are static and trained entirely offline.
- *Solution:* Introduce an orchestrator (like Apache Airflow) to schedule recurring training pipelines and evaluate data/concept drift to automatically refresh production models.

**4. Compute Acceleration**
- *Issue:* The LSTM model is CPU-bound, which limits scaling to larger datasets.
- *Solution:* Add GPU support to the Docker Compose deployment for TensorFlow.
