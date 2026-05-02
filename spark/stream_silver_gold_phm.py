import logging
import os
import json
import datetime
import numpy as np
import pandas as pd
from typing import Any, Dict, List, Tuple
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from sklearn.preprocessing import RobustScaler

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
LOGGER = logging.getLogger("stream_silver_gold_phm")

SEQUENCE_LENGTH = 30
BASE_FEATURE_COLS = [
    "rms_x", "rms_y", "kurt_x", "kurt_y", "p2p_x", "p2p_y", "crest_x", "crest_y", "skew_x", "skew_y",
    "spec_entropy_x", "spec_entropy_y", "band_energy_x", "band_energy_y", "peak_freq_x", "peak_freq_y",
    "fft_x_1", "fft_x_2", "fft_x_3", "fft_x_4", "fft_x_5",
    "fft_y_1", "fft_y_2", "fft_y_3", "fft_y_4", "fft_y_5"
]

FEATURE_COLS = [
    "time_norm",
    "rms_x", "rms_y", "kurt_x", "kurt_y", "p2p_x", "p2p_y", "crest_x", "crest_y", "skew_x", "skew_y",
    "spec_entropy_x", "spec_entropy_y", "band_energy_x", "band_energy_y", "peak_freq_x", "peak_freq_y",
    "fft_x_1", "fft_x_2", "fft_x_3", "fft_x_4", "fft_x_5",
    "fft_y_1", "fft_y_2", "fft_y_3", "fft_y_4", "fft_y_5",
    "rms_x_z", "rms_y_z", "kurt_x_z", "kurt_y_z", "crest_x_z", "crest_y_z",
    "rms_x_slope10", "kurt_x_slope10", "crest_x_slope10", "band_energy_x_slope10"
]
RUL_CLIP = 130.0   # Max cycles (matches train_phm_model FPT limit)
HISTORY_LOOKBACK_MULTIPLIER = 2

# ----------------- Worker Executor State -----------------
_model_cache = None
_scaler_cache = None

def get_scaler():
    global _scaler_cache
    if _scaler_cache is None:
        csv_path = "/app/Data/train_history_phm.csv"
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            sc = RobustScaler()
            sc.fit(df[FEATURE_COLS].values)
            _scaler_cache = sc
        else:
            _scaler_cache = RobustScaler() # Dummy fallback
    return _scaler_cache

def get_model():
    global _model_cache
    if _model_cache is None:
        try:
            import mlflow.xgboost
            mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
            _model_cache = mlflow.xgboost.load_model("models:/PHM_XGBoost_Model@production")
        except Exception as e:
            LOGGER.error(f"Error loading MLflow model: {e}")
            _model_cache = False
    return _model_cache


def delta_path_exists(spark: SparkSession, path: str) -> bool:
    try:
        spark.read.format("delta").load(path).limit(1).collect()
        return True
    except Exception:
        return False

def compute_symptom(window_df):
    """Compute symptom score and per-sensor details from a sliding window."""
    score = 0.0
    details = {}
    for sensor in BASE_FEATURE_COLS:
        vals = window_df[sensor].values.astype(np.float64)
        mean_val = float(np.mean(vals))
        std_val = float(np.std(vals))
        # Simple linear trend via polyfit slope
        if len(vals) > 1:
            trend = float(np.polyfit(np.arange(len(vals)), vals, 1)[0])
        else:
            trend = 0.0
        details[sensor] = {
            "mean": mean_val, "std": std_val,
            "trend": trend,
            "deviation": round(abs(mean_val), 4),
            "volatility": round(std_val, 4),
            "score": round(min(abs(trend) * 100 + std_val * 5, 100.0), 4),
        }
        score += std_val
    score = min(score * 10, 100.0)
    return round(score, 2), details


def compute_trend_score(window_df):
    """Measure how fast RMS_x is degrading (rising) over the window.
    Positive slope = vibration increasing = degradation accelerating.
    Returns 0-100.
    """
    rms_vals = window_df["rms_x"].values.astype(np.float64)
    if len(rms_vals) < 3:
        return 0.0
    slope = float(np.polyfit(np.arange(len(rms_vals)), rms_vals, 1)[0])
    # Normalise: slope > 0.05 g/measurement = very fast degradation
    trend_score = min(max(slope / 0.05 * 100, 0.0), 100.0)
    return round(trend_score, 2)


def write_pipeline_quality(silver: DataFrame, batch_id: int, path: str, bronze_in_batch: int, run_id: str) -> None:
    row = (
        silver.agg(
            F.count(F.lit(1)).alias("silver_total_records"),
            F.sum(F.when(F.col("is_valid") == F.lit(True), 1).otherwise(0)).alias(
                "silver_valid_records"
            ),
            F.sum(F.when(F.col("is_valid") == F.lit(False), 1).otherwise(0)).alias(
                "silver_invalid_records"
            ),
            F.countDistinct("unit_nr", "time_cycles").alias("silver_distinct_keys"),
        )
        .withColumn("run_id", F.lit(run_id))
        .withColumn("bronze_in_batch", F.lit(int(bronze_in_batch)))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("quality_time", F.current_timestamp())
    )
    row.write.format("delta").mode("append").save(path)

# ----------------- PANDAS UDF -----------------
def predict_rul_pandas_udf(pdf: pd.DataFrame) -> pd.DataFrame:
    """Grouped pd.DataFrame containing records for ONE unit_nr"""
    pdf = pdf.sort_values("time_cycles").reset_index(drop=True)
    if len(pdf) < SEQUENCE_LENGTH:
        return pd.DataFrame() # Return empty df for this group

    window = pdf.tail(SEQUENCE_LENGTH).copy()
    window[BASE_FEATURE_COLS] = window[BASE_FEATURE_COLS].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    
    # 1. time_norm
    ASSUMED_LIFE = 1428.0
    window['time_norm'] = window['time_cycles'] / ASSUMED_LIFE

    # 2. z-scores (using window mean/std as approximation for streaming)
    for col in ["rms_x", "rms_y", "kurt_x", "kurt_y", "crest_x", "crest_y"]:
        mu = window[col].mean()
        sigma = window[col].std() + 1e-8
        window[f"{col}_z"] = (window[col] - mu) / sigma

    # 3. slope10
    for col in ["rms_x", "kurt_x", "crest_x", "band_energy_x"]:
        window[f"{col}_slope10"] = window[col].diff(10).fillna(0.0)

    window = window.fillna(0.0)

    unit_nr = str(pdf["unit_nr"].iloc[0])
    run_id = "unknown"
    if "run_id" in window.columns and window["run_id"].notna().any():
        run_id = str(window["run_id"].iloc[-1])
    w_start = int(window["time_cycles"].iloc[0])
    w_end = int(window["time_cycles"].iloc[-1])
    
    symptom_score, symptom_details = compute_symptom(window)
    trend_score = compute_trend_score(window)

    if np.isnan(symptom_score):
        symptom_score = 0.0
    if np.isnan(trend_score):
        trend_score = 0.0

    scaler = get_scaler()
    try:
        scaled = scaler.transform(window[FEATURE_COLS].values)
    except Exception:
        # Fallback if train_history is missing
        scaled = window[FEATURE_COLS].values
        
    # Clip ±5 like in training
    scaled = np.clip(scaled, -5.0, 5.0)
    scaled = np.nan_to_num(scaled, nan=0.0, posinf=0.0, neginf=0.0)
    # For XGBoost tabular inference, we only need the latest row of the sequence window
    X_latest = scaled[-1:]  # shape (1, num_features)

    model = get_model()
    if model:
        pred_val = float(model.predict(X_latest)[0])
        if np.isnan(pred_val):
            predicted_rul = float(RUL_CLIP)
        else:
            predicted_rul = max(0.0, pred_val)
        model_version = "mlflow-xgboost-prod"
    else:
        # Physics-informed heuristic: RUL drops faster as vibration grows
        cycle = float(pdf["time_cycles"].iloc[-1])
        rms_now = float(window["rms_x"].iloc[-1])
        # Decay: at normal vibration (~0.1g), lose ~1 file/measurement; at high vibration (>1g), lose 3x faster
        decay_factor = 1.0 + min(rms_now / 0.5, 2.0)
        if np.isnan(decay_factor):
            decay_factor = 1.0
        predicted_rul = max(0.0, RUL_CLIP - cycle * 0.08 * decay_factor)
        model_version = "heuristic"

    res = {
        "unit_nr": unit_nr,
        "run_id": run_id,
        "prediction_time": datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00', 'Z'),
        "window_start_cycle": w_start,
        "window_end_cycle": w_end,
        "predicted_rul": round(predicted_rul, 2),
        "symptom_score": round(symptom_score, 2),
        "trend_score": round(trend_score, 2),
        "symptom_details_json": json.dumps(symptom_details),
        "model_version": model_version
    }
    return pd.DataFrame([res])

prediction_schema = StructType([
    StructField("unit_nr", StringType()),
    StructField("run_id", StringType()),
    StructField("prediction_time", StringType()),
    StructField("window_start_cycle", LongType()),
    StructField("window_end_cycle", LongType()),
    StructField("predicted_rul", DoubleType()),
    StructField("symptom_score", DoubleType()),
    StructField("trend_score", DoubleType()),
    StructField("symptom_details_json", StringType()),
    StructField("model_version", StringType())
])

def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("StreamSilverGoldPHM")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

# Pipeline functions
def build_silver_stream(bronze_stream_df: DataFrame) -> DataFrame:
    typed = bronze_stream_df.withColumn("event_time_ts", F.to_timestamp("event_time"))
    feature_nulls = F.array_compact(F.array(*[
        F.when(F.col(c).isNull(), F.lit(f"missing_{c}")) for c in BASE_FEATURE_COLS
    ]))
    errs = F.array_compact(F.array(
        F.when(F.col("payload_parse_ok") != F.lit(True), F.lit("payload_parse_failed")),
        F.when(F.col("time_cycles") <= 0, F.lit("non_positive_cycle")),
        F.when(F.col("event_time_ts").isNull(), F.lit("invalid_event_time")),
        F.when(F.size(feature_nulls) > 0, F.lit("missing_features")),
    ))
    return typed.withColumn("validation_errors", errs).withColumn("is_valid", F.size("validation_errors") == 0)

def dedup_silver_cross_batch(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("unit_nr", "run_id", "time_cycles").orderBy(
        F.col("kafka_timestamp").desc(), F.col("kafka_offset").desc()
    )
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

def risk_level(rul, symptom_score, trend_score):
    """3-factor risk score. RUL is in actual cycles (1 cycle = 10s).
    Alert thresholds (tuned for FPT = 130 cycles limit):
      Critical : RUL <= 20   (< ~3.3 minutes remaining)
      Warning  : RUL <= 60   (< ~10 minutes remaining)
      Watch    : RUL <= 90   (< ~15 minutes remaining)
      Normal   : everything else
    """
    rul_pct = min(rul / RUL_CLIP * 100.0, 100.0)  # Reference point for score scaling
    risk = 0.5 * (100.0 - rul_pct) + 0.3 * symptom_score + 0.2 * trend_score
    if rul <= 20 or risk > 75: return "Critical"
    if rul <= 60 or risk > 55: return "Warning"
    if rul <= 90 or risk > 35: return "Watch"
    return "Normal"

def process_batch(batch_df: DataFrame, batch_id: int, spark: SparkSession):
    if not batch_df.take(1):
        return
    
    silver_path = os.environ.get("SILVER_STREAM_CLEAN_PATH")
    pred_current_path = os.environ.get("GOLD_PREDICTION_CURRENT_PATH")
    pred_history_path = os.environ.get("GOLD_PREDICTION_HISTORY_PATH", "s3a://lakehouse/gold/prediction_history_phm/")
    alert_current_path = os.environ.get("GOLD_ALERT_CURRENT_PATH")
    alert_history_path = os.environ.get("GOLD_ALERT_HISTORY_PATH", "s3a://lakehouse/gold/alert_history_phm/")
    pipeline_quality_path = os.environ.get("GOLD_PIPELINE_QUALITY_PATH", "s3a://lakehouse/gold/pipeline_quality_phm/")

    bronze_in_batch = batch_df.count()
    batch_df.write.format("delta").mode("append").option("mergeSchema", "true").save(silver_path)

    # Lọc danh sách máy đang có dữ liệu trong micro-batch này
    active_runs_df = batch_df.select("unit_nr", "run_id").distinct()
    active_units = [row.unit_nr for row in active_runs_df.select("unit_nr").distinct().collect() if row.unit_nr is not None]
    if not active_units:
        return

    run_id_row = batch_df.select("run_id").filter(F.col("run_id").isNotNull()).limit(1).collect()
    latest_run_id = str(run_id_row[0]["run_id"]) if run_id_row else "unknown"

    # Tối ưu scan: chỉ đọc dữ liệu cho các máy active và phạm vi time_cycles gần nhất
    unit_bounds = (
        batch_df.groupBy("unit_nr", "run_id")
        .agg(F.max("time_cycles").alias("max_cycle"))
        .withColumn("min_cycle", F.col("max_cycle") - F.lit(SEQUENCE_LENGTH * HISTORY_LOOKBACK_MULTIPLIER))
    )
    silver_candidate = (
        spark.read.format("delta").load(silver_path)
        .filter(F.col("unit_nr").isin(active_units))
        .join(F.broadcast(unit_bounds), on=["unit_nr", "run_id"], how="inner")
        .filter(F.col("time_cycles") >= F.col("min_cycle"))
        .drop("max_cycle", "min_cycle")
    )

    silver_deduped = dedup_silver_cross_batch(silver_candidate)
    silver_valid = silver_deduped.filter(F.col("is_valid") == F.lit(True))

    if pipeline_quality_path:
        write_pipeline_quality(silver_deduped, batch_id, pipeline_quality_path, bronze_in_batch, latest_run_id)

    # Chỉ lấy 30 time_cycles gần nhất cho từng máy để dự đoán
    w_top = Window.partitionBy("unit_nr").orderBy(F.col("time_cycles").desc())
    recent_silver = (
        silver_valid
        .withColumn("_seq", F.row_number().over(w_top))
        .filter(F.col("_seq") <= SEQUENCE_LENGTH)
        .drop("_seq")
    )
    
    # -------------------------------------------------------------
    # PANDAS UDF Execution
    # -------------------------------------------------------------
    preds_df = (
        recent_silver
        .select("unit_nr", "run_id", "time_cycles", *BASE_FEATURE_COLS)
        .groupBy("unit_nr")
        .applyInPandas(predict_rul_pandas_udf, schema=prediction_schema)
    )
    
    # Materialize predictions (triggers UDF execution)
    new_preds_pdf = preds_df.toPandas()
    if new_preds_pdf.empty:
        return

    # Append prediction_history (dedup by unit_nr + window_end_cycle)
    if delta_path_exists(spark, pred_history_path):
        existing_keys = (
            spark.read.format("delta")
            .load(pred_history_path)
            .filter(F.col("unit_nr").isin(active_units))
            .select("unit_nr", "run_id", "window_end_cycle")
            .distinct()
            .toPandas()
        )
        existing_set = set(
            zip(
                existing_keys["unit_nr"].astype(str),
                existing_keys["run_id"].astype(str),
                existing_keys["window_end_cycle"].astype(int),
            )
        )
        truly_new = new_preds_pdf[
            ~new_preds_pdf.apply(
                lambda r: (str(r["unit_nr"]), str(r["run_id"]), int(r["window_end_cycle"])) in existing_set,
                axis=1,
            )
        ]
    else:
        truly_new = new_preds_pdf

    if not truly_new.empty:
        spark.createDataFrame(truly_new).write.format("delta").mode("append").option("mergeSchema", "true").save(pred_history_path)

    # Build prediction_current using latest per unit
    try:
        existing_preds = spark.read.format("delta").load(pred_current_path).toPandas()
        combined_preds = pd.concat([existing_preds, new_preds_pdf], ignore_index=True)
        new_preds_pdf_full = combined_preds.sort_values("prediction_time", ascending=False).drop_duplicates(subset=["unit_nr"], keep="first")
    except Exception:
        new_preds_pdf_full = new_preds_pdf

    spark.createDataFrame(new_preds_pdf_full).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(pred_current_path)

    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    alert_rows = []
    alert_hist_rows = []
    for _, pr in new_preds_pdf.iterrows():
        ts = pr.get("trend_score", 0.0)
        lvl = risk_level(pr["predicted_rul"], pr["symptom_score"], float(ts))
        rul_pct = min(float(pr["predicted_rul"]) / RUL_CLIP * 100, 100.0)
        computed_risk = 0.5 * (100 - rul_pct) + 0.3 * float(pr["symptom_score"]) + 0.2 * float(ts)
        reason = {
            "raw_level": lvl,
            "confirmed_level": lvl,
            "risk_score": round(computed_risk, 2),
            "rul_score": float(pr["predicted_rul"]),
            "trend_score": float(ts),
            "symptom_score": float(pr["symptom_score"]),
        }
        alert_hist_rows.append({
            "unit_nr": str(pr["unit_nr"]),
            "run_id": str(pr.get("run_id", latest_run_id)),
            "alert_time": now_iso,
            "alert_level": lvl,
            "risk_score": round(computed_risk, 2),
            "rul_score": float(pr["predicted_rul"]),
            "trend_score": float(ts),
            "symptom_score": float(pr["symptom_score"]),
            "reason_json": json.dumps(reason),
        })
        alert_rows.append({
            "unit_nr": str(pr["unit_nr"]),
            "run_id": str(pr.get("run_id", latest_run_id)),
            "alert_level": lvl,
            "risk_score": round(computed_risk, 2),
            "rul_score": float(pr["predicted_rul"]),
            "trend_score": float(ts),
            "symptom_score": float(pr["symptom_score"]),
            "pending_count": 0,
            "reason_json": json.dumps(reason),
            "updated_at": now_iso
        })
        
    if alert_hist_rows:
        spark.createDataFrame(pd.DataFrame(alert_hist_rows)).write.format("delta").mode("append").option("mergeSchema", "true").save(alert_history_path)

    if alert_rows:
        new_alerts_pdf = pd.DataFrame(alert_rows)
        try:
            existing_alerts = spark.read.format("delta").load(alert_current_path).toPandas()
            combined_alerts = pd.concat([existing_alerts, new_alerts_pdf], ignore_index=True)
            new_alerts_pdf_full = combined_alerts.sort_values("updated_at", ascending=False).drop_duplicates(subset=["unit_nr"], keep="first")
        except Exception:
            new_alerts_pdf_full = new_alerts_pdf
            
        spark.createDataFrame(new_alerts_pdf_full).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(alert_current_path)
        
    LOGGER.info(
        f"Batch {batch_id}: processed {len(new_preds_pdf)} new predictions. Total machines in dashboard: {len(new_preds_pdf_full)}."
    )

def main():
    spark = create_spark_session()
    bronze_raw = os.environ.get("BRONZE_RAW_DELTA_PATH")
    bronze_stream = spark.readStream.format("delta").load(bronze_raw)
    silver_stream = build_silver_stream(bronze_stream)

    query = (
        silver_stream.writeStream.outputMode("append")
        .option("checkpointLocation", os.environ.get("SILVER_STREAM_CHECKPOINT_PATH"))
        .trigger(processingTime=os.environ.get("SILVER_STREAM_TRIGGER_INTERVAL"))
        .foreachBatch(lambda df, bid: process_batch(df, bid, spark))
        .start()
    )
    query.awaitTermination()

if __name__ == "__main__":
    main()
