# src/config.py
import os

# Data source: minio_gold | csv
DATA_SOURCE = os.getenv("NASA_DATA_SOURCE", "csv")

MINIO_GOLD_DELTA_PATH = os.getenv(
    "MINIO_GOLD_DELTA_PATH", "s3://lakehouse/gold/train_rul/"
)
MINIO_ENDPOINT_URL = os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")
MINIO_ALLOW_HTTP = os.getenv("MINIO_ALLOW_HTTP", "true").lower() in ("1", "true", "yes")

TRAIN_CSV_PATH = os.getenv("TRAIN_CSV_PATH", "Data/train_history.csv")
HOLDOUT_STREAM_CSV = os.getenv("HOLDOUT_STREAM_CSV", "Data/raw_streaming.csv")
FD001_FALLBACK_TXT = os.getenv("FD001_FALLBACK_TXT", "Data/train_FD001.txt")

DROP_COLS = ["setting_3", "s_1", "s_5", "s_10", "s_16", "s_18", "s_19"]

RUL_CLIP = 125
SMOOTHING_ALPHA = 0.1
SEQUENCE_LENGTH = 25

EPOCHS = int(os.getenv("NASA_EPOCHS", "30"))
BATCH_SIZE = int(os.getenv("NASA_BATCH_SIZE", "32"))
VAL_UNIT_RATIO = float(os.getenv("NASA_VAL_UNIT_RATIO", "0.15"))
SPLIT_SEED = int(os.getenv("NASA_SPLIT_SEED", "42"))
EARLY_STOPPING_PATIENCE = int(os.getenv("NASA_EARLY_STOPPING_PATIENCE", "5"))

DATASET_LABEL = os.getenv("NASA_DATASET_LABEL", "GOLD_MINIO")
MODEL_PATH = os.getenv(
    "NASA_MODEL_PATH",
    f"NASA-Turbofan-Predictive-Modeling/model_{DATASET_LABEL}.keras",
)
METRICS_PATH = os.getenv(
    "NASA_METRICS_PATH",
    f"NASA-Turbofan-Predictive-Modeling/metrics_{DATASET_LABEL}.json",
)
