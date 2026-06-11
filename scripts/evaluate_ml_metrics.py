"""Offline ML evaluation metrics for thesis report (RMSE / MAE)."""
from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT / "scripts"))

from train_phm_model import (  # noqa: E402
    LEARNING_BEARINGS,
    RUL_CLIP,
    extract_bearing_data,
)


def _metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    err = y_pred - y_true
    return {
        "mae": float(np.mean(np.abs(err))),
        "rmse": float(np.sqrt(np.mean(err**2))),
        "n": int(len(y_true)),
    }


def evaluate_phm() -> dict[str, object]:
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import RobustScaler
    import xgboost as xgb

    frames = [extract_bearing_data(b) for b in LEARNING_BEARINGS]
    df = pd.concat(frames, ignore_index=True)

    feature_cols = [
        "time_norm",
        "rms_x", "rms_y", "kurt_x", "kurt_y", "p2p_x", "p2p_y", "crest_x", "crest_y", "skew_x", "skew_y",
        "spec_entropy_x", "spec_entropy_y", "band_energy_x", "band_energy_y", "peak_freq_x", "peak_freq_y",
        "fft_x_1", "fft_x_2", "fft_x_3", "fft_x_4", "fft_x_5",
        "fft_y_1", "fft_y_2", "fft_y_3", "fft_y_4", "fft_y_5",
        "rms_x_z", "rms_y_z", "kurt_x_z", "kurt_y_z", "crest_x_z", "crest_y_z",
        "rms_x_slope10", "kurt_x_slope10", "crest_x_slope10", "band_energy_x_slope10",
    ]

    scaler = RobustScaler()
    df[feature_cols] = scaler.fit_transform(df[feature_cols]).clip(-5, 5)
    X = df[feature_cols].values
    y = df["RUL"].values
    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.15, random_state=42
    )

    model = xgb.XGBRegressor(
        objective="reg:squarederror",
        max_depth=6,
        learning_rate=0.05,
        n_estimators=500,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
    )
    model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)
    pred = model.predict(X_val)
    m = _metrics(y_val, pred)
    return {
        "dataset": "PHM IEEE 2012",
        "model": "XGBoost",
        "split": "85% train / 15% validation (random_state=42)",
        "unit": "chu kỳ (10 s/chu kỳ)",
        **m,
    }


def evaluate_nasa() -> dict[str, object] | None:
    """Evaluate LSTM on holdout streaming units if model + train CSV exist."""
    try:
        import tensorflow as tf  # noqa: F401
        from tensorflow import keras
    except ImportError:
        return None

    model_path = ROOT / "NASA-Turbofan-Predictive-Modeling" / "model_GOLD_MINIO.keras"
    train_csv = ROOT / "Data" / "train_history.csv"
    test_csv = ROOT / "Data" / "raw_streaming.csv"
    if not model_path.exists() or not train_csv.exists() or not test_csv.exists():
        return None

    sys.path.insert(0, str(ROOT / "NASA-Turbofan-Predictive-Modeling"))
    from src import config  # noqa: E402
    from src.data_utils import CMAPSSData  # noqa: E402

    engine = CMAPSSData()
    train_df = pd.read_csv(train_csv).sort_values(["unit_nr", "time_cycles"])
    test_df = pd.read_csv(test_csv).sort_values(["unit_nr", "time_cycles"])

    train_df = train_df.drop(columns=config.DROP_COLS, errors="ignore")
    test_df = test_df.drop(columns=config.DROP_COLS, errors="ignore")
    feature_cols = [
        c
        for c in train_df.columns
        if c not in ["unit_nr", "time_cycles", "RUL", "event_time"]
    ]

    for col in feature_cols:
        train_df[col] = train_df[col].ewm(alpha=config.SMOOTHING_ALPHA).mean()
    engine.scaler.fit(train_df[feature_cols].values)

    model = keras.models.load_model(model_path, compile=False)
    y_true: list[float] = []
    y_pred: list[float] = []

    for unit_nr, grp in test_df.groupby("unit_nr"):
        grp = grp.sort_values("time_cycles").reset_index(drop=True)
        max_cycle = int(grp["time_cycles"].max())
        feats = grp[feature_cols].copy()
        for col in feature_cols:
            feats[col] = feats[col].ewm(alpha=config.SMOOTHING_ALPHA).mean()
        scaled = engine.scaler.transform(feats.values)
        scaled = np.nan_to_num(scaled, nan=0.0, posinf=1.0, neginf=0.0)
        if len(scaled) < config.SEQUENCE_LENGTH:
            continue
        for stop in range(config.SEQUENCE_LENGTH, len(scaled) + 1):
            window = scaled[stop - config.SEQUENCE_LENGTH : stop]
            X = window.reshape(1, config.SEQUENCE_LENGTH, len(feature_cols))
            pred = float(model.predict(X, verbose=0).flatten()[0])
            pred = max(0.0, min(pred, config.RUL_CLIP))
            cycle = int(grp.loc[stop - 1, "time_cycles"])
            true_rul = min(max_cycle - cycle, config.RUL_CLIP)
            y_true.append(true_rul)
            y_pred.append(pred)

    if not y_true:
        return None

    m = _metrics(np.array(y_true), np.array(y_pred))
    return {
        "dataset": "NASA CMAPSS FD001",
        "model": "LSTM (Keras)",
        "split": "70 máy train / 30 máy holdout (raw_streaming.csv)",
        "unit": "chu kỳ bay",
        **m,
    }


def main() -> None:
    phm = evaluate_phm()
    nasa = evaluate_nasa()

    print("PHM:", phm)
    print("NASA:", nasa)


if __name__ == "__main__":
    main()
