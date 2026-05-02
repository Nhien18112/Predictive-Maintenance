import os
import sys
import subprocess
import glob
import time

try:
    import xgboost as xgb
    import mlflow
    import mlflow.xgboost
    from sklearn.preprocessing import RobustScaler
    from sklearn.model_selection import train_test_split
except ImportError:
    print("Installing required dependencies: xgboost, scikit-learn, mlflow...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "xgboost", "scikit-learn", "mlflow", "requests", "pandas", "numpy", "scipy"])
    import xgboost as xgb
    import mlflow
    import mlflow.xgboost
    from sklearn.preprocessing import RobustScaler
    from sklearn.model_selection import train_test_split

import pandas as pd
import numpy as np
import scipy.stats
import json
import requests


sys.stdout.reconfigure(encoding='utf-8')

# Configurations
LEARNING_SET_DIR = "Data/phm-ieee-2012-data-challenge-dataset-master/Learning_set"
FULL_TEST_SET_DIR = "Data/phm-ieee-2012-data-challenge-dataset-master/Full_Test_Set"

# Train bearings: Learning_set only (100% of Learning_set)
# Full_Test_Set is reserved for replay/demo
LEARNING_BEARINGS = ["Bearing1_1", "Bearing1_2", "Bearing2_1", "Bearing2_2", "Bearing3_1", "Bearing3_2"]
TRAIN_BEARINGS = LEARNING_BEARINGS

CSV_COLS = ["Hour", "Minute", "Second", "Microsecond", "Acc_x", "Acc_y"]
RUL_CLIP = 130.0             # Piecewise Linear RUL: Flat 130 max, then drops linearly
ASSUMED_LIFE = 1428.0        # Actual median lifecycle from 15 training bearings
MLFLOW_URI = "http://localhost:5000"
EXPERIMENT_NAME = "PHM_IEEE_2012_RUL"

def check_mlflow_health(timeout=5, retries=3):
    """Kiểm tra xem MLflow server có sẵn sàng không"""
    for attempt in range(retries):
        try:
            response = requests.get(f"{MLFLOW_URI}/health", timeout=timeout)
            if response.status_code == 200:
                print(f"✓ MLflow server ready at {MLFLOW_URI}")
                return True
        except Exception as e:
            if attempt < retries - 1:
                print(f"  MLflow not ready (attempt {attempt+1}/{retries}), retrying in 2s...")
                time.sleep(2)
            else:
                print(f"✗ MLflow unavailable after {retries} attempts. Using local artifact storage.")
                return False
    return False

def calculate_features(df):
    x = df["Acc_x"].values
    y = df["Acc_y"].values
    
    # RMS
    rms_x = np.sqrt(np.mean(x**2))
    rms_y = np.sqrt(np.mean(y**2))
    
    # Kurtosis
    kurt_x = scipy.stats.kurtosis(x)
    kurt_y = scipy.stats.kurtosis(y)
    
    # Peak-to-Peak
    p2p_x = np.ptp(x)
    p2p_y = np.ptp(y)
    
    # Crest Factor
    crest_x = np.max(np.abs(x)) / rms_x if rms_x > 0 else 0
    crest_y = np.max(np.abs(y)) / rms_y if rms_y > 0 else 0
    
    # Skewness
    skew_x = scipy.stats.skew(x)
    skew_y = scipy.stats.skew(y)
    
    # FFT Features
    fft_x = np.abs(np.fft.rfft(x))
    fft_y = np.abs(np.fft.rfft(y))
    fft_x_amps = sorted([float(v) for v in fft_x], reverse=True)
    fft_y_amps = sorted([float(v) for v in fft_y], reverse=True)
    while len(fft_x_amps) < 5: fft_x_amps.append(0.0)
    while len(fft_y_amps) < 5: fft_y_amps.append(0.0)

    def _spectral_entropy(fft_vals):
        power = np.square(fft_vals)
        total = np.sum(power)
        if total == 0:
            return 0.0
        p = power / total
        p = p[p > 0]
        return float(-np.sum(p * np.log(p)))

    def _band_energy(fft_vals, ratio=0.1):
        n = len(fft_vals)
        if n == 0:
            return 0.0
        k = max(1, int(n * ratio))
        return float(np.sum(np.square(fft_vals[1:k])))

    def _peak_freq(fft_vals):
        if len(fft_vals) <= 1:
            return 0.0
        peak_idx = int(np.argmax(fft_vals[1:]) + 1)
        return float(peak_idx)

    def _safe(v):
        """Return 0.0 if value is NaN or Inf."""
        try:
            f = float(v)
            return 0.0 if (np.isnan(f) or np.isinf(f)) else f
        except Exception:
            return 0.0

    res = {
        "rms_x": _safe(rms_x), "rms_y": _safe(rms_y),
        "kurt_x": _safe(kurt_x), "kurt_y": _safe(kurt_y),
        "p2p_x": _safe(p2p_x), "p2p_y": _safe(p2p_y),
        "crest_x": _safe(crest_x), "crest_y": _safe(crest_y),
        "skew_x": _safe(skew_x), "skew_y": _safe(skew_y),
        "spec_entropy_x": _safe(_spectral_entropy(fft_x)),
        "spec_entropy_y": _safe(_spectral_entropy(fft_y)),
        "band_energy_x": _safe(_band_energy(fft_x)),
        "band_energy_y": _safe(_band_energy(fft_y)),
        "peak_freq_x": _safe(_peak_freq(fft_x)),
        "peak_freq_y": _safe(_peak_freq(fft_y)),
    }
    
    for i in range(5):
        res[f"fft_x_{i+1}"] = fft_x_amps[i]
        res[f"fft_y_{i+1}"] = fft_y_amps[i]
        
    return res

def extract_bearing_data(bearing_name):
    folder_path = os.path.join(LEARNING_SET_DIR, bearing_name)
    files = sorted(glob.glob(os.path.join(folder_path, "acc_*.csv")))
    
    data = []
    print(f"Extracting {len(files)} files for {bearing_name}...")
    for idx, file_path in enumerate(files):
        df = pd.read_csv(file_path, header=None, names=CSV_COLS, sep=r';|,', engine='python')
        features = calculate_features(df)
        features["unit_nr"] = bearing_name
        features["time_cycles"] = idx + 1
        data.append(features)
        
    df_features = pd.DataFrame(data)

    # Normalized elapsed time
    df_features['time_norm'] = df_features['time_cycles'] / ASSUMED_LIFE

    # Relative features: z-score from each bearing's own early-life baseline
    n_base = min(50, max(10, len(df_features) // 10))
    for col in ["rms_x", "rms_y", "kurt_x", "kurt_y", "crest_x", "crest_y"]:
        mu = df_features[col].head(n_base).mean()
        sigma = df_features[col].head(n_base).std() + 1e-8
        df_features[f"{col}_z"] = (df_features[col] - mu) / sigma

    # --- Slope features: rate of change over last 10 cycles ---
    for col in ["rms_x", "kurt_x", "crest_x", "band_energy_x"]:
        df_features[f"{col}_slope10"] = df_features[col].diff(10).fillna(0)

    # RUL is total chunks minus current chunk
    max_cycle = df_features["time_cycles"].max()
    true_rul = max_cycle - df_features["time_cycles"]
    
    # Piecewise Linear RUL logic
    # Set to RUL_CLIP for healthy state, then decrease linearly
    df_features["RUL"] = np.clip(true_rul, 0, RUL_CLIP)
    df_features["true_RUL"] = true_rul
    df_features["max_cycle"] = max_cycle
    return df_features

def main():
    print("="*70)
    print("Starting PHM IEEE 2012 Model Training Pipeline (XGBoost)...")
    print("="*70)
    
    # 1. Feature Extraction
    print("\n PHASE 1: Feature Extraction")
    print("-" * 70)
    all_data = []

    for bearing in LEARNING_BEARINGS:
        path = os.path.join(LEARNING_SET_DIR, bearing)
        if os.path.exists(path):
            df_b = extract_bearing_data(bearing)
            all_data.append(df_b)
        else:
            print(f"Warning: {bearing} not found in {LEARNING_SET_DIR}")

    if not all_data:
        raise ValueError("No training data found.")

    df_train_raw = pd.concat(all_data, ignore_index=True)

    # Save the training history so the streaming pipeline can load baselines!
    os.makedirs("Data", exist_ok=True)
    df_train_raw.to_csv("Data/train_history_phm.csv", index=False)
    print(" Saved extracted features to Data/train_history_phm.csv (Learning_set only)")
    print(f"  Train bearings: {len(TRAIN_BEARINGS)}")
    print(f"  Train samples: {df_train_raw.shape[0]}, Features: {df_train_raw.shape[1]}")
    
    # 2. Preprocessing
    print("\n PHASE 2: Data Preprocessing & Normalization")
    print("-" * 70)
    feature_cols = [
        "time_norm",
        "rms_x", "rms_y",
        "kurt_x", "kurt_y",
        "p2p_x", "p2p_y",
        "crest_x", "crest_y",
        "skew_x", "skew_y",
        "spec_entropy_x", "spec_entropy_y",
        "band_energy_x", "band_energy_y",
        "peak_freq_x", "peak_freq_y",
        "fft_x_1", "fft_x_2", "fft_x_3", "fft_x_4", "fft_x_5",
        "fft_y_1", "fft_y_2", "fft_y_3", "fft_y_4", "fft_y_5",
        "rms_x_z", "rms_y_z",
        "kurt_x_z", "kurt_y_z",
        "crest_x_z", "crest_y_z",
        "rms_x_slope10", "kurt_x_slope10",
        "crest_x_slope10", "band_energy_x_slope10",
    ]
    
    df_train = df_train_raw.copy()

    scaler = RobustScaler()
    df_train[feature_cols] = scaler.fit_transform(df_train[feature_cols])
    df_train[feature_cols] = df_train[feature_cols].clip(-5, 5)

    # Remove NaN / Inf
    n_nan_train = df_train[feature_cols].isnull().any(axis=1).sum()
    df_train[feature_cols] = (
        df_train[feature_cols].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    )
    if n_nan_train > 0:
        print(f"  ⚠ Removed NaN/Inf rows: {n_nan_train} train (filled with 0)")
    print(f"✓ Normalized {len(feature_cols)} features using RobustScaler (clipped ±5, NaN→0)")
    print(f"✓ Piecewise-linear RUL: targets clipped at RUL_CLIP={RUL_CLIP}")
    
    # 3. Model Definition and Training (XGBoost)
    print("\n PHASE 3: Model Training (XGBoost)")
    print("-" * 70)
    
    X = df_train[feature_cols].values
    y = df_train["RUL"].values

    # We randomly hold out 15% purely for XGBoost early stopping to prevent over-fitting.
    # The final evaluation will be done via Spark/Dashboard on the Full_Test_Set.
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.15, random_state=42)

    print(f" Training set: {X_train.shape[0]} samples")
    print(f" Validation set: {X_val.shape[0]} samples (for early stopping)")
    
    xgb_params = {
        "objective": "reg:squarederror",
        "eval_metric": ["rmse", "mae"],
        "max_depth": 6,
        "learning_rate": 0.05,
        "n_estimators": 500,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "random_state": 42
    }

    model = xgb.XGBRegressor(**xgb_params)
    
    # Check MLflow availability
    mlflow_available = check_mlflow_health()
    
    if mlflow_available:
        mlflow.set_tracking_uri(MLFLOW_URI)
        mlflow.set_experiment(EXPERIMENT_NAME)
        mlflow_enabled = True
    else:
        mlflow_enabled = False
        print(" Training will proceed without MLflow tracking (local mode)")
    
    try:
        if mlflow_enabled:
            # We use autologging for XGBoost
            mlflow.xgboost.autolog()
            with mlflow.start_run() as run:
                mlflow.log_params({
                    "rul_clip": RUL_CLIP,
                    "model_type": "XGBoost",
                    "feature_count": len(feature_cols)
                })
                
                print(" Training XGBoost Model...")
                model.fit(
                    X_train, y_train,
                    eval_set=[(X_train, y_train), (X_val, y_val)],
                    verbose=50
                )
                
                # Log metrics for the final validation set manually as well
                y_val_pred = model.predict(X_val)
                errors = y_val_pred - y_val
                val_mae = float(np.mean(np.abs(errors)))
                val_rmse = float(np.sqrt(np.mean(errors ** 2)))
                
                mlflow.log_metrics({
                    "val_mae_cycles": val_mae,
                    "val_rmse_cycles": val_rmse
                })
                
                # Register model
                client = mlflow.tracking.MlflowClient()
                model_name = "PHM_XGBoost_Model"
                
                model_info = mlflow.xgboost.log_model(
                    model, "phm_xgboost_model",
                    registered_model_name=model_name
                )
                
                versions = client.search_model_versions(f"name='{model_name}'")
                latest_version = max([int(v.version) for v in versions])
                client.set_registered_model_alias(model_name, "production", str(latest_version))
                
                print(f"\n✓ Model registered to MLflow as {model_name} (version {latest_version}) with 'production' alias")
        else:
            print(" Training XGBoost Model...")
            model.fit(
                X_train, y_train,
                eval_set=[(X_train, y_train), (X_val, y_val)],
                verbose=50
            )
            y_val_pred = model.predict(X_val)
            errors = y_val_pred - y_val
            val_mae = float(np.mean(np.abs(errors)))
            val_rmse = float(np.sqrt(np.mean(errors ** 2)))
            
    except Exception as e:
        print(f"\n Training error: {e}")
        raise

    print("\n" + "="*70)
    print(" TRAINING COMPLETED!")
    print("="*70)
    print(" FINAL METRICS SUMMARY:")
    print("-" * 70)
    print(f"Validation MAE:     {val_mae:.4f} cycles")
    print(f"Validation RMSE:    {val_rmse:.4f} cycles")
    print("="*70)

if __name__ == "__main__":
    main()

