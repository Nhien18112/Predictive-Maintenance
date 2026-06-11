# train.py
from __future__ import annotations

import argparse
import json
import os
import sys

from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint
from tensorflow.keras.models import load_model

NASA_ROOT = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(NASA_ROOT, ".."))
if NASA_ROOT not in sys.path:
    sys.path.insert(0, NASA_ROOT)

from src import config  # noqa: E402
from src.data_utils import CMAPSSData  # noqa: E402
from src.metrics import mae_rmse  # noqa: E402
from src.model import create_lstm_model  # noqa: E402


def _repo_root() -> str:
    return REPO_ROOT


def _resolve_path(path: str) -> str:
    if os.path.isabs(path):
        return path
    return os.path.join(_repo_root(), path)


def _metrics_only() -> dict:
    os.chdir(_repo_root())
    model_path = _resolve_path(config.MODEL_PATH)
    metrics_path = _resolve_path(config.METRICS_PATH)
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model not found: {model_path}. Run training first.")

    data = CMAPSSData()
    arrays = data.prepare_datasets()
    eval_model = load_model(model_path)
    val_pred = eval_model.predict(arrays["X_val"], verbose=0).reshape(-1)
    holdout_pred = eval_model.predict(arrays["X_holdout"], verbose=0).reshape(-1)
    metrics = {
        "dataset": config.DATASET_LABEL,
        "model_path": model_path,
        "validation": mae_rmse(arrays["y_val"], val_pred),
        "holdout": mae_rmse(arrays["y_holdout"], holdout_pred),
        "split": arrays["split_meta"],
        "hyperparameters": {"monitor": "val_loss", "metrics_only": True},
    }
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)
    print(json.dumps(metrics, indent=2))
    return metrics


def train_and_evaluate(force: bool = False) -> dict:
    os.chdir(_repo_root())
    model_path = _resolve_path(config.MODEL_PATH)
    metrics_path = _resolve_path(config.METRICS_PATH)
    os.makedirs(os.path.dirname(model_path), exist_ok=True)

    if os.path.exists(model_path) and not force:
        if os.path.exists(metrics_path):
            with open(metrics_path, encoding="utf-8") as f:
                return json.load(f)
        print(f"Model exists at {model_path}; use --force to retrain.", file=sys.stderr)
        sys.exit(0)

    data = CMAPSSData()
    arrays = data.prepare_datasets()
    X_train, y_train = arrays["X_train"], arrays["y_train"]
    X_val, y_val = arrays["X_val"], arrays["y_val"]
    X_holdout, y_holdout = arrays["X_holdout"], arrays["y_holdout"]

    if len(X_train) == 0 or len(X_val) == 0:
        raise RuntimeError("Train or validation set has no sequences; check data split.")

    input_shape = (config.SEQUENCE_LENGTH, len(arrays["feature_cols"]))
    model = create_lstm_model(input_shape)

    callbacks = [
        EarlyStopping(
            monitor="val_loss",
            patience=config.EARLY_STOPPING_PATIENCE,
            restore_best_weights=True,
            verbose=1,
        ),
        ModelCheckpoint(
            model_path,
            monitor="val_loss",
            save_best_only=True,
            verbose=1,
        ),
    ]

    print(
        f"Training LSTM: train={len(X_train)} val={len(X_val)} holdout={len(X_holdout)} "
        f"units={arrays['split_meta']}"
    )
    model.fit(
        X_train,
        y_train,
        validation_data=(X_val, y_val),
        epochs=config.EPOCHS,
        batch_size=config.BATCH_SIZE,
        callbacks=callbacks,
        verbose=1,
    )

    if len(X_holdout) == 0:
        raise RuntimeError("Holdout set has no sequences; check FD001 data and raw_streaming.csv.")

    eval_model = load_model(model_path)
    val_pred = eval_model.predict(X_val, verbose=0).reshape(-1)
    holdout_pred = eval_model.predict(X_holdout, verbose=0).reshape(-1)

    metrics = {
        "dataset": config.DATASET_LABEL,
        "model_path": model_path,
        "validation": mae_rmse(y_val, val_pred),
        "holdout": mae_rmse(y_holdout, holdout_pred),
        "split": arrays["split_meta"],
        "hyperparameters": {
            "epochs": config.EPOCHS,
            "batch_size": config.BATCH_SIZE,
            "sequence_length": config.SEQUENCE_LENGTH,
            "rul_clip": config.RUL_CLIP,
            "val_unit_ratio": config.VAL_UNIT_RATIO,
            "early_stopping_patience": config.EARLY_STOPPING_PATIENCE,
            "monitor": "val_loss",
        },
    }

    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    print(
        f"Validation  MAE={metrics['validation']['mae']:.2f} "
        f"RMSE={metrics['validation']['rmse']:.2f} (n={metrics['validation']['n']})"
    )
    print(
        f"Holdout     MAE={metrics['holdout']['mae']:.2f} "
        f"RMSE={metrics['holdout']['rmse']:.2f} (n={metrics['holdout']['n']})"
    )
    print(f"Metrics saved to {metrics_path}")
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Train NASA FD001 LSTM with unit-level validation."
    )
    parser.add_argument("--force", action="store_true", help="Retrain even if model exists.")
    parser.add_argument(
        "--metrics-only",
        action="store_true",
        help="Skip training; recompute validation/holdout metrics from saved model.",
    )
    args = parser.parse_args()
    if args.metrics_only:
        _metrics_only()
        return
    train_and_evaluate(force=args.force)


if __name__ == "__main__":
    main()
