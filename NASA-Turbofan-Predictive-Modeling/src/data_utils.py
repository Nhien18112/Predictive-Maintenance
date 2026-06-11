# src/data_utils.py
from __future__ import annotations

import os
from typing import Any

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

from src import config


class CMAPSSData:
  def __init__(self):
    self.id_col = "unit_nr"
    self.cycle_col = "time_cycles"
    self.rul_col = "RUL"
    self.feature_cols: list[str] = []
    self.scaler = MinMaxScaler()

  def load_dataframe(self) -> pd.DataFrame:
    if config.DATA_SOURCE == "minio_gold":
      df = self._load_from_minio_gold()
    else:
      df = self._load_from_csv_or_txt()
    return self._ensure_rul(df)

  def get_holdout_units(self) -> set[int]:
    path = config.HOLDOUT_STREAM_CSV
    if not os.path.exists(path):
      return set()
    return set(pd.read_csv(path)[self.id_col].astype(int).unique())

  def split_by_units(
    self, df: pd.DataFrame, holdout_units: set[int]
  ) -> dict[str, pd.DataFrame]:
    holdout_df = df[df[self.id_col].isin(holdout_units)].copy()
    train_pool = df[~df[self.id_col].isin(holdout_units)].copy()
    pool_units = sorted(train_pool[self.id_col].unique())
    rng = np.random.RandomState(config.SPLIT_SEED)
    rng.shuffle(pool_units)
    n_val = max(1, int(len(pool_units) * config.VAL_UNIT_RATIO))
    val_units = set(pool_units[:n_val])
    train_units = set(pool_units[n_val:])
    return {
      "train": train_pool[train_pool[self.id_col].isin(train_units)].copy(),
      "val": train_pool[train_pool[self.id_col].isin(val_units)].copy(),
      "holdout": holdout_df,
      "train_units": sorted(train_units),
      "val_units": sorted(val_units),
      "holdout_units": sorted(holdout_units),
    }

  def prepare_datasets(self) -> dict[str, Any]:
    df = self.load_dataframe()
    holdout_units = self.get_holdout_units()
    if not holdout_units:
      raise FileNotFoundError(
        f"Holdout unit list not found at {config.HOLDOUT_STREAM_CSV}. "
        "Run split-train-stream first."
      )
    splits = self.split_by_units(df, holdout_units)
    self._fit_scaler(splits["train"])
    arrays = {
      "X_train": self._sequences_from_df(splits["train"])[0],
      "y_train": self._sequences_from_df(splits["train"])[1],
      "X_val": self._sequences_from_df(splits["val"])[0],
      "y_val": self._sequences_from_df(splits["val"])[1],
      "X_holdout": self._sequences_from_df(splits["holdout"])[0],
      "y_holdout": self._sequences_from_df(splits["holdout"])[1],
      "feature_cols": self.feature_cols,
      "scaler": self.scaler,
      "split_meta": {
        "train_units": [int(u) for u in splits["train_units"]],
        "val_units": [int(u) for u in splits["val_units"]],
        "holdout_units": [int(u) for u in splits["holdout_units"]],
        "n_train": int(len(splits["train_units"])),
        "n_val": int(len(splits["val_units"])),
        "n_holdout": int(len(splits["holdout_units"])),
      },
    }
    return arrays

  def _load_from_csv_or_txt(self) -> pd.DataFrame:
    # Full FD001 train file is required so holdout unit_nr (30 máy) có nhãn RUL.
    if os.path.exists(config.FD001_FALLBACK_TXT):
      return self._load_fd001_txt(config.FD001_FALLBACK_TXT)
    if os.path.exists(config.TRAIN_CSV_PATH):
      train_df = pd.read_csv(config.TRAIN_CSV_PATH)
      holdout_units = self.get_holdout_units()
      if holdout_units and not set(train_df[self.id_col]).intersection(holdout_units):
        raise FileNotFoundError(
          f"{config.TRAIN_CSV_PATH} lacks holdout units; place {config.FD001_FALLBACK_TXT}"
        )
      return train_df
    raise FileNotFoundError(
      f"No training data at {config.FD001_FALLBACK_TXT} or {config.TRAIN_CSV_PATH}"
    )

  def _load_fd001_txt(self, path: str) -> pd.DataFrame:
    cols = (
      [self.id_col, self.cycle_col]
      + [f"setting_{i}" for i in range(1, 4)]
      + [f"s_{i}" for i in range(1, 22)]
    )
    df = pd.read_csv(path, sep=r"\s+", header=None, names=cols)
    return df

  def _load_from_minio_gold(self) -> pd.DataFrame:
    from deltalake import DeltaTable

    storage_options = {
      "AWS_ACCESS_KEY_ID": config.MINIO_ACCESS_KEY,
      "AWS_SECRET_ACCESS_KEY": config.MINIO_SECRET_KEY,
      "AWS_REGION": config.MINIO_REGION,
      "AWS_ENDPOINT_URL": config.MINIO_ENDPOINT_URL,
      "AWS_ALLOW_HTTP": str(config.MINIO_ALLOW_HTTP).lower(),
    }
    dt = DeltaTable(config.MINIO_GOLD_DELTA_PATH, storage_options=storage_options)
    return dt.to_pandas()

  def _ensure_rul(self, df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if self.rul_col not in out.columns:
      max_cycle = out.groupby(self.id_col)[self.cycle_col].transform("max")
      out[self.rul_col] = (max_cycle - out[self.cycle_col]).clip(0, config.RUL_CLIP)
    else:
      out[self.rul_col] = out[self.rul_col].clip(0, config.RUL_CLIP)
    return out

  def _fit_scaler(self, train_df: pd.DataFrame) -> None:
    df = train_df.copy()
    self.feature_cols = [c for c in df.columns if c not in config.DROP_COLS + [self.rul_col]]
    df = self._smooth_features(df)
    self.scaler.fit(df[self.feature_cols])

  def _smooth_features(self, df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in self.feature_cols:
      out[col] = out.groupby(self.id_col)[col].transform(
        lambda s: s.ewm(alpha=config.SMOOTHING_ALPHA).mean()
      )
    return out

  def _transform_df(self, df: pd.DataFrame) -> pd.DataFrame:
    out = self._smooth_features(df.copy())
    out[self.feature_cols] = self.scaler.transform(out[self.feature_cols])
    return out

  def _sequences_from_df(self, df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    if df.empty:
      return np.empty((0, config.SEQUENCE_LENGTH, len(self.feature_cols))), np.empty((0,))
    transformed = self._transform_df(df)
    xs: list[np.ndarray] = []
    ys: list[float] = []
    for _, unit_df in transformed.groupby(self.id_col):
      unit_df = unit_df.sort_values(self.cycle_col)
      features = unit_df[self.feature_cols].values
      targets = unit_df[self.rul_col].values
      for i in range(len(unit_df) - config.SEQUENCE_LENGTH):
        xs.append(features[i : i + config.SEQUENCE_LENGTH])
        ys.append(targets[i + config.SEQUENCE_LENGTH])
    if not xs:
      return np.empty((0, config.SEQUENCE_LENGTH, len(self.feature_cols))), np.empty((0,))
    return np.array(xs, dtype=np.float32), np.array(ys, dtype=np.float32)
