"""Evaluate NASA LSTM holdout metrics using the shared training pipeline."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NASA_ROOT = ROOT / "NASA-Turbofan-Predictive-Modeling"
os.chdir(ROOT)
sys.path.insert(0, str(NASA_ROOT))

from train import train_and_evaluate  # noqa: E402


def main() -> None:
    metrics = train_and_evaluate(force="--force" in sys.argv)
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
