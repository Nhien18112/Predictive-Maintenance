"""Train NASA FD001 LSTM with unit-level validation and holdout evaluation."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NASA_ROOT = ROOT / "NASA-Turbofan-Predictive-Modeling"
os.chdir(ROOT)
sys.path.insert(0, str(NASA_ROOT))

from train import main  # noqa: E402

if __name__ == "__main__":
    main()
