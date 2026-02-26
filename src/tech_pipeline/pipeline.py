from __future__ import annotations

import datetime as dt
from pathlib import Path

from .config import PipelineConfig
from .pm import PM


def run(mode: str = "full", tickers: list[str] | None = None) -> Path:
    run_dir = Path("runs") / dt.date.today().isoformat()
    run_dir.mkdir(parents=True, exist_ok=True)

    cfg = PipelineConfig(tickers=tickers)
    pm = PM(config=cfg, run_dir=run_dir)
    return pm.run_full()
