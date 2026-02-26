from __future__ import annotations

import datetime as dt

from .config import FRED_SERIES
from .data_sources import get_fred_series


def utcnow() -> str:
    return dt.datetime.utcnow().isoformat()


def macro_snapshot() -> list[dict]:
    rows = []
    for series in FRED_SERIES:
        df = get_fred_series(series)
        if df.empty:
            continue
        rows.append(
            {
                "series": series,
                "date": str(df.iloc[-1]["date"].date()),
                "value": float(df.iloc[-1]["value"]),
                "retrieved_at": utcnow(),
                "source_tier": "tier_1_public_api",
            }
        )
    return rows
