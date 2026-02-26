from pathlib import Path

import pandas as pd

from tech_pipeline.config import PipelineConfig
from tech_pipeline.pm import PM


def test_candidate_pool_uses_universe_when_tickers_not_provided(tmp_path: Path):
    pm = PM(PipelineConfig(tickers=None), tmp_path)
    ticker_map = {f"T{i}": i for i in range(1000)}

    candidates = pm._candidate_tickers(ticker_map, pool_size=800)

    assert len(candidates) == 800


def test_market_cap_sanity_sets_calc_error(monkeypatch, tmp_path: Path):
    pm = PM(PipelineConfig(tickers=["AAPL"]), tmp_path)

    def fake_prices(_symbol: str):
        return pd.DataFrame(
            {
                "Date": pd.date_range("2024-01-01", periods=130, freq="D"),
                "Close": [100.0] * 130,
                "Volume": [1_000_000] * 130,
            }
        )

    monkeypatch.setattr("tech_pipeline.pm.get_stooq_prices", fake_prices)
    monkeypatch.setattr("tech_pipeline.pm.get_company_facts", lambda _cik: {"facts": {"us-gaap": {}}})

    metrics = pm._compute_market_metrics(["AAPL"], {"AAPL": 320193})

    assert metrics["AAPL"]["market_cap_usd"] == 0.0
    assert metrics["AAPL"]["calc_error"] is True
    assert "sanity_check_failed" in metrics["AAPL"]["calc_error_reason"]
