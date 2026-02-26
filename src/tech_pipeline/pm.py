from __future__ import annotations

from pathlib import Path

import pandas as pd

from .analyst_a_quick import run_quick_for_ticker
from .analyst_b_deep import run_deep_for_ticker
from .config import KPI_TERMS, PipelineConfig
from .data_sources import (
    extract_fundamental,
    get_company_facts,
    get_sec_ticker_map,
    get_stooq_prices,
    latest_filings_docs,
    mine_evidence,
    write_jsonl,
)
from .macro import macro_snapshot
from .reporting import validate_required_outputs, write_reports
from .verifier import critical_tickers_from_issues, verify_records


class PM:
    def __init__(self, config: PipelineConfig, run_dir: Path):
        self.config = config
        self.run_dir = run_dir
        self.quick_spec, self.deep_spec = config.load_specs()

    def _compute_market_metrics(self, tickers: list[str], ticker_map: dict[str, int]) -> dict[str, dict]:
        market: dict[str, dict] = {}
        spy = get_stooq_prices("SPY").tail(252).copy()
        spy_ret126 = (spy["Close"].iloc[-1] / spy["Close"].iloc[-126]) - 1 if len(spy) >= 126 else 0.0

        for t in tickers:
            df = get_stooq_prices(t).tail(252).copy()
            if df.empty:
                continue
            adtv = float((df.tail(63)["Close"] * df.tail(63)["Volume"]).mean())
            rs126 = 0.0
            if len(df) >= 126:
                t_ret = (df["Close"].iloc[-1] / df["Close"].iloc[-126]) - 1
                rs126 = float(t_ret - spy_ret126)

            cik = ticker_map.get(t)
            market_cap_usd = 0.0
            if cik:
                try:
                    facts = get_company_facts(cik)
                    shares, _, _ = extract_fundamental(facts, ["CommonStockSharesOutstanding"])
                    if shares:
                        market_cap_usd = float(shares) * float(df["Close"].iloc[-1])
                except Exception:
                    market_cap_usd = 0.0

            market[t] = {"adtv_3m_usd": adtv, "rs126": rs126, "market_cap_usd": market_cap_usd}
        return market

    def run_full(self) -> Path:
        tickers = self.config.tickers
        ticker_map = get_sec_ticker_map()
        market = self._compute_market_metrics(tickers, ticker_map)
        macro_rows = macro_snapshot()

        provenance: list[dict] = []
        evidence_rows: list[dict] = []
        quick_rows: list[dict] = []
        deep_rows: list[dict] = []

        quick_passed: list[tuple[str, int, float, float]] = []

        for ticker in tickers:
            cik = ticker_map.get(ticker)
            m = market.get(ticker)
            if not cik or not m:
                continue
            filings = latest_filings_docs(cik, ticker)
            latest_date = max((pd.to_datetime(f.filing_date) for f in filings), default=pd.Timestamp("1970-01-01"))
            filing_recency_days = float((pd.Timestamp.today() - latest_date).days)
            evidence_rows.extend(mine_evidence(filings, KPI_TERMS))
            backlog_signal = float(sum(1 for e in evidence_rows if e.get("ticker") == ticker and str(e.get("kpi", "")).lower() in {"backlog", "rpo", "remaining performance obligation"}))

            qrow, qprov = run_quick_for_ticker(ticker, cik, m, filing_recency_days, self.quick_spec)
            quick_rows.append(qrow)
            provenance.extend(qprov)
            if qrow["passed_step_a"]:
                quick_passed.append((ticker, cik, filing_recency_days, backlog_signal))

        attempts = 0
        pending = list(quick_passed)
        issues: list[dict] = []
        while attempts <= self.config.max_verifier_retries and pending:
            attempts += 1
            round_rows: list[dict] = []
            round_prov: list[dict] = []

            for ticker, cik, filing_recency_days, backlog_signal in pending:
                drow, dprov = run_deep_for_ticker(ticker, cik, market[ticker], filing_recency_days, backlog_signal, self.deep_spec, attempts)
                round_rows.append(drow)
                round_prov.extend(dprov)

            deep_rows = [r for r in deep_rows if r["ticker"] not in {x[0] for x in pending}]
            deep_rows.extend(round_rows)
            provenance.extend(round_prov)

            _, issues = verify_records(provenance, self.run_dir)
            critical = critical_tickers_from_issues(issues)
            if not critical:
                break
            pending = [item for item in pending if item[0] in critical]

        write_jsonl(self.run_dir / "provenance.jsonl", provenance)
        write_jsonl(self.run_dir / "evidence_snippets.jsonl", evidence_rows)
        write_reports(self.run_dir, macro_rows, quick_rows, deep_rows, issues)

        missing = validate_required_outputs(self.run_dir)
        if missing:
            raise RuntimeError(f"Missing required outputs: {missing}")

        return self.run_dir
