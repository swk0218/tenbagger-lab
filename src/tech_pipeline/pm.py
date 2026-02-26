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

MEGA_CAP_SANITY_TICKERS = {"AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META"}


class PM:
    def __init__(self, config: PipelineConfig, run_dir: Path):
        self.config = config
        self.run_dir = run_dir
        self.quick_spec, self.deep_spec = config.load_specs()

    def _candidate_tickers(self, ticker_map: dict[str, int], pool_size: int) -> list[str]:
        if self.config.tickers:
            return [t for t in self.config.tickers if t in ticker_map]
        return sorted(ticker_map.keys())[:pool_size]

    def _compute_market_metrics(self, tickers: list[str], ticker_map: dict[str, int]) -> dict[str, dict]:
        market: dict[str, dict] = {}
        spy = get_stooq_prices("SPY").tail(252).copy()
        spy_ret126 = (spy["Close"].iloc[-1] / spy["Close"].iloc[-126]) - 1 if len(spy) >= 126 else 0.0

        for t in tickers:
            try:
                df = get_stooq_prices(t).tail(252).copy()
            except Exception:
                continue
            if df.empty:
                continue

            price = float(df["Close"].iloc[-1])
            adtv = float((df.tail(63)["Close"] * df.tail(63)["Volume"]).mean())
            rs126 = 0.0
            if len(df) >= 126:
                t_ret = (df["Close"].iloc[-1] / df["Close"].iloc[-126]) - 1
                rs126 = float(t_ret - spy_ret126)

            cik = ticker_map.get(t)
            market_cap_usd = 0.0
            shares_outstanding = 0.0
            calc_error = False
            calc_error_reason = ""

            if cik:
                try:
                    facts = get_company_facts(cik)
                    shares, _, _ = extract_fundamental(facts, ["CommonStockSharesOutstanding"])
                    shares_outstanding = float(shares or 0.0)
                    market_cap_usd = shares_outstanding * price
                except Exception:
                    market_cap_usd = 0.0

            if t in MEGA_CAP_SANITY_TICKERS and market_cap_usd <= 200_000_000_000:
                calc_error = True
                calc_error_reason = "market_cap_usd_sanity_check_failed"

            market[t] = {
                "adtv_3m_usd": adtv,
                "rs126": rs126,
                "market_cap_usd": market_cap_usd,
                "price": price,
                "shares_outstanding": shares_outstanding,
                "calc_error": calc_error,
                "calc_error_reason": calc_error_reason,
            }
        return market

    def _run_quick_screen(self, ticker_map: dict[str, int], pool_size: int) -> tuple[list[dict], list[dict], list[dict], list[tuple[str, int, float, float]], list[str]]:
        tickers = self._candidate_tickers(ticker_map, pool_size)
        market = self._compute_market_metrics(tickers, ticker_map)

        provenance: list[dict] = []
        evidence_rows: list[dict] = []
        pass_rows: list[dict] = []
        quick_passed: list[tuple[str, int, float, float]] = []
        rejected_examples: list[str] = []

        for ticker in tickers:
            cik = ticker_map.get(ticker)
            m = market.get(ticker)
            if not cik or not m:
                continue

            filings = latest_filings_docs(cik, ticker)
            latest_date = max((pd.to_datetime(f.filing_date) for f in filings), default=pd.Timestamp("1970-01-01"))
            filing_recency_days = float((pd.Timestamp.today() - latest_date).days)
            evidence_rows.extend(mine_evidence(filings, KPI_TERMS))
            backlog_signal = float(
                sum(
                    1
                    for e in evidence_rows
                    if e.get("ticker") == ticker and str(e.get("kpi", "")).lower() in {"backlog", "rpo", "remaining performance obligation"}
                )
            )

            qrow, qprov = run_quick_for_ticker(ticker, cik, m, filing_recency_days, self.quick_spec)
            provenance.extend(qprov)

            if m.get("calc_error"):
                reason = m.get("calc_error_reason") or "calc_error"
                rejected_examples.append(f"{ticker} ({reason})")
                continue

            if qrow["passed_step_a"]:
                pass_rows.append(qrow)
                quick_passed.append((ticker, cik, filing_recency_days, backlog_signal))
            elif len(rejected_examples) < 10:
                reason = qrow["failures"] or "hard_gate_failed"
                rejected_examples.append(f"{ticker} ({reason})")

        for row in pass_rows:
            row["segment"] = "edge"
        for i, row in enumerate(sorted(pass_rows, key=lambda r: r["score"], reverse=True)[:6]):
            if i < 2:
                row["segment"] = "core"
            elif i < 4:
                row["segment"] = "watch"
            else:
                row["segment"] = "edge"

        return pass_rows, provenance, evidence_rows, quick_passed, rejected_examples[:10]

    def run_full(self) -> Path:
        ticker_map = get_sec_ticker_map()
        macro_rows = macro_snapshot()

        quick_rows, provenance, evidence_rows, quick_passed, rejected_examples = self._run_quick_screen(
            ticker_map,
            pool_size=self.config.quick_candidate_pool_size,
        )

        if len(quick_passed) < 6 and not self.config.tickers:
            quick_rows, provenance, evidence_rows, quick_passed, rejected_examples = self._run_quick_screen(ticker_map, pool_size=2000)

        quick_rows = sorted(quick_rows, key=lambda r: r["score"], reverse=True)[:6]
        quick_pass_tickers = {r["ticker"] for r in quick_rows}

        deep_rows: list[dict] = []
        pending = [item for item in quick_passed if item[0] in quick_pass_tickers]

        attempts = 0
        issues: list[dict] = []
        while attempts <= self.config.max_verifier_retries and pending:
            attempts += 1
            round_rows: list[dict] = []
            round_prov: list[dict] = []

            market = self._compute_market_metrics([x[0] for x in pending], ticker_map)
            for ticker, cik, filing_recency_days, backlog_signal in pending:
                if ticker not in market:
                    continue
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

        notes: list[str] = []
        if len(quick_rows) < 3:
            notes.append("스크리닝 부적합")
        if rejected_examples:
            notes.append("Rejected examples: " + ", ".join(rejected_examples))

        if quick_rows:
            top = quick_rows[0]
            notes.append(
                "Top1 market_cap calc: "
                f"price={top.get('price', 0.0):.2f}, "
                f"shares_outstanding={top.get('shares_outstanding', 0.0):.0f}, "
                f"computed_market_cap_usd={top.get('market_cap_usd', 0.0):.2f}"
            )

        write_jsonl(self.run_dir / "provenance.jsonl", provenance)
        write_jsonl(self.run_dir / "evidence_snippets.jsonl", evidence_rows)
        write_reports(self.run_dir, macro_rows, quick_rows, deep_rows, issues, notes)

        missing = validate_required_outputs(self.run_dir)
        if missing:
            raise RuntimeError(f"Missing required outputs: {missing}")

        return self.run_dir
