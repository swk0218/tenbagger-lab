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

MEGA_SANITY_TICKERS = {"AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META"}


class PM:
    def __init__(self, config: PipelineConfig, run_dir: Path):
        self.config = config
        self.run_dir = run_dir
        self.quick_spec, self.deep_spec = config.load_specs()

    def _resolve_universe(self, ticker_map: dict[str, int], pool_size: int) -> list[str]:
        if self.config.tickers:
            return [t.upper() for t in self.config.tickers]
        universe = sorted(t for t in ticker_map.keys() if t.isalpha() and 1 <= len(t) <= 5)
        return universe[:pool_size]

    def _compute_market_metrics(self, tickers: list[str], ticker_map: dict[str, int]) -> tuple[dict[str, dict], list[str]]:
        market: dict[str, dict] = {}
        calc_errors: list[str] = []
        spy = get_stooq_prices("SPY").tail(252).copy()
        spy_ret126 = (spy["Close"].iloc[-1] / spy["Close"].iloc[-126]) - 1 if len(spy) >= 126 else 0.0

        for ticker in tickers:
            try:
                df = get_stooq_prices(ticker).tail(252).copy()
                if df.empty:
                    continue
                price = float(df["Close"].iloc[-1])
                adtv = float((df.tail(63)["Close"] * df.tail(63)["Volume"]).mean())
                rs126 = 0.0
                if len(df) >= 126:
                    t_ret = (df["Close"].iloc[-1] / df["Close"].iloc[-126]) - 1
                    rs126 = float(t_ret - spy_ret126)

                cik = ticker_map.get(ticker)
                shares_outstanding = None
                market_cap_usd = 0.0
                if cik:
                    facts = get_company_facts(cik)
                    shares_outstanding, _, _ = extract_fundamental(facts, ["CommonStockSharesOutstanding"])
                    if shares_outstanding:
                        market_cap_usd = float(shares_outstanding) * price

                calc_error = False
                if ticker in MEGA_SANITY_TICKERS and market_cap_usd <= 200_000_000_000:
                    calc_error = True
                    calc_errors.append(f"{ticker} (calc_error: market cap sanity failed)")

                market[ticker] = {
                    "adtv_3m_usd": adtv,
                    "rs126": rs126,
                    "market_cap_usd": market_cap_usd,
                    "price_usd": price,
                    "shares_outstanding": shares_outstanding,
                    "calc_error": calc_error,
                }
            except Exception:
                continue
        return market, calc_errors

    def _select_top6(self, pass_rows: list[dict]) -> list[dict]:
        ordered = sorted(pass_rows, key=lambda r: float(r.get("score", 0.0)), reverse=True)
        core = [r for r in ordered if float(r.get("score", 0.0)) >= 75]
        watch = [r for r in ordered if 60 <= float(r.get("score", 0.0)) < 75]
        edge = [r for r in ordered if float(r.get("score", 0.0)) < 60]
        return (core + watch + edge)[:6]

    def _run_quick_stage(
        self,
        tickers: list[str],
        ticker_map: dict[str, int],
        market: dict[str, dict],
        evidence_rows: list[dict],
        provenance: list[dict],
    ) -> tuple[list[dict], list[dict], list[tuple[str, int, float, float]], list[str]]:
        pass_rows: list[dict] = []
        failed_examples: list[dict] = []
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
            backlog_signal = float(
                sum(
                    1
                    for e in evidence_rows
                    if e.get("ticker") == ticker and str(e.get("kpi", "")).lower() in {"backlog", "rpo", "remaining performance obligation"}
                )
            )

            qrow, qprov = run_quick_for_ticker(ticker, cik, m, filing_recency_days, self.quick_spec)
            if m.get("calc_error"):
                qrow["passed_step_a"] = False
                qrow["failures"] = "; ".join([x for x in [qrow.get("failures", ""), "calc_error_market_cap"] if x]).strip("; ")
                qrow["score"] = 0.0

            provenance.extend(qprov)
            if qrow["passed_step_a"]:
                pass_rows.append(qrow)
                quick_passed.append((ticker, cik, filing_recency_days, backlog_signal))
            else:
                failed_examples.append({"ticker": ticker, "reason": qrow.get("failures") or "failed_step_a"})

        rejected_notes = [f"Rejected examples: {r['ticker']} ({r['reason']})" for r in failed_examples[:10]]
        return pass_rows, failed_examples, quick_passed, rejected_notes

    def run_full(self) -> Path:
        pool_size = self.config.quick_candidate_pool_size
        evidence_rows: list[dict] = []
        provenance: list[dict] = []
        quick_notes: list[str] = []

        try:
            ticker_map = get_sec_ticker_map()
        except Exception as exc:
            ticker_map = {}
            quick_notes.append(f"Universe load failed: {type(exc).__name__}")

        try:
            macro_rows = macro_snapshot()
        except Exception as exc:
            macro_rows = []
            quick_notes.append(f"Macro snapshot failed: {type(exc).__name__}")


        universe = self._resolve_universe(ticker_map, pool_size) if ticker_map else []
        calc_errors: list[str] = []
        market: dict[str, dict] = {}
        pass_rows: list[dict] = []
        quick_passed: list[tuple[str, int, float, float]] = []
        rejected_notes: list[str] = []

        if universe:
            market, calc_errors = self._compute_market_metrics(universe, ticker_map)
            pass_rows, _, quick_passed, rejected_notes = self._run_quick_stage(universe, ticker_map, market, evidence_rows, provenance)

        if universe and (not self.config.tickers) and len(pass_rows) < 6:
            expanded_universe = self._resolve_universe(ticker_map, 2000)
            market, calc_errors = self._compute_market_metrics(expanded_universe, ticker_map)
            evidence_rows = []
            provenance = []
            pass_rows, _, quick_passed, rejected_notes = self._run_quick_stage(expanded_universe, ticker_map, market, evidence_rows, provenance)
            quick_notes.append("Pass candidates < 6 at pool 800. Retried once with pool 2000.")

        ranked_quick = self._select_top6(pass_rows)

        if len(pass_rows) < 3:
            quick_notes.insert(0, "스크리닝 부적합")
        quick_notes.extend(rejected_notes)
        quick_notes.extend(calc_errors[:10])

        if ranked_quick:
            top = ranked_quick[0]
            mm = market.get(top["ticker"], {})
            quick_notes.append(
                "Market cap example (#1 pass): "
                f"ticker={top['ticker']}, price={mm.get('price_usd')}, shares_outstanding={mm.get('shares_outstanding')}, "
                f"computed_market_cap_usd={mm.get('market_cap_usd')}"
            )

        attempts = 0
        pending = list(quick_passed)
        deep_rows: list[dict] = []
        issues: list[dict] = []

        while attempts <= self.config.max_verifier_retries and pending:
            attempts += 1
            round_rows: list[dict] = []
            round_prov: list[dict] = []

            for ticker, cik, filing_recency_days, backlog_signal in pending:
                if ticker not in {r["ticker"] for r in ranked_quick}:
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

        write_jsonl(self.run_dir / "provenance.jsonl", provenance)
        write_jsonl(self.run_dir / "evidence_snippets.jsonl", evidence_rows)
        write_reports(self.run_dir, macro_rows, ranked_quick, deep_rows, issues, quick_notes)

        missing = validate_required_outputs(self.run_dir)
        if missing:
            raise RuntimeError(f"Missing required outputs: {missing}")
        return self.run_dir
