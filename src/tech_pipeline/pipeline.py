from __future__ import annotations

import datetime as dt
import json
from dataclasses import asdict
from pathlib import Path

import pandas as pd

from .data_sources import (
    extract_fundamental,
    get_company_facts,
    get_fred_series,
    get_sec_ticker_map,
    get_stooq_prices,
    latest_filings_docs,
    mine_evidence,
    write_jsonl,
)
from .rules import RuleSpec, evaluate_gate, load_rules
from .contract import verify_contract

DEFAULT_TICKERS = ["MSFT", "AAPL", "NVDA", "AMZN", "GOOGL", "META"]
FRED_SERIES = ["DGS10", "EFFR", "ICSA", "BAMLH0A0HYM2", "T10Y2Y"]
KPI_TERMS = ["backlog", "remaining performance obligation", "rpo", "net revenue retention", "nrr"]


def _utcnow() -> str:
    return dt.datetime.utcnow().isoformat()


def _numeric_record(ticker: str, metric: str, value, period, unit, source_url, role: str) -> dict:
    return {
        "ticker": ticker,
        "metric": metric,
        "value": value,
        "period": period,
        "unit": unit,
        "source_url": source_url,
        "retrieved_at": _utcnow(),
        "role": role,
    }


def _compute_market_metrics(tickers: list[str]) -> dict[str, dict]:
    market: dict[str, dict] = {}
    spy = get_stooq_prices("SPY")
    spy = spy.tail(252).copy()
    spy_ret126 = (spy["Close"].iloc[-1] / spy["Close"].iloc[-126]) - 1 if len(spy) >= 126 else 0.0
    for t in tickers:
        df = get_stooq_prices(t)
        df = df.tail(252).copy()
        if df.empty:
            continue
        adtv = (df.tail(63)["Close"] * df.tail(63)["Volume"]).mean()
        rs126 = 0.0
        if len(df) >= 126 and spy_ret126 != 0:
            t_ret = (df["Close"].iloc[-1] / df["Close"].iloc[-126]) - 1
            rs126 = t_ret - spy_ret126
        market[t] = {"adtv_3m_usd": float(adtv), "rs126": float(rs126)}
    return market


def _macro_snapshot() -> list[dict]:
    rows = []
    for s in FRED_SERIES:
        df = get_fred_series(s)
        if not df.empty:
            rows.append({"series": s, "date": str(df.iloc[-1]["date"].date()), "value": float(df.iloc[-1]["value"])})
    return rows


def _score_ticker(metrics: dict, spec: RuleSpec) -> tuple[bool, float, list[str]]:
    failures = []
    for rule in spec.step_a:
        if not evaluate_gate(metrics.get(rule.field), rule.op, rule.threshold):
            failures.append(f"{rule.field} {rule.op} {rule.threshold}")
    passed = not failures
    score = 0.0
    if passed:
        for k, w in spec.step_b.items():
            score += float(metrics.get(k, 0) or 0) * w
    return passed, score, failures


def run(mode: str = "full", tickers: list[str] | None = None) -> Path:
    tickers = tickers or DEFAULT_TICKERS
    run_dir = Path("runs") / dt.date.today().isoformat()
    run_dir.mkdir(parents=True, exist_ok=True)

    quick_spec = load_rules("spec/quick_rules.md")
    deep_spec = load_rules("spec/deep_rules.md")

    macro = _macro_snapshot()
    market = _compute_market_metrics(tickers)
    ticker_map = get_sec_ticker_map()

    all_records: list[dict] = []
    evidence: list[dict] = []
    quick_rows: list[dict] = []
    deep_rows: list[dict] = []

    for ticker in tickers:
        cik = ticker_map.get(ticker)
        if not cik or ticker not in market:
            continue

        facts = get_company_facts(cik)
        rev, rev_period, rev_unit = extract_fundamental(facts, ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax"])
        gm, gm_period, gm_unit = extract_fundamental(facts, ["GrossProfit"])
        op, op_period, op_unit = extract_fundamental(facts, ["OperatingIncomeLoss"])
        debt, debt_period, debt_unit = extract_fundamental(facts, ["LongTermDebtNoncurrent", "LongTermDebt"])
        eq, eq_period, eq_unit = extract_fundamental(facts, ["StockholdersEquity"])
        mc = market[ticker]

        filings = latest_filings_docs(cik, ticker)
        filing_recency = 999.0
        if filings:
            latest_date = max(pd.to_datetime(f.filing_date) for f in filings)
            filing_recency = float((pd.Timestamp.today() - latest_date).days)
        evidence.extend(mine_evidence(filings, KPI_TERMS))
        backlog_signal = sum(1 for e in evidence if e["ticker"] == ticker and e["kpi"] in {"backlog", "rpo", "remaining performance obligation"})

        base_metrics = {
            "adtv_3m_usd": mc.get("adtv_3m_usd"),
            "rs126": mc.get("rs126"),
            "market_cap_usd": None,
            "sec_filing_recency_days": filing_recency,
            "revenue_growth_yoy": 0.0,
            "gross_margin": (gm / rev) if gm and rev else 0.0,
            "operating_margin": (op / rev) if op and rev else 0.0,
            "debt_to_equity": (debt / eq) if debt and eq else 0.0,
            "backlog_signal": float(backlog_signal),
        }

        all_records.extend(
            [
                _numeric_record(ticker, "revenue", rev if rev is not None else "N/A", rev_period, rev_unit, f"https://data.sec.gov/api/xbrl/companyfacts/CIK{str(cik).zfill(10)}.json", "AnalystA"),
                _numeric_record(ticker, "gross_profit", gm if gm is not None else "N/A", gm_period, gm_unit, f"https://data.sec.gov/api/xbrl/companyfacts/CIK{str(cik).zfill(10)}.json", "AnalystA"),
                _numeric_record(ticker, "operating_income", op if op is not None else "N/A", op_period, op_unit, f"https://data.sec.gov/api/xbrl/companyfacts/CIK{str(cik).zfill(10)}.json", "AnalystB"),
                _numeric_record(ticker, "debt", debt if debt is not None else "N/A", debt_period, debt_unit, f"https://data.sec.gov/api/xbrl/companyfacts/CIK{str(cik).zfill(10)}.json", "AnalystB"),
            ]
        )

        if mode in {"quick", "full"}:
            passed, score, failures = _score_ticker(base_metrics, quick_spec)
            quick_rows.append({"ticker": ticker, "passed_step_a": passed, "score": score, "failures": "; ".join(failures), **base_metrics})

        if mode in {"deep", "full"}:
            attempts = 0
            final_row = None
            while attempts < 2:
                attempts += 1
                passed, score, failures = _score_ticker(base_metrics, deep_spec)
                final_row = {"ticker": ticker, "passed_step_a": passed, "score": score, "failures": "; ".join(failures), "attempt": attempts, **base_metrics}
                if backlog_signal > 0:
                    break
                filings = latest_filings_docs(cik, ticker)
                evidence.extend(mine_evidence(filings, KPI_TERMS))
                backlog_signal = sum(1 for e in evidence if e["ticker"] == ticker and e["kpi"] in {"backlog", "rpo", "remaining performance obligation"})
                base_metrics["backlog_signal"] = float(backlog_signal)
            if final_row:
                deep_rows.append(final_row)

    all_records, verifier_issues = verify_contract(all_records, run_dir)

    write_jsonl(run_dir / "provenance.jsonl", all_records)
    write_jsonl(run_dir / "evidence_snippets.jsonl", evidence)

    if quick_rows:
        qdf = pd.DataFrame(quick_rows)
        qdf.to_parquet(run_dir / "metrics_quick.parquet", index=False)
        (run_dir / "report_quick.md").write_text(
            "# PM + AnalystA Quick Report\n\n"
            f"Generated: {_utcnow()}\n\n"
            "## MacroStrategist Snapshot\n"
            + "\n".join(f"- {m['series']}: {m['value']} ({m['date']})" for m in macro)
            + "\n\n## Quick Ranking\n"
            + qdf[["ticker", "score", "passed_step_a", "failures"]].sort_values("score", ascending=False).to_markdown(index=False)
            + f"\n\n## DataVerifier Issues\n- {len(verifier_issues)} issues"
            ,
            encoding="utf-8",
        )

    if deep_rows:
        ddf = pd.DataFrame(deep_rows)
        ddf.to_parquet(run_dir / "metrics_deep.parquet", index=False)
        (run_dir / "report_deep.md").write_text(
            "# PM + AnalystB Deep Report\n\n"
            f"Generated: {_utcnow()}\n\n"
            "## Verification Loop\n"
            "- PM retries evidence mining and reruns AnalystB for affected tickers (max 2 attempts).\n\n"
            "## Deep Ranking\n"
            + ddf[["ticker", "score", "attempt", "passed_step_a", "failures"]].sort_values("score", ascending=False).to_markdown(index=False)
            + f"\n\n## DataVerifier Issues\n- {len(verifier_issues)} issues"
            ,
            encoding="utf-8",
        )

    return run_dir
