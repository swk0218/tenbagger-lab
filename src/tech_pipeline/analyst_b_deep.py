from __future__ import annotations

from .data_sources import extract_fundamental, get_company_facts
from .macro import utcnow
from .rules import RuleSpec, evaluate_gate


def _normalize_0_100(value: float, min_v: float, max_v: float) -> float:
    if max_v <= min_v:
        return 0.0
    v = max(min_v, min(max_v, value))
    return ((v - min_v) / (max_v - min_v)) * 100.0


def deep_score_0_100(metrics: dict, spec: RuleSpec) -> float:
    normalized = {
        "rs126": _normalize_0_100(float(metrics.get("rs126", 0.0) or 0.0), -0.5, 0.5),
        "revenue_growth_yoy": _normalize_0_100(float(metrics.get("revenue_growth_yoy", 0.0) or 0.0), -0.2, 0.8),
        "operating_margin": _normalize_0_100(float(metrics.get("operating_margin", 0.0) or 0.0), -0.1, 0.5),
        "backlog_signal": _normalize_0_100(float(metrics.get("backlog_signal", 0.0) or 0.0), 0.0, 8.0),
    }
    score = 0.0
    for field, weight in spec.step_b.items():
        score += normalized.get(field, 0.0) * weight
    return round(max(0.0, min(100.0, score)), 2)


def run_deep_for_ticker(
    ticker: str,
    cik: int,
    market_metrics: dict,
    filing_recency_days: float,
    backlog_signal: float,
    spec: RuleSpec,
    attempt: int,
) -> tuple[dict, list[dict]]:
    facts = get_company_facts(cik)
    rev, rev_period, rev_unit = extract_fundamental(facts, ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax"])
    op, op_period, op_unit = extract_fundamental(facts, ["OperatingIncomeLoss"])
    debt, debt_period, debt_unit = extract_fundamental(facts, ["LongTermDebtNoncurrent", "LongTermDebt"])
    eq, eq_period, eq_unit = extract_fundamental(facts, ["StockholdersEquity"])

    metrics = {
        "adtv_3m_usd": float(market_metrics.get("adtv_3m_usd", 0.0) or 0.0),
        "rs126": float(market_metrics.get("rs126", 0.0) or 0.0),
        "market_cap_usd": float(market_metrics.get("market_cap_usd", 0.0) or 0.0),
        "sec_filing_recency_days": filing_recency_days,
        "revenue_growth_yoy": 0.0,
        "operating_margin": (op / rev) if (op and rev) else 0.0,
        "debt_to_equity": (debt / eq) if (debt and eq) else 0.0,
        "backlog_signal": float(backlog_signal),
    }

    failures = [f"{r.field} {r.op} {r.threshold}" for r in spec.step_a if not evaluate_gate(metrics.get(r.field), r.op, r.threshold)]
    passed_step_a = len(failures) == 0
    score = deep_score_0_100(metrics, spec) if passed_step_a else 0.0

    row = {
        "ticker": ticker,
        **metrics,
        "score": score,
        "attempt": attempt,
        "passed_step_a": passed_step_a,
        "failures": "; ".join(failures),
    }

    provenance = [
        {
            "ticker": ticker,
            "metric": "operating_income",
            "value": op if op is not None else "N/A",
            "period": op_period or "N/A",
            "period_type": "FY",
            "unit": op_unit or "USD",
            "source_url": f"https://data.sec.gov/api/xbrl/companyfacts/CIK{str(cik).zfill(10)}.json",
            "source_tier": "tier_1_public_api",
            "retrieved_at": utcnow(),
            "role": "AnalystB_Deep",
        },
        {
            "ticker": ticker,
            "metric": "debt",
            "value": debt if debt is not None else "N/A",
            "period": debt_period or "N/A",
            "period_type": "FY",
            "unit": debt_unit or "USD",
            "source_url": f"https://data.sec.gov/api/xbrl/companyfacts/CIK{str(cik).zfill(10)}.json",
            "source_tier": "tier_1_public_api",
            "retrieved_at": utcnow(),
            "role": "AnalystB_Deep",
        },
        {
            "ticker": ticker,
            "metric": "equity",
            "value": eq if eq is not None else "N/A",
            "period": eq_period or "N/A",
            "period_type": "FY",
            "unit": eq_unit or "USD",
            "source_url": f"https://data.sec.gov/api/xbrl/companyfacts/CIK{str(cik).zfill(10)}.json",
            "source_tier": "tier_1_public_api",
            "retrieved_at": utcnow(),
            "role": "AnalystB_Deep",
        },
    ]
    return row, provenance
