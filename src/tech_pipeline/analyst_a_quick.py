from __future__ import annotations

from .data_sources import extract_fundamental, get_company_facts
from .macro import utcnow
from .rules import RuleSpec, evaluate_gate


def _normalize_0_100(value: float, min_v: float, max_v: float) -> float:
    if max_v <= min_v:
        return 0.0
    v = max(min_v, min(max_v, value))
    return ((v - min_v) / (max_v - min_v)) * 100.0


def quick_score_0_100(metrics: dict, spec: RuleSpec) -> float:
    normalized = {
        "rs126": _normalize_0_100(float(metrics.get("rs126", 0.0) or 0.0), -0.5, 0.5),
        "revenue_growth_yoy": _normalize_0_100(float(metrics.get("revenue_growth_yoy", 0.0) or 0.0), -0.2, 0.8),
        "gross_margin": _normalize_0_100(float(metrics.get("gross_margin", 0.0) or 0.0), 0.0, 0.9),
    }
    score = 0.0
    for field, weight in spec.step_b.items():
        score += normalized.get(field, 0.0) * weight
    return round(max(0.0, min(100.0, score)), 2)


def run_quick_for_ticker(ticker: str, cik: int, market_metrics: dict, filing_recency_days: float, spec: RuleSpec) -> tuple[dict, list[dict]]:
    facts = get_company_facts(cik)
    rev, rev_period, rev_unit = extract_fundamental(facts, ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax"])
    gm, gm_period, gm_unit = extract_fundamental(facts, ["GrossProfit"])

    metrics = {
        "adtv_3m_usd": float(market_metrics.get("adtv_3m_usd", 0.0) or 0.0),
        "rs126": float(market_metrics.get("rs126", 0.0) or 0.0),
        "market_cap_usd": float(market_metrics.get("market_cap_usd", 0.0) or 0.0),
        "price": float(market_metrics.get("price", 0.0) or 0.0),
        "shares_outstanding": float(market_metrics.get("shares_outstanding", 0.0) or 0.0),
        "sec_filing_recency_days": filing_recency_days,
        "revenue_growth_yoy": 0.0,
        "gross_margin": (gm / rev) if (gm and rev) else 0.0,
    }

    failures = [f"{r.field} {r.op} {r.threshold}" for r in spec.step_a if not evaluate_gate(metrics.get(r.field), r.op, r.threshold)]
    passed_step_a = len(failures) == 0
    score = quick_score_0_100(metrics, spec) if passed_step_a else 0.0

    row = {
        "ticker": ticker,
        **metrics,
        "score": score,
        "passed_step_a": passed_step_a,
        "failures": "; ".join(failures),
    }

    provenance = [
        {
            "ticker": ticker,
            "metric": "revenue",
            "value": rev if rev is not None else "N/A",
            "period": rev_period or "N/A",
            "period_type": "TTM",
            "unit": rev_unit or "USD",
            "source_url": f"https://data.sec.gov/api/xbrl/companyfacts/CIK{str(cik).zfill(10)}.json",
            "source_tier": "tier_1_public_api",
            "retrieved_at": utcnow(),
            "role": "AnalystA_Quick",
        },
        {
            "ticker": ticker,
            "metric": "gross_profit",
            "value": gm if gm is not None else "N/A",
            "period": gm_period or "N/A",
            "period_type": "TTM",
            "unit": gm_unit or "USD",
            "source_url": f"https://data.sec.gov/api/xbrl/companyfacts/CIK{str(cik).zfill(10)}.json",
            "source_tier": "tier_1_public_api",
            "retrieved_at": utcnow(),
            "role": "AnalystA_Quick",
        },
    ]
    return row, provenance
