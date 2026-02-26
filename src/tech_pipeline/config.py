from __future__ import annotations

from dataclasses import dataclass, field

from .rules import GateRule, RuleSpec, load_rules

DEFAULT_TICKERS = ["MSFT", "AAPL", "NVDA", "AMZN", "GOOGL", "META"]
FRED_SERIES = ["DGS10", "EFFR", "ICSA", "BAMLH0A0HYM2", "T10Y2Y"]
KPI_TERMS = ["backlog", "remaining performance obligation", "rpo", "net revenue retention", "nrr"]

# Hard gate: excludes mega-cap > 40B by default.
MAX_MARKET_CAP_USD = 40_000_000_000

REQUIRED_OUTPUTS = [
    "report_quick.md",
    "report_deep.md",
    "metrics_quick.parquet",
    "metrics_deep.parquet",
    "provenance.jsonl",
    "evidence_snippets.jsonl",
]


@dataclass(slots=True)
class PipelineConfig:
    tickers: list[str] | None = None
    quick_rules_path: str = "spec/quick_rules.md"
    deep_rules_path: str = "spec/deep_rules.md"
    hard_max_market_cap_usd: float = MAX_MARKET_CAP_USD
    max_verifier_retries: int = 2
    quick_candidate_pool_size: int = 800

    def load_specs(self) -> tuple[RuleSpec, RuleSpec]:
        quick = load_rules(self.quick_rules_path)
        deep = load_rules(self.deep_rules_path)
        quick.step_a = [*quick.step_a, GateRule("market_cap_usd", "<=", self.hard_max_market_cap_usd)]
        deep.step_a = [*deep.step_a, GateRule("market_cap_usd", "<=", self.hard_max_market_cap_usd)]
        return quick, deep
