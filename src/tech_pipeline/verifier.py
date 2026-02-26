from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

CRITICAL_FIELDS = ["unit", "period_type", "retrieved_at", "source_tier"]
VALID_PERIOD_TYPES = {"TTM", "FY", "NTM"}


def _utcnow() -> str:
    return dt.datetime.utcnow().isoformat()


def verify_records(records: list[dict], out_dir: Path) -> tuple[list[dict], list[dict]]:
    issues: list[dict] = []
    ticket_dir = out_dir / "tickets"
    ticket_dir.mkdir(parents=True, exist_ok=True)

    for rec in records:
        if rec.get("value") in (None, "", "N/A"):
            continue

        missing = []
        for field in CRITICAL_FIELDS:
            if rec.get(field) in (None, "", "N/A"):
                missing.append(field)
        if rec.get("period_type") not in VALID_PERIOD_TYPES:
            missing.append("period_type_label")

        for field in missing:
            issues.append(
                {
                    "ticker": rec.get("ticker"),
                    "metric": rec.get("metric"),
                    "issue": f"missing_or_invalid_{field}",
                    "critical": True,
                    "created_at": _utcnow(),
                }
            )

    for idx, issue in enumerate(issues, start=1):
        (ticket_dir / f"ticket_{idx:03d}.json").write_text(json.dumps(issue, indent=2), encoding="utf-8")
    return records, issues


def critical_tickers_from_issues(issues: list[dict]) -> set[str]:
    return {str(i.get("ticker")) for i in issues if i.get("critical") and i.get("ticker")}
