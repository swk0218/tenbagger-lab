from __future__ import annotations

import datetime as dt
import json
from pathlib import Path


def _utcnow() -> str:
    return dt.datetime.utcnow().isoformat()


def verify_contract(records: list[dict], out_dir: Path) -> tuple[list[dict], list[dict]]:
    verifier_issues: list[dict] = []
    for rec in records:
        if isinstance(rec.get("value"), (float, int)):
            for field in ["period", "unit", "source_url", "retrieved_at"]:
                if rec.get(field) in (None, "", "N/A"):
                    issue = {
                        "ticker": rec.get("ticker"),
                        "metric": rec.get("metric"),
                        "issue": f"missing_{field}",
                        "created_at": _utcnow(),
                    }
                    verifier_issues.append(issue)
                    rec["value"] = "N/A"
                    rec["period"] = rec.get("period") or "N/A"
                    rec["unit"] = rec.get("unit") or "N/A"
                    rec["source_url"] = rec.get("source_url") or "N/A"
                    rec["retrieved_at"] = rec.get("retrieved_at") or "N/A"
    ticket_dir = out_dir / "tickets"
    ticket_dir.mkdir(parents=True, exist_ok=True)
    for i, issue in enumerate(verifier_issues, start=1):
        (ticket_dir / f"ticket_{i:03d}.json").write_text(json.dumps(issue, indent=2), encoding="utf-8")
    return records, verifier_issues
