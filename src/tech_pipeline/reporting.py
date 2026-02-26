from __future__ import annotations

from pathlib import Path

import pandas as pd

from .config import REQUIRED_OUTPUTS
from .macro import utcnow


def write_reports(
    run_dir: Path,
    macro_rows: list[dict],
    quick_ranked_rows: list[dict],
    deep_rows: list[dict],
    verifier_issues: list[dict],
    quick_notes: list[str],
) -> None:
    qdf = pd.DataFrame(quick_ranked_rows)
    ddf = pd.DataFrame(deep_rows)

    qdf.to_parquet(run_dir / "metrics_quick.parquet", index=False)
    ddf.to_parquet(run_dir / "metrics_deep.parquet", index=False)

    notes_md = "\n".join(f"- {n}" for n in quick_notes) if quick_notes else "- N/A"
    quick_table = qdf[["ticker", "score", "passed_step_a", "failures"]].sort_values("score", ascending=False).to_markdown(index=False) if not qdf.empty else "No ranked pass candidates"

    (run_dir / "report_quick.md").write_text(
        "# PM + AnalystA Quick Report\n\n"
        f"Generated: {utcnow()}\n\n"
        "## Macro Snapshot\n"
        + "\n".join(f"- {m['series']}: {m['value']} ({m['date']})" for m in macro_rows)
        + "\n\n## Quick Ranking (0-100, pass-only)\n"
        + quick_table
        + "\n\n## Notes\n"
        + notes_md,
        encoding="utf-8",
    )

    (run_dir / "report_deep.md").write_text(
        "# PM + AnalystB Deep Report\n\n"
        f"Generated: {utcnow()}\n\n"
        "## Orchestration\n"
        "- Auto sequence: Quick -> Deep.\n"
        "- PM retries when verifier flags critical missing fields (max 2 retries).\n\n"
        "## Deep Ranking (0-100)\n"
        + (ddf[["ticker", "score", "attempt", "passed_step_a", "failures"]].sort_values("score", ascending=False).to_markdown(index=False) if not ddf.empty else "No rows")
        + f"\n\n## Verifier Issues\n- {len(verifier_issues)} issues",
        encoding="utf-8",
    )


def validate_required_outputs(run_dir: Path) -> list[str]:
    return [name for name in REQUIRED_OUTPUTS if not (run_dir / name).exists()]
