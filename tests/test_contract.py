from pathlib import Path

from tech_pipeline.contract import verify_contract


def test_verify_contract_sets_na_and_tickets(tmp_path: Path):
    recs = [{"ticker": "ABC", "metric": "m", "value": 1.0, "period": None, "unit": "USD", "source_url": "u", "retrieved_at": "r"}]
    updated, issues = verify_contract(recs, tmp_path)
    assert issues
    assert updated[0]["value"] == "N/A"
    assert (tmp_path / "tickets").exists()
