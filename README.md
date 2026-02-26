# tenbagger-lab

Reproducible weekly US tech stock research pipeline using only free public data sources.

## Setup

Using uv:

```bash
uv venv
uv pip install -e .[dev]
```

Using poetry (alternative):

```bash
poetry install
```

## Run

```bash
make run
make run_quick
make run_deep
```

Outputs are written to `runs/YYYY-MM-DD/`:
- `report_quick.md`, `report_deep.md`
- `metrics_quick.parquet`, `metrics_deep.parquet`
- `provenance.jsonl`, `evidence_snippets.jsonl`
- `tickets/*.json`

## Data sources
- SEC EDGAR submissions + company facts APIs (XBRL)
- SEC filing documents (10-Q/10-K/8-K) for KPI keyword evidence snippets
- FRED public CSV endpoints for macro series: DGS10, EFFR, ICSA, BAMLH0A0HYM2, T10Y2Y
- Stooq free daily EOD prices for SPY and tickers
