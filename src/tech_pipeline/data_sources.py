from __future__ import annotations

import datetime as dt
import json
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

SEC_HEADERS = {"User-Agent": "tenbagger-lab/0.1 (research@local)"}


@dataclass
class FilingDoc:
    ticker: str
    form: str
    filing_date: str
    source_url: str
    text: str


def get_stooq_prices(symbol: str) -> pd.DataFrame:
    url = f"https://stooq.com/q/d/l/?s={symbol.lower()}.us&i=d"
    df = pd.read_csv(url)
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date")
    return df


def get_fred_series(series_id: str) -> pd.DataFrame:
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    df = pd.read_csv(url)
    df.columns = ["date", "value"]
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df.dropna()


def get_sec_ticker_map() -> dict[str, int]:
    url = "https://www.sec.gov/files/company_tickers.json"
    data = requests.get(url, headers=SEC_HEADERS, timeout=20).json()
    out: dict[str, int] = {}
    for _, v in data.items():
        out[v["ticker"].upper()] = int(v["cik_str"])
    return out


def _cik(cik_int: int) -> str:
    return str(cik_int).zfill(10)


def get_submissions(cik_int: int) -> dict:
    url = f"https://data.sec.gov/submissions/CIK{_cik(cik_int)}.json"
    return requests.get(url, headers=SEC_HEADERS, timeout=30).json()


def get_company_facts(cik_int: int) -> dict:
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{_cik(cik_int)}.json"
    return requests.get(url, headers=SEC_HEADERS, timeout=30).json()


def extract_fundamental(company_facts: dict, tags: list[str]) -> tuple[float | None, str | None, str | None]:
    us_gaap = company_facts.get("facts", {}).get("us-gaap", {})
    for tag in tags:
        if tag in us_gaap:
            units = us_gaap[tag].get("units", {})
            for unit, points in units.items():
                if points:
                    last = sorted(points, key=lambda x: x.get("end", ""))[-1]
                    return float(last.get("val")), last.get("end"), unit
    return None, None, None


def latest_filings_docs(cik_int: int, ticker: str, forms: tuple[str, ...] = ("10-Q", "10-K", "8-K"), limit: int = 6) -> list[FilingDoc]:
    sub = get_submissions(cik_int)
    recent = sub.get("filings", {}).get("recent", {})
    docs: list[FilingDoc] = []
    for form, acc, filing_date, primary in zip(
        recent.get("form", []),
        recent.get("accessionNumber", []),
        recent.get("filingDate", []),
        recent.get("primaryDocument", []),
    ):
        if form not in forms:
            continue
        acc_no_dash = acc.replace("-", "")
        base = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_no_dash}"
        filing_url = f"{base}/{primary}"
        try:
            r = requests.get(filing_url, headers=SEC_HEADERS, timeout=30)
            text = BeautifulSoup(r.text, "lxml").get_text(" ", strip=True)
        except Exception:
            text = ""
        docs.append(FilingDoc(ticker=ticker, form=form, filing_date=filing_date, source_url=filing_url, text=text))
        if len(docs) >= limit:
            break
    return docs


def mine_evidence(docs: list[FilingDoc], kpis: list[str]) -> list[dict]:
    snippets: list[dict] = []
    for doc in docs:
        low = doc.text.lower()
        for kpi in kpis:
            for m in re.finditer(rf"(.{{0,120}}\b{re.escape(kpi.lower())}\b.{{0,180}})", low):
                snippets.append(
                    {
                        "ticker": doc.ticker,
                        "kpi": kpi,
                        "form": doc.form,
                        "filing_date": doc.filing_date,
                        "snippet": m.group(1),
                        "source_url": doc.source_url,
                        "retrieved_at": dt.datetime.utcnow().isoformat(),
                    }
                )
                if len(snippets) > 200:
                    return snippets
    return snippets


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
