from __future__ import annotations

import datetime as dt
import json
import os
import random
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup


# -----------------------------------------------------------------------------
# SEC access settings
# -----------------------------------------------------------------------------
# IMPORTANT: SEC는 자동화 요청에 명확한 User-Agent(앱/조직 + 연락처) 사용을 권장/요구합니다.
# GitHub Actions에서는 Repository Secret으로 SEC_USER_AGENT를 넣어주세요.
# 예: "tenbagger-lab (swk0218; contact: youremail@gmail.com)"
SEC_UA = os.getenv("SEC_USER_AGENT", "tenbagger-lab (contact: research@local)")

SEC_HEADERS = {
    "User-Agent": SEC_UA,
    "Accept": "application/json,text/plain,*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# SEC는 과도한 요청을 싫어합니다. (특히 Actions 환경에서)
# 필요시 0.2~0.5초로 올리세요.
SEC_POLITE_SLEEP_SEC = float(os.getenv("SEC_POLITE_SLEEP_SEC", "0.15"))


@dataclass
class FilingDoc:
    ticker: str
    form: str
    filing_date: str
    source_url: str
    text: str


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _sleep_backoff(attempt: int) -> None:
    # exponential backoff + jitter
    wait = (2**attempt) * 0.7 + random.uniform(0.0, 0.8)
    time.sleep(wait)


def _get_text_with_retries(url: str, headers: dict[str, str], timeout: int = 30, retries: int = 5) -> str:
    last_err: Exception | None = None
    for i in range(retries):
        try:
            time.sleep(SEC_POLITE_SLEEP_SEC)
            r = requests.get(url, headers=headers, timeout=timeout)

            if r.status_code in (429, 500, 502, 503, 504):
                _sleep_backoff(i)
                continue

            if r.status_code != 200:
                preview = (r.text or "")[:300].replace("\n", " ")
                raise RuntimeError(f"HTTP {r.status_code} for {url} body={preview}")

            return r.text or ""
        except Exception as e:
            last_err = e
            if i == retries - 1:
                raise
            _sleep_backoff(i)
    raise last_err  # pragma: no cover


def _get_json_with_retries(url: str, headers: dict[str, str], timeout: int = 30, retries: int = 5) -> Any:
    last_err: Exception | None = None
    for i in range(retries):
        try:
            time.sleep(SEC_POLITE_SLEEP_SEC)
            r = requests.get(url, headers=headers, timeout=timeout)

            # rate limit / server errors -> retry
            if r.status_code in (429, 500, 502, 503, 504):
                _sleep_backoff(i)
                continue

            # other non-200 -> fail fast with body preview
            if r.status_code != 200:
                preview = (r.text or "")[:300].replace("\n", " ")
                raise RuntimeError(f"SEC request failed: {r.status_code} url={url} body={preview}")

            # JSON decode guard
            try:
                return r.json()
            except Exception as e:
                preview = (r.text or "")[:300].replace("\n", " ")
                raise RuntimeError(f"SEC returned non-JSON for url={url} body={preview}") from e

        except Exception as e:
            last_err = e
            if i == retries - 1:
                raise
            _sleep_backoff(i)

    raise last_err  # pragma: no cover


def _cik(cik_int: int) -> str:
    return str(int(cik_int)).zfill(10)


# -----------------------------------------------------------------------------
# Free price/macro sources
# -----------------------------------------------------------------------------
def get_stooq_prices(symbol: str) -> pd.DataFrame:
    """
    Free daily OHLCV from Stooq. For US tickers use {ticker}.us
    """
    url = f"https://stooq.com/q/d/l/?s={symbol.lower()}.us&i=d"
    df = pd.read_csv(url)
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date")
    return df


def get_fred_series(series_id: str) -> pd.DataFrame:
    """
    Free FRED csv endpoint (no key needed for this endpoint).
    If you later switch to the official API, keep this as fallback.
    """
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    df = pd.read_csv(url)
    df.columns = ["date", "value"]
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df.dropna()


# -----------------------------------------------------------------------------
# SEC data sources
# -----------------------------------------------------------------------------
def get_sec_ticker_map() -> dict[str, int]:
    """
    Returns mapping: TICKER -> CIK(int)
    """
    url = "https://www.sec.gov/files/company_tickers.json"
    data = _get_json_with_retries(url, headers=SEC_HEADERS, timeout=30, retries=5)

    out: dict[str, int] = {}
    # expected structure: {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}, ...}
    for _, v in data.items():
        t = str(v.get("ticker", "")).upper().strip()
        cik = v.get("cik_str")
        if not t or cik is None:
            continue
        try:
            out[t] = int(cik)
        except Exception:
            continue
    return out


def get_submissions(cik_int: int) -> dict:
    url = f"https://data.sec.gov/submissions/CIK{_cik(cik_int)}.json"
    return _get_json_with_retries(url, headers=SEC_HEADERS, timeout=30, retries=5)


def get_company_facts(cik_int: int) -> dict:
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{_cik(cik_int)}.json"
    return _get_json_with_retries(url, headers=SEC_HEADERS, timeout=30, retries=5)


def extract_fundamental(company_facts: dict, tags: list[str]) -> tuple[float | None, str | None, str | None]:
    """
    Extract latest value from SEC companyfacts for a list of possible tags.
    Returns (value, period_end, unit)
    """
    us_gaap = company_facts.get("facts", {}).get("us-gaap", {})
    for tag in tags:
        if tag in us_gaap:
            units = us_gaap[tag].get("units", {})
            for unit, points in units.items():
                if points:
                    last = sorted(points, key=lambda x: x.get("end", ""))[-1]
                    val = last.get("val")
                    end = last.get("end")
                    if val is None:
                        continue
                    try:
                        return float(val), str(end) if end else None, str(unit)
                    except Exception:
                        continue
    return None, None, None


def latest_filings_docs(
    cik_int: int,
    ticker: str,
    forms: tuple[str, ...] = ("10-Q", "10-K", "8-K"),
    limit: int = 6,
) -> list[FilingDoc]:
    """
    Downloads the primary document text for latest filings (forms) and extracts plain text.
    """
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

        acc_no_dash = str(acc).replace("-", "")
        # NOTE: Archives path uses *integer* cik without zero padding
        cik_path = str(int(cik_int))
        base = f"https://www.sec.gov/Archives/edgar/data/{cik_path}/{acc_no_dash}"
        filing_url = f"{base}/{primary}"

        text = ""
        try:
            html = _get_text_with_retries(filing_url, headers=SEC_HEADERS, timeout=30, retries=4)
            text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
        except Exception:
            # keep empty text; evidence miner will just find nothing
            text = ""

        docs.append(FilingDoc(ticker=ticker, form=str(form), filing_date=str(filing_date), source_url=filing_url, text=text))
        if len(docs) >= limit:
            break

    return docs


def mine_evidence(docs: list[FilingDoc], kpis: list[str]) -> list[dict]:
    """
    Naive keyword window miner.
    Returns snippet rows with provenance (url, retrieved_at).
    """
    snippets: list[dict] = []
    for doc in docs:
        low = (doc.text or "").lower()
        if not low:
            continue
        for kpi in kpis:
            key = kpi.lower().strip()
            if not key:
                continue
            # capture small context window around keyword
            for m in re.finditer(rf"(.{{0,140}}\b{re.escape(key)}\b.{{0,220}})", low):
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
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
