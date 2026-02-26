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
# HTTP / polite settings
# -----------------------------------------------------------------------------
DEFAULT_UA = os.getenv("HTTP_USER_AGENT", "tenbagger-lab (contact: research@local)")
HTTP_HEADERS = {
    "User-Agent": DEFAULT_UA,
    "Accept": "application/json,text/plain,text/csv,*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

POLITE_SLEEP_SEC = float(os.getenv("POLITE_SLEEP_SEC", "0.15"))

# -----------------------------------------------------------------------------
# SEC settings
# -----------------------------------------------------------------------------
SEC_UA = os.getenv("SEC_USER_AGENT", "tenbagger-lab (contact: research@local)")
SEC_HEADERS = {
    "User-Agent": SEC_UA,
    "Accept": "application/json,text/plain,*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}
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
    wait = (2**attempt) * 0.7 + random.uniform(0.0, 0.8)
    time.sleep(wait)


def _get_text_with_retries(url: str, headers: dict[str, str], timeout: int = 30, retries: int = 5, polite_sleep: float = 0.0) -> str:
    last_err: Exception | None = None
    for i in range(retries):
        try:
            if polite_sleep:
                time.sleep(polite_sleep)
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


def _get_json_with_retries(url: str, headers: dict[str, str], timeout: int = 30, retries: int = 5, polite_sleep: float = 0.0) -> Any:
    last_err: Exception | None = None
    for i in range(retries):
        try:
            if polite_sleep:
                time.sleep(polite_sleep)
            r = requests.get(url, headers=headers, timeout=timeout)

            if r.status_code in (429, 500, 502, 503, 504):
                _sleep_backoff(i)
                continue

            if r.status_code != 200:
                preview = (r.text or "")[:300].replace("\n", " ")
                raise RuntimeError(f"Request failed: {r.status_code} url={url} body={preview}")

            try:
                return r.json()
            except Exception as e:
                preview = (r.text or "")[:300].replace("\n", " ")
                raise RuntimeError(f"Non-JSON response for url={url} body={preview}") from e
        except Exception as e:
            last_err = e
            if i == retries - 1:
                raise
            _sleep_backoff(i)
    raise last_err  # pragma: no cover


def _cik(cik_int: int) -> str:
    return str(int(cik_int)).zfill(10)


def _normalize_price_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize price dataframe to columns: Date, Open, High, Low, Close, Volume (as available).
    Stooq usually returns: Date, Open, High, Low, Close, Volume
    But if blocked, it may return HTML/text.
    """
    # Trim column whitespace just in case
    df.columns = [str(c).strip() for c in df.columns]

    # Common variants
    if "Date" not in df.columns:
        # sometimes lowercase or different label
        for cand in ["date", "DATE", "<DATE>"]:
            if cand in df.columns:
                df = df.rename(columns={cand: "Date"})
                break

    if "Date" not in df.columns:
        raise RuntimeError(f"Price CSV missing Date column. Columns={list(df.columns)[:20]}")

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"]).sort_values("Date")

    return df


# -----------------------------------------------------------------------------
# Free price/macro sources
# -----------------------------------------------------------------------------
def get_stooq_prices(symbol: str) -> pd.DataFrame:
    """
    Free daily OHLCV from Stooq.

    IMPORTANT:
    - Stooq may rate-limit/bot-block CI environments and return HTML or a different CSV.
    - We validate the response and raise a clear error if it's not a proper CSV.

    For US tickers, use {ticker}.us (Stooq convention).
    """
    url = f"https://stooq.com/q/d/l/?s={symbol.lower()}.us&i=d"

    # fetch text first to validate it's CSV-like
    text = _get_text_with_retries(url, headers=HTTP_HEADERS, timeout=30, retries=4, polite_sleep=POLITE_SLEEP_SEC)

    # quick detection: if it's HTML, stop early with preview
    head = (text or "").lstrip()[:50].lower()
    if head.startswith("<!doctype") or head.startswith("<html") or "<html" in head:
        preview = (text or "")[:300].replace("\n", " ")
        raise RuntimeError(f"Stooq returned HTML (blocked?). url={url} body={preview}")

    # parse CSV from text
    from io import StringIO
    df = pd.read_csv(StringIO(text))

    # normalize and validate
    df = _normalize_price_df(df)
    return df


def get_fred_series(series_id: str) -> pd.DataFrame:
    """
    Free FRED csv endpoint (no key needed).
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
    url = "https://www.sec.gov/files/company_tickers.json"
    data = _get_json_with_retries(url, headers=SEC_HEADERS, timeout=30, retries=5, polite_sleep=SEC_POLITE_SLEEP_SEC)

    out: dict[str, int] = {}
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
    return _get_json_with_retries(url, headers=SEC_HEADERS, timeout=30, retries=5, polite_sleep=SEC_POLITE_SLEEP_SEC)


def get_company_facts(cik_int: int) -> dict:
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{_cik(cik_int)}.json"
    return _get_json_with_retries(url, headers=SEC_HEADERS, timeout=30, retries=5, polite_sleep=SEC_POLITE_SLEEP_SEC)


def extract_fundamental(company_facts: dict, tags: list[str]) -> tuple[float | None, str | None, str | None]:
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
        cik_path = str(int(cik_int))  # archives path uses integer cik
        base = f"https://www.sec.gov/Archives/edgar/data/{cik_path}/{acc_no_dash}"
        filing_url = f"{base}/{primary}"

        text = ""
        try:
            html = _get_text_with_retries(filing_url, headers=SEC_HEADERS, timeout=30, retries=4, polite_sleep=SEC_POLITE_SLEEP_SEC)
            text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
        except Exception:
            text = ""

        docs.append(FilingDoc(ticker=ticker, form=str(form), filing_date=str(filing_date), source_url=filing_url, text=text))
        if len(docs) >= limit:
            break

    return docs


def mine_evidence(docs: list[FilingDoc], kpis: list[str]) -> list[dict]:
    snippets: list[dict] = []
    for doc in docs:
        low = (doc.text or "").lower()
        if not low:
            continue
        for kpi in kpis:
            key = kpi.lower().strip()
            if not key:
                continue
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
