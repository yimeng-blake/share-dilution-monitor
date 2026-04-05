"""SEC EDGAR XBRL Company Concept API client.

Fetches structured financial data (diluted shares, buyback activity) from the
SEC's pre-parsed XBRL dataset. Each API call returns the full history of a
specific XBRL tag for a company -- no HTML parsing needed.

Design: one-time full ingestion per company (~6 API calls), then incremental
updates only insert new quarterly data points (deduped by frame).
"""

import logging
import time
from typing import Optional

import requests

from config import settings

logger = logging.getLogger(__name__)

_last_request_time: float = 0.0

# Process-lifetime cache for ticker -> CIK mapping
_ticker_cik_cache: dict[str, dict] = {}

# XBRL tags we fetch per company
DILUTED_SHARES_TAGS = [
    ("us-gaap", "WeightedAverageNumberOfDilutedSharesOutstanding"),
]
BASIC_SHARES_TAGS = [
    ("us-gaap", "CommonStockSharesOutstanding"),
    ("dei", "EntityCommonStockSharesOutstanding"),
]
BUYBACK_SHARES_TAGS = [
    ("us-gaap", "StockRepurchasedAndRetiredDuringPeriodShares"),
    ("us-gaap", "StockRepurchasedDuringPeriodShares"),
]
BUYBACK_VALUE_TAGS = [
    ("us-gaap", "StockRepurchasedAndRetiredDuringPeriodValue"),
    ("us-gaap", "StockRepurchasedDuringPeriodValue"),
]
BUYBACK_AUTH_TAGS = [
    ("us-gaap", "StockRepurchaseProgramAuthorizedAmount1"),
]


def _rate_limited_get(url: str, max_retries: int = 3) -> requests.Response:
    """Make a GET request respecting SEC EDGAR rate limits with retry logic."""
    global _last_request_time

    headers = {
        "User-Agent": settings.SEC_EDGAR_USER_AGENT,
        "Accept-Encoding": "gzip, deflate",
    }

    for attempt in range(max_retries):
        elapsed = time.time() - _last_request_time
        wait = settings.SEC_EDGAR_RATE_LIMIT - elapsed
        if wait > 0:
            time.sleep(wait)

        try:
            resp = requests.get(url, headers=headers, timeout=30)
            _last_request_time = time.time()

            if resp.status_code == 200:
                return resp

            if resp.status_code == 404:
                return resp  # let caller handle missing tags

            if resp.status_code in (403, 429, 500, 502, 503, 504):
                backoff = (2 ** attempt) * 1.0
                logger.warning(
                    f"EDGAR returned {resp.status_code} for {url}, "
                    f"retrying in {backoff}s (attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(backoff)
                continue

            resp.raise_for_status()

        except requests.exceptions.Timeout:
            backoff = (2 ** attempt) * 1.0
            logger.warning(f"Timeout fetching {url}, retrying in {backoff}s")
            time.sleep(backoff)
            continue
        except requests.exceptions.ConnectionError:
            backoff = (2 ** attempt) * 2.0
            logger.warning(f"Connection error for {url}, retrying in {backoff}s")
            time.sleep(backoff)
            continue

    # Final attempt
    resp = requests.get(url, headers=headers, timeout=30)
    _last_request_time = time.time()
    resp.raise_for_status()
    return resp


def resolve_ticker_to_cik(ticker: str) -> Optional[dict]:
    """Resolve a ticker to CIK and company info via EDGAR.

    Returns dict with keys: cik, name, ticker, exchange, sic
    or None if not found.
    """
    ticker_upper = ticker.upper()
    if ticker_upper in _ticker_cik_cache:
        return _ticker_cik_cache[ticker_upper]

    cik_str = _search_cik_for_ticker(ticker_upper)
    if not cik_str:
        return None

    try:
        url = f"{settings.SEC_EDGAR_BASE_URL}/submissions/CIK{cik_str}.json"
        resp = _rate_limited_get(url)
        if resp.status_code != 200:
            return None
        data = resp.json()

        # Determine IPO quarter: earliest 10-Q report date across all filing pages
        ipo_date = _find_first_10q_date(data)

        result = {
            "cik": data["cik"],
            "name": data.get("name", ""),
            "ticker": ticker_upper,
            "exchange": (data.get("exchanges") or [""])[0],
            "sic": data.get("sic", ""),
            "ipo_date": ipo_date,
        }
        _ticker_cik_cache[ticker_upper] = result
        return result
    except Exception as e:
        logger.error(f"Failed to resolve CIK for {ticker_upper}: {e}")
        return None


def _find_first_10q_date(submissions_data: dict) -> Optional[str]:
    """Find the earliest 10-Q report date from EDGAR submissions.

    This serves as a proxy for IPO date — companies only file 10-Qs
    after going public. Returns date string (YYYY-MM-DD) or None.
    """
    all_report_dates = []

    # Check recent filings
    recent = submissions_data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    dates = recent.get("reportDate", [])
    for f, d in zip(forms, dates):
        if f == "10-Q" and d:
            all_report_dates.append(d)

    # Check older filing pages
    older_files = submissions_data.get("filings", {}).get("files", [])
    for file_info in older_files:
        try:
            fname = file_info.get("name", "")
            url = f"{settings.SEC_EDGAR_BASE_URL}/submissions/{fname}"
            resp = _rate_limited_get(url)
            if resp.status_code != 200:
                continue
            page = resp.json()
            forms = page.get("form", [])
            dates = page.get("reportDate", [])
            for f, d in zip(forms, dates):
                if f == "10-Q" and d:
                    all_report_dates.append(d)
        except Exception:
            continue

    if all_report_dates:
        all_report_dates.sort()
        return all_report_dates[0]
    return None


def _search_cik_for_ticker(ticker: str) -> Optional[str]:
    """Look up CIK for a ticker using EDGAR company tickers JSON."""
    if not hasattr(_search_cik_for_ticker, "_data"):
        url = "https://www.sec.gov/files/company_tickers.json"
        resp = _rate_limited_get(url)
        _search_cik_for_ticker._data = resp.json()

    for entry in _search_cik_for_ticker._data.values():
        if entry.get("ticker", "").upper() == ticker.upper():
            return str(entry["cik_str"]).zfill(10)
    return None


def _fetch_company_concept(cik: str, taxonomy: str, tag: str) -> Optional[dict]:
    """Fetch a single XBRL concept for a company. Returns parsed JSON or None."""
    cik_padded = cik.zfill(10)
    url = (
        f"{settings.SEC_EDGAR_BASE_URL}/api/xbrl/companyconcept/"
        f"CIK{cik_padded}/{taxonomy}/{tag}.json"
    )
    resp = _rate_limited_get(url)
    if resp.status_code == 404:
        logger.info(f"Tag {taxonomy}/{tag} not found for CIK {cik}")
        return None
    if resp.status_code != 200:
        logger.warning(f"Unexpected status {resp.status_code} for {url}")
        return None
    return resp.json()


def _extract_quarterly_points(concept_data: dict) -> list[dict]:
    """Extract quarterly data points from a company concept response.

    Filters to 10-K and 10-Q forms, deduplicates by frame field,
    and returns only quarterly duration or instant data.
    """
    if not concept_data:
        return []

    entity_name = concept_data.get("entityName", "")
    units = concept_data.get("units", {})
    points = []
    seen_frames = set()

    # XBRL data can be in "USD", "shares", or "pure" units
    # First pass: collect all native frames to know which Q4 frames exist
    all_entries = []
    native_frames = set()
    for unit_key, entries in units.items():
        for entry in entries:
            form = entry.get("form", "")
            if form not in ("10-K", "10-Q", "10-K/A", "10-Q/A"):
                continue
            frame = entry.get("frame")
            if frame:
                all_entries.append(entry)
                native_frames.add(frame)

    # Second pass: build data points, re-tagging annual frames as Q4
    for entry in all_entries:
        frame = entry.get("frame")
        form = entry.get("form", "")

        # Keep quarterly frames (CY2024Q1, CY2024Q1I, etc.)
        # For annual frames (CY2024, CY2025 — no Q), include them only
        # from 10-K filings as a Q4 proxy so we don't lose the latest
        # year-end data point.  Re-tag the frame to CYxxxxQ4 so it
        # slots naturally into the quarterly timeline.
        #
        # IMPORTANT: Only re-tag instant concepts (no "start" date).
        # Duration concepts (e.g., WeightedAverageNumberOfDilutedSharesOutstanding)
        # report the full-year value in annual frames, which is NOT a Q4
        # value and would create massive spikes if mixed into quarterly data.
        if "Q" not in frame:
            if form not in ("10-K", "10-K/A"):
                continue
            # Duration concept — annual value != Q4 value, skip
            if entry.get("start"):
                continue
            # Skip if a native Q4 frame already exists for this year
            # (e.g. CY2025Q4I for instant concepts)
            q4_tag = frame + "Q4"
            q4i_tag = frame + "Q4I"
            if q4_tag in native_frames or q4i_tag in native_frames:
                continue
            frame = q4_tag

        # Deduplicate by frame
        if frame in seen_frames:
            continue
        seen_frames.add(frame)

        points.append({
            "entity_name": entity_name,
            "val": entry.get("val"),
            "period_start": entry.get("start"),
            "period_end": entry.get("end"),
            "fiscal_year": entry.get("fy"),
            "fiscal_period": entry.get("fp"),
            "form_type": form,
            "filed_date": entry.get("filed"),
            "accession_number": entry.get("accn"),
            "frame": frame,
        })

    # Sort by period_end
    points.sort(key=lambda p: p.get("period_end") or "")
    return points


def _fetch_best_concept(cik: str, tag_list: list[tuple]) -> list[dict]:
    """Try multiple XBRL tags in priority order, return first non-empty result."""
    for taxonomy, tag in tag_list:
        data = _fetch_company_concept(cik, taxonomy, tag)
        points = _extract_quarterly_points(data)
        if points:
            logger.info(f"Found {len(points)} points for {taxonomy}/{tag}")
            return points
    return []


def fetch_diluted_shares(cik: str) -> list[dict]:
    """Fetch quarterly weighted-average diluted shares outstanding."""
    points = _fetch_best_concept(cik, DILUTED_SHARES_TAGS)
    return [
        {**p, "diluted_shares": int(p["val"])}
        for p in points if p.get("val") is not None
    ]


def fetch_basic_shares(cik: str) -> list[dict]:
    """Fetch quarterly basic shares outstanding.

    Tries CommonStockSharesOutstanding first, then EntityCommonStockSharesOutstanding.
    These are instant (point-in-time) values, but we still filter to quarterly frames.
    """
    points = _fetch_best_concept(cik, BASIC_SHARES_TAGS)
    return [
        {**p, "basic_shares": int(p["val"])}
        for p in points if p.get("val") is not None
    ]


def fetch_buyback_shares(cik: str) -> list[dict]:
    """Fetch shares repurchased per period."""
    points = _fetch_best_concept(cik, BUYBACK_SHARES_TAGS)
    return [
        {**p, "shares_repurchased": int(p["val"])}
        for p in points if p.get("val") is not None
    ]


def fetch_buyback_value(cik: str) -> list[dict]:
    """Fetch dollar value of share repurchases per period."""
    points = _fetch_best_concept(cik, BUYBACK_VALUE_TAGS)
    return [
        {**p, "repurchase_value_usd": float(p["val"])}
        for p in points if p.get("val") is not None
    ]


def fetch_buyback_authorization(cik: str) -> list[dict]:
    """Fetch authorized buyback program amounts."""
    points = _fetch_best_concept(cik, BUYBACK_AUTH_TAGS)
    return [
        {**p, "authorized_amount_usd": float(p["val"])}
        for p in points if p.get("val") is not None
    ]


def derive_quarterly_buybacks(
    buyback_shares: list[dict], buyback_value: list[dict]
) -> list[dict]:
    """Combine buyback shares and values into unified records.

    SEC buyback data can be cumulative (YTD). When we detect cumulative
    patterns (values increasing within a fiscal year and resetting), we
    diff consecutive periods to get per-quarter values.
    """
    # Index value data by frame for merging
    value_by_frame = {
        r["frame"]: r.get("repurchase_value_usd", 0) for r in buyback_value
    }

    results = []
    for rec in buyback_shares:
        frame = rec["frame"]
        result = {
            "entity_name": rec.get("entity_name", ""),
            "period_start": rec.get("period_start"),
            "period_end": rec.get("period_end"),
            "fiscal_year": rec.get("fiscal_year"),
            "fiscal_period": rec.get("fiscal_period"),
            "form_type": rec.get("form_type"),
            "filed_date": rec.get("filed_date"),
            "accession_number": rec.get("accession_number"),
            "frame": frame,
            "shares_repurchased": rec.get("shares_repurchased", 0),
            "repurchase_value_usd": value_by_frame.get(frame, 0),
        }
        results.append(result)

    # Also include value-only records (where shares tag was missing)
    shares_frames = {r["frame"] for r in buyback_shares}
    for rec in buyback_value:
        if rec["frame"] not in shares_frames:
            results.append({
                "entity_name": rec.get("entity_name", ""),
                "period_start": rec.get("period_start"),
                "period_end": rec.get("period_end"),
                "fiscal_year": rec.get("fiscal_year"),
                "fiscal_period": rec.get("fiscal_period"),
                "form_type": rec.get("form_type"),
                "filed_date": rec.get("filed_date"),
                "accession_number": rec.get("accession_number"),
                "frame": rec["frame"],
                "shares_repurchased": 0,
                "repurchase_value_usd": rec.get("repurchase_value_usd", 0),
            })

    results.sort(key=lambda r: r.get("period_end") or "")
    return results


def get_filing_list(
    cik: str,
    form_types: tuple[str, ...] = ("10-K", "10-Q"),
    after_date: Optional[str] = None,
) -> list[dict]:
    """Get list of filings for a company from EDGAR submissions.

    Returns list of dicts with keys:
        accession_number, form_type, filed_date, report_date,
        primary_document, fiscal_year, fiscal_period

    If after_date is provided (YYYY-MM-DD), only filings with
    reportDate >= after_date are returned.
    """
    cik_padded = cik.zfill(10)
    url = f"{settings.SEC_EDGAR_BASE_URL}/submissions/CIK{cik_padded}.json"
    resp = _rate_limited_get(url)
    if resp.status_code != 200:
        logger.error(f"Failed to fetch submissions for CIK {cik}: {resp.status_code}")
        return []
    data = resp.json()

    entity_name = data.get("name", "")
    filings = []

    def _extract_filings(recent: dict):
        forms = recent.get("form", [])
        accessions = recent.get("accessionNumber", [])
        filed_dates = recent.get("filingDate", [])
        report_dates = recent.get("reportDate", [])
        primary_docs = recent.get("primaryDocument", [])

        for i, form in enumerate(forms):
            if form not in form_types:
                continue
            report_date = report_dates[i] if i < len(report_dates) else None
            if after_date and report_date and report_date < after_date:
                continue
            filings.append({
                "accession_number": accessions[i] if i < len(accessions) else "",
                "form_type": form,
                "filed_date": filed_dates[i] if i < len(filed_dates) else None,
                "report_date": report_date,
                "primary_document": primary_docs[i] if i < len(primary_docs) else "",
                "entity_name": entity_name,
            })

    # Recent filings
    recent = data.get("filings", {}).get("recent", {})
    _extract_filings(recent)

    # Older filing pages
    older_files = data.get("filings", {}).get("files", [])
    for file_info in older_files:
        try:
            fname = file_info.get("name", "")
            page_url = f"{settings.SEC_EDGAR_BASE_URL}/submissions/{fname}"
            page_resp = _rate_limited_get(page_url)
            if page_resp.status_code != 200:
                continue
            _extract_filings(page_resp.json())
        except Exception:
            continue

    # Sort by report_date ascending
    filings.sort(key=lambda f: f.get("report_date") or "")
    return filings


def fetch_filing_text(cik: str, accession_number: str, primary_document: str) -> Optional[str]:
    """Fetch the full text of a filing document from EDGAR.

    Downloads the primary document (usually .htm) and strips HTML tags
    to produce plain text suitable for LLM processing.

    Returns plain text or None on failure.
    """
    import re
    from html import unescape

    cik_padded = cik.zfill(10)
    accession_clean = accession_number.replace("-", "")
    url = (
        f"https://www.sec.gov/Archives/edgar/data/"
        f"{cik_padded}/{accession_clean}/{primary_document}"
    )

    try:
        resp = _rate_limited_get(url)
        if resp.status_code != 200:
            logger.warning(f"Failed to fetch filing {accession_number}: {resp.status_code}")
            return None

        html = resp.text

        # Strip HTML to plain text
        # Remove script/style blocks
        text = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', html, flags=re.DOTALL | re.IGNORECASE)
        # Remove XBRL inline tags but keep content
        text = re.sub(r'</?ix:[^>]*>', '', text)
        # Remove all other HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)
        # Decode HTML entities
        text = unescape(text)
        # Collapse whitespace
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n\s*\n', '\n\n', text)
        text = text.strip()

        return text

    except Exception as e:
        logger.error(f"Error fetching filing {accession_number}: {e}")
        return None


def ingest_company(cik: str, ticker: str) -> dict:
    """Full ingestion for a single company. Returns all fetched data.

    Makes ~6 API calls to EDGAR (one per XBRL tag). Returns dict with keys:
    diluted_shares, basic_shares, buyback_activity, buyback_programs
    """
    logger.info(f"Starting ingestion for {ticker} (CIK: {cik})")

    diluted = fetch_diluted_shares(cik)
    logger.info(f"  Diluted shares: {len(diluted)} quarterly points")

    basic = fetch_basic_shares(cik)
    logger.info(f"  Basic shares: {len(basic)} quarterly points")

    bb_shares = fetch_buyback_shares(cik)
    bb_value = fetch_buyback_value(cik)
    buyback_activity = derive_quarterly_buybacks(bb_shares, bb_value)
    logger.info(f"  Buyback activity: {len(buyback_activity)} quarterly points")

    buyback_programs = fetch_buyback_authorization(cik)
    logger.info(f"  Buyback programs: {len(buyback_programs)} authorization records")

    # Attach ticker and cik to all records
    for rec in diluted + basic + buyback_activity + buyback_programs:
        rec["ticker"] = ticker
        rec["cik"] = cik

    return {
        "diluted_shares": diluted,
        "basic_shares": basic,
        "buyback_activity": buyback_activity,
        "buyback_programs": buyback_programs,
    }
