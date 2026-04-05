"""Ingest SEC filing documents into the SEC_FILINGS data lake.

Usage:
    python ingest_filings.py NOW          # single ticker
    python ingest_filings.py ALL          # all ingested tickers
"""

import sys
import os
import logging
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from services import sec_xbrl as edgar
from services import sec_filings as sf_lake
from services import snowflake_dilution as sf

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def ingest_filings_for_ticker(ticker: str) -> dict:
    """Fetch and store all post-IPO 10-K/10-Q filings for a ticker.

    Returns dict with counts: total, inserted, skipped, failed.
    """
    # Resolve CIK
    company = edgar.resolve_ticker_to_cik(ticker)
    if not company:
        logger.error(f"Could not resolve ticker {ticker}")
        return {"total": 0, "inserted": 0, "skipped": 0, "failed": 0}

    cik = company["cik"]
    entity_name = company["name"]

    # Get IPO date for filtering
    ipo_date = sf.get_ipo_date(ticker)
    logger.info(f"Ticker={ticker}, CIK={cik}, IPO date={ipo_date}")

    # Get filing list from EDGAR
    filings = edgar.get_filing_list(cik, after_date=ipo_date)
    logger.info(f"Found {len(filings)} post-IPO 10-K/10-Q filings for {ticker}")

    stats = {"total": len(filings), "inserted": 0, "skipped": 0, "failed": 0}

    for i, filing in enumerate(filings):
        accession = filing["accession_number"]
        form = filing["form_type"]
        report_date = filing.get("report_date", "")
        primary_doc = filing["primary_document"]

        # Skip if already in data lake
        if sf_lake.filing_exists(cik, accession):
            logger.info(f"  [{i+1}/{len(filings)}] SKIP {form} {report_date} (already ingested)")
            stats["skipped"] += 1
            continue

        # Fetch full document text
        logger.info(f"  [{i+1}/{len(filings)}] Fetching {form} {report_date} ({accession})...")
        text = edgar.fetch_filing_text(cik, accession, primary_doc)

        if not text:
            logger.warning(f"  [{i+1}/{len(filings)}] FAILED to fetch text for {accession}")
            stats["failed"] += 1
            continue

        # Build filing URL
        accession_clean = accession.replace("-", "")
        filing_url = (
            f"https://www.sec.gov/Archives/edgar/data/"
            f"{cik.zfill(10)}/{accession_clean}/{primary_doc}"
        )

        # Store in data lake
        inserted = sf_lake.insert_filing(
            cik=cik,
            ticker=ticker.upper(),
            entity_name=entity_name,
            accession_number=accession,
            form_type=form,
            filed_date=filing.get("filed_date"),
            report_date=report_date,
            primary_document=primary_doc,
            filing_url=filing_url,
            document_text=text,
        )

        if inserted:
            logger.info(
                f"  [{i+1}/{len(filings)}] OK {form} {report_date} "
                f"({len(text):,} chars)"
            )
            stats["inserted"] += 1
        else:
            stats["skipped"] += 1

    return stats


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python ingest_filings.py <TICKER|ALL>")
        sys.exit(1)

    target = sys.argv[1].upper()

    if target == "ALL":
        tickers = [r["TICKER"] for r in sf.get_all_ingested_tickers()]
        logger.info(f"Ingesting filings for {len(tickers)} tickers: {tickers}")
    else:
        tickers = [target]

    for ticker in tickers:
        logger.info(f"\n{'='*60}\nIngesting filings for {ticker}\n{'='*60}")
        t0 = time.time()
        stats = ingest_filings_for_ticker(ticker)
        elapsed = time.time() - t0
        logger.info(
            f"Done {ticker}: {stats['inserted']} inserted, "
            f"{stats['skipped']} skipped, {stats['failed']} failed "
            f"(of {stats['total']} total) in {elapsed:.1f}s"
        )
