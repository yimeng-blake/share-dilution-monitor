"""Scheduled ingestion script for Share Dilution Monitor.

Runs via GitHub Actions daily cron. Re-ingests all tracked companies
to pick up newly filed 10-K/10-Q data. Uses MERGE-style inserts so
duplicate frames are skipped automatically.

Usage:
    python scheduled_ingest.py              # ingest all tracked companies
    python scheduled_ingest.py AAPL MSFT    # ingest specific tickers
"""

import sys
import os
import logging
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from services import snowflake_dilution as sf
from services import sec_xbrl as edgar

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

MONITOR_KEY = "dilution_monitor"


def ingest_ticker(ticker: str) -> bool:
    """Ingest a single ticker. Returns True on success."""
    logger.info(f"--- Ingesting {ticker} ---")

    ingestion = sf.get_ingestion_status(ticker)
    if ingestion:
        cik = ingestion["CIK"]
    else:
        company = edgar.resolve_ticker_to_cik(ticker)
        if not company:
            logger.error(f"Could not resolve CIK for {ticker}")
            return False
        cik = company["cik"]

    try:
        data = edgar.ingest_company(cik, ticker)

        d_count = sf.insert_diluted_shares(data["diluted_shares"])
        b_count = sf.insert_basic_shares(data["basic_shares"])
        ba_count = sf.insert_buyback_activity(data["buyback_activity"])
        bp_count = sf.insert_buyback_programs(data["buyback_programs"])

        total_new = d_count + b_count + ba_count + bp_count
        entity_name = (
            data["diluted_shares"][0].get("entity_name", ticker)
            if data["diluted_shares"]
            else ticker
        )

        sf.upsert_ingestion_log(ticker, cik, entity_name, status="SUCCESS")
        logger.info(
            f"  {ticker}: {total_new} new data points "
            f"(diluted={d_count}, basic={b_count}, "
            f"buyback_activity={ba_count}, buyback_programs={bp_count})"
        )
        return True

    except Exception as e:
        logger.error(f"  {ticker}: ingestion failed - {e}")
        entity_name = ingestion.get("ENTITY_NAME", ticker) if ingestion else ticker
        sf.upsert_ingestion_log(
            ticker, cik, entity_name,
            status="FAILED", error_message=str(e)[:2000],
        )
        return False


def process_queue():
    """Check the INGESTION_QUEUE for pending requests and process them."""
    queued = sf.claim_pending_ingestions(MONITOR_KEY)
    if not queued:
        return
    logger.info(f"Processing {len(queued)} queued ingestion request(s)")
    for row in queued:
        ticker = row["TICKER"]
        queue_id = row["ID"]
        try:
            ok = ingest_ticker(ticker)
            sf.complete_queued_ingestion(
                queue_id,
                status="COMPLETED" if ok else "FAILED",
                error_message=None if ok else "ingest_ticker returned False",
            )
        except Exception as e:
            logger.error(f"[{ticker}] Queued ingestion failed: {e}")
            sf.complete_queued_ingestion(
                queue_id, status="FAILED", error_message=str(e)[:2000],
            )


def main():
    specific_tickers = [t.upper() for t in sys.argv[1:]] if len(sys.argv) > 1 else None

    # Always process the cross-app queue first
    process_queue()

    if specific_tickers:
        tickers = specific_tickers
        logger.info(f"Ingesting specific tickers: {tickers}")
    else:
        # Get all tracked tickers from ingestion log + centralised watchlist
        ingested = sf.get_all_ingested_tickers()
        ingested_tickers = {r["TICKER"] for r in ingested}

        watchlist = sf.get_watchlist()
        watchlist_tickers = {r["TICKER"] for r in watchlist}

        tickers = sorted(ingested_tickers | watchlist_tickers)
        logger.info(
            f"Ingesting {len(tickers)} tickers "
            f"({len(ingested_tickers)} from log, "
            f"{len(watchlist_tickers)} from watchlist)"
        )

    if not tickers:
        logger.info("No tickers to ingest. Exiting.")
        return

    success = 0
    failed = 0
    start = time.time()

    for ticker in tickers:
        if ingest_ticker(ticker):
            success += 1
        else:
            failed += 1

    elapsed = time.time() - start
    logger.info(
        f"Done. {success} succeeded, {failed} failed, "
        f"{elapsed:.1f}s elapsed."
    )

    sf.close_session()

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
