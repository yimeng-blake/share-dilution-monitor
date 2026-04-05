"""Analysis and derived metrics for the Dilution Monitor.

Computes:
- Stock split detection and normalization
- Quarter-over-quarter diluted share change (absolute + %)
- Buyback program remaining (V1: authorization minus cumulative spend)
- Summary statistics for display
"""

import pandas as pd
from typing import Optional


# ---------------------------------------------------------------------------
# Stock split detection and normalization
# ---------------------------------------------------------------------------

# Common forward split ratios and their approximate QoQ multiplier
_KNOWN_SPLIT_RATIOS = [2, 3, 4, 5, 6, 7, 8, 10]
_SPLIT_TOLERANCE = 0.20  # ±20% to account for buybacks/issuance around split date


def detect_splits(
    dates: pd.Series, values: pd.Series, min_ratio: float = 1.5,
) -> list[dict]:
    """Detect stock splits from a time-sorted share count series.

    Scans consecutive quarters for jumps where the ratio is close to a common
    split factor (2:1 through 10:1). Returns a list of detected splits, each
    with keys: index, date, ratio (the integer split factor).

    The series must already be sorted by date ascending.
    """
    splits = []
    for i in range(1, len(values)):
        prev = values.iloc[i - 1]
        curr = values.iloc[i]
        if prev <= 0 or curr <= 0:
            continue

        raw_ratio = curr / prev

        # Check forward splits (share count increases by a factor)
        if raw_ratio >= min_ratio:
            best_factor = None
            best_err = float("inf")
            for factor in _KNOWN_SPLIT_RATIOS:
                err = abs(raw_ratio - factor) / factor
                if err < best_err:
                    best_err = err
                    best_factor = factor
            if best_err <= _SPLIT_TOLERANCE:
                splits.append({
                    "index": i,
                    "date": dates.iloc[i],
                    "ratio": best_factor,
                })

        # Check reverse splits (share count decreases by a factor)
        elif raw_ratio <= (1.0 / min_ratio):
            inv = 1 / raw_ratio
            best_factor = None
            best_err = float("inf")
            for factor in _KNOWN_SPLIT_RATIOS:
                err = abs(inv - factor) / factor
                if err < best_err:
                    best_err = err
                    best_factor = factor
            if best_err <= _SPLIT_TOLERANCE:
                splits.append({
                    "index": i,
                    "date": dates.iloc[i],
                    "ratio": round(1.0 / best_factor, 4),
                })

    return splits


def adjust_for_splits(
    df: pd.DataFrame, share_col: str, date_col: str = "period_end",
    known_splits: Optional[list] = None, min_ratio: float = 1.5,
) -> tuple[pd.DataFrame, list[dict]]:
    """Normalize a share-count column to the latest (current) split-adjusted basis.

    If known_splits is provided, those are used directly (date-based matching).
    Otherwise, detects splits from the data automatically.

    After initial adjustment, runs an outlier correction pass to fix values that
    are still dramatically out of scale (e.g., 10-K restatements mixed with
    un-restated 10-Q filings, or XBRL filing errors).

    Returns (adjusted_df, detected_splits).
    Dollar-denominated columns are never touched.
    """
    if df.empty or share_col not in df.columns:
        return df, []

    df = df.copy()

    if known_splits is not None:
        splits = known_splits
    else:
        splits = detect_splits(df[date_col], df[share_col], min_ratio=min_ratio)

    forward_splits = [s for s in splits if s.get("ratio", 0) > 1]

    if not forward_splits:
        # Even without splits, fix scale outliers (e.g., Cortex AI values
        # missing the *1000 multiplier).
        df = _fix_scale_outliers(df, share_col)
        return df, []

    # Build a multiplier array: everything before a split gets multiplied
    # by that split's ratio (to bring it up to post-split scale).
    multipliers = pd.Series(1.0, index=df.index)

    if known_splits is not None:
        # Date-based matching for externally provided splits
        for split in sorted(forward_splits, key=lambda s: s["date"], reverse=True):
            split_date = pd.to_datetime(split["date"])
            mask = df[date_col] < split_date
            multipliers[mask] *= split["ratio"]
    else:
        # Index-based matching for auto-detected splits
        for split in sorted(forward_splits, key=lambda s: s["index"], reverse=True):
            multipliers.iloc[: split["index"]] *= split["ratio"]

    df[share_col] = (df[share_col] * multipliers).round(0).astype(int)

    # Outlier correction: fix values that are still dramatically out of scale.
    # This handles mixed restated/non-restated filings around split dates and
    # XBRL data errors (e.g., missing "000" suffix).
    df = _fix_split_outliers(df, share_col, forward_splits)
    df = _fix_scale_outliers(df, share_col)

    return df, forward_splits


def _fix_split_outliers(
    df: pd.DataFrame, share_col: str, splits: list[dict],
) -> pd.DataFrame:
    """Correct individual values that are still out of scale after split adjustment.

    After the initial blanket date-based adjustment, some values may be wrong:
    - Over-adjusted: a 10-K filing that was already restated to post-split basis
      got multiplied again (value is ~ratio× too large)
    - Under-adjusted: a 10-Q filing after the split date that wasn't restated
      didn't get multiplied (value is ~ratio× too small)
    - Data errors: XBRL value missing "000" suffix (value is ~1000× too small)

    Uses a two-phase approach:
    1. Build a smooth reference curve via rolling median of the majority-scale values
    2. Correct outliers that deviate from this reference by a known factor
    """
    if len(df) < 3 or not splits:
        return df

    df = df.copy()
    values = df[share_col].astype(float).values.copy()
    n = len(values)
    split_ratios = sorted(set(s["ratio"] for s in splits), reverse=True)

    # Build correction candidates from split ratios and their products
    correction_factors = set()
    for r in split_ratios:
        correction_factors.add(r)
    for i, r1 in enumerate(split_ratios):
        for r2 in split_ratios[i:]:
            correction_factors.add(r1 * r2)
    # Add 1000 for XBRL data errors (missing "000" suffix)
    correction_factors.add(1000)
    for r in split_ratios:
        correction_factors.add(r * 1000)
    correction_factors = sorted(correction_factors)

    max_passes = 5
    for _ in range(max_passes):
        changed = False

        # Compute a local reference for each point: median of a wide window
        # around it, excluding extreme outliers. Window size adapts to data length.
        window = max(5, n // 6)

        for i in range(n):
            start = max(0, i - window)
            end = min(n, i + window + 1)
            neighborhood = sorted(values[start:end])

            # Trim top/bottom 20% to get robust center
            trim = max(1, len(neighborhood) // 5)
            trimmed = neighborhood[trim:-trim] if len(neighborhood) > 2 * trim else neighborhood
            if not trimmed:
                continue
            local_ref = trimmed[len(trimmed) // 2]
            if local_ref <= 0:
                continue

            ratio = values[i] / local_ref

            # Value is way too small — try multiplying by a correction factor
            if ratio < 0.33:
                best_factor = None
                best_err = float("inf")
                for cf in correction_factors:
                    corrected_ratio = (values[i] * cf) / local_ref
                    err = abs(corrected_ratio - 1.0)
                    if err < best_err:
                        best_err = err
                        best_factor = cf
                if best_factor and best_err < 0.5:
                    values[i] = round(values[i] * best_factor)
                    changed = True

            # Value is way too large — try dividing by a correction factor
            elif ratio > 3.0:
                best_factor = None
                best_err = float("inf")
                for cf in correction_factors:
                    corrected_ratio = (values[i] / cf) / local_ref
                    err = abs(corrected_ratio - 1.0)
                    if err < best_err:
                        best_err = err
                        best_factor = cf
                if best_factor and best_err < 0.5:
                    values[i] = round(values[i] / best_factor)
                    changed = True

        if not changed:
            break

    df[share_col] = pd.array(values, dtype=int)
    return df


def _fix_scale_outliers(
    df: pd.DataFrame, share_col: str,
) -> pd.DataFrame:
    """Fix values that are ~1000× too small (missing thousands multiplier).

    Runs independently of split detection.  Uses a rolling median reference
    to find points that are dramatically below their neighbors and corrects
    them by multiplying by 1000.
    """
    if len(df) < 3:
        return df

    df = df.copy()
    values = df[share_col].astype(float).values.copy()
    n = len(values)
    window = max(5, n // 6)

    for _ in range(3):
        changed = False
        for i in range(n):
            start = max(0, i - window)
            end = min(n, i + window + 1)
            neighborhood = sorted(values[start:end])

            # Trim top/bottom 20% for robust center
            trim = max(1, len(neighborhood) // 5)
            trimmed = neighborhood[trim:-trim] if len(neighborhood) > 2 * trim else neighborhood
            if not trimmed:
                continue
            local_ref = trimmed[len(trimmed) // 2]
            if local_ref <= 0:
                continue

            ratio = values[i] / local_ref

            # Value is ~1000× too small
            if ratio < 0.01:
                corrected = values[i] * 1000
                if 0.5 < (corrected / local_ref) < 2.0:
                    values[i] = round(corrected)
                    changed = True
            # Value is ~1000× too large
            elif ratio > 100:
                corrected = values[i] / 1000
                if 0.5 < (corrected / local_ref) < 2.0:
                    values[i] = round(corrected)
                    changed = True

        if not changed:
            break

    df[share_col] = pd.array(values, dtype=int)
    return df


def compute_dilution_metrics(
    diluted_rows: list[dict],
    known_splits: Optional[list] = None,
) -> tuple[pd.DataFrame, list[dict]]:
    """Build a DataFrame of diluted shares with QoQ change metrics.

    If known_splits is provided (e.g. from Cortex AI filing extraction),
    those splits are applied directly instead of auto-detecting from the
    share series (which can produce false positives).

    Input: rows from DILUTED_SHARES table (list of dicts with uppercase keys).
    Returns (DataFrame, detected_splits) where DataFrame has columns:
        period_end, fiscal_year, fiscal_period, form_type,
        diluted_shares, qoq_change, qoq_change_pct
    """
    if not diluted_rows:
        return pd.DataFrame(), []

    df = pd.DataFrame(diluted_rows)
    # Normalize column names to lowercase
    df.columns = [c.lower() for c in df.columns]

    df["period_end"] = pd.to_datetime(df["period_end"])
    df = df.sort_values("period_end").reset_index(drop=True)

    # Adjust for stock splits. Use a high threshold (>= 3x) because XBRL
    # weighted-average diluted shares are partially retroactively adjusted
    # by filers, and organic post-IPO dilution can produce 2x jumps that
    # are NOT stock splits (e.g., lockup expiry, option exercises).
    df, splits = adjust_for_splits(df, "diluted_shares", known_splits=known_splits, min_ratio=3.0)

    df["qoq_change"] = df["diluted_shares"].diff()
    df["qoq_change_pct"] = df["diluted_shares"].pct_change() * 100

    return df, splits


def compute_basic_shares_metrics(
    basic_rows: list[dict],
    known_splits: Optional[list] = None,
) -> tuple[pd.DataFrame, list[dict]]:
    """Build a DataFrame of basic shares outstanding with QoQ change.

    If known_splits is provided (e.g. from diluted shares detection), those
    splits are applied directly instead of auto-detecting from basic shares
    (which can be noisy due to 10-K restatements).

    Returns (DataFrame, detected_splits).
    """
    if not basic_rows:
        return pd.DataFrame(), []

    df = pd.DataFrame(basic_rows)
    df.columns = [c.lower() for c in df.columns]

    df["period_end"] = pd.to_datetime(df["period_end"])
    df = df.sort_values("period_end").reset_index(drop=True)

    # Adjust for stock splits — use same high threshold as diluted shares
    df, splits = adjust_for_splits(df, "basic_shares", known_splits=known_splits, min_ratio=3.0)

    df["qoq_change"] = df["basic_shares"].diff()
    df["qoq_change_pct"] = df["basic_shares"].pct_change() * 100

    return df, splits


def derive_q4_from_fy(
    primary_df: pd.DataFrame,
    fy_shares: list[dict],
    share_col: str = "diluted_shares",
    known_splits: Optional[list] = None,
) -> pd.DataFrame:
    """Derive Q4 share counts from full-year (10-K) and quarterly (10-Q) data.

    Uses the Morgan Stanley methodology: FY weighted-avg = AVERAGE(Q1:Q4),
    therefore Q4 = 4 * FY - Q1 - Q2 - Q3.

    Because FY share values from 10-K filing facts are Cortex AI extractions
    of pre-split figures, they must be multiplied by any subsequent split
    ratios so they match the split-adjusted quarterly series in primary_df.

    Args:
        primary_df: DataFrame with period_end, share_col, fiscal_year, fiscal_period, form_type.
        fy_shares: List of dicts with keys fy_end (str) and fy_shares (int) from 10-K filing facts.
        share_col: Column name for the share count ("diluted_shares" or "basic_shares").
        known_splits: List of dicts with keys date (str) and ratio (int)
            from verified split data. Used to adjust pre-split FY values.

    Returns a DataFrame of derived Q4 rows (may be empty) with the same columns
    as primary_df plus is_derived=True.
    """
    if primary_df.empty or not fy_shares:
        return pd.DataFrame()

    forward_splits = []
    if known_splits:
        forward_splits = [s for s in known_splits if s.get("ratio", 0) > 1]

    derived_rows = []

    for fy in fy_shares:
        fy_end = pd.to_datetime(fy["fy_end"])
        fy_val = fy["fy_shares"]

        # Skip if this FY period_end already exists in the data (already have Q4)
        if (primary_df["period_end"] == fy_end).any():
            continue

        # Find the 3 quarterly data points in the 12 months before the FY end
        mask = (
            (primary_df["period_end"] < fy_end)
            & (primary_df["period_end"] >= fy_end - pd.DateOffset(months=12))
        )
        quarters = primary_df.loc[mask]

        if len(quarters) != 3:
            continue

        # Apply split adjustment: multiply fy_val by ratio of any splits
        # that occurred after the FY end date (FY value is pre-split,
        # quarterly values in primary_df are already split-adjusted).
        for split in forward_splits:
            if fy_end < pd.to_datetime(split["date"]):
                fy_val *= split["ratio"]

        # Auto-correct FY shares reported "in thousands" by Cortex AI.
        # SEC filings often state values "in thousands"; if the AI extracts
        # the raw number without multiplying, the FY value will be ~1000×
        # smaller than the quarterly XBRL values.
        q_median = quarters[share_col].median()
        if q_median > 0 and fy_val / q_median < 0.01:
            fy_val = fy_val * 1000

        q_sum = quarters[share_col].sum()
        derived_q4 = 4 * fy_val - q_sum

        # Safety: derived Q4 must be positive and within 70-130% of FY average.
        # Values outside this range indicate bad scale in the FY data (e.g.,
        # Cortex AI dropping the thousands multiplier).
        if derived_q4 <= 0:
            continue
        ratio = derived_q4 / fy_val
        if ratio < 0.7 or ratio > 1.3:
            continue

        # Build the derived row — infer fiscal metadata from the last known quarter
        last_q = quarters.iloc[-1]
        derived_rows.append({
            "period_end": fy_end,
            share_col: int(derived_q4),
            "fiscal_year": last_q.get("fiscal_year"),
            "fiscal_period": "Q4",
            "form_type": "Derived",
            "is_derived": True,
        })

    if not derived_rows:
        return pd.DataFrame()

    return pd.DataFrame(derived_rows)


def derive_q4_from_eps(
    primary_df: pd.DataFrame,
    q4_eps_data: list[dict],
    share_col: str = "diluted_shares",
    known_splits: Optional[list] = None,
) -> pd.DataFrame:
    """Derive Q4 share counts from Q4 net income and Q4 EPS (Morgan Stanley method).

    Computes Q4 weighted-avg diluted shares = |Q4 net income × 1000| / |Q4 EPS|.
    Net income is in thousands; EPS is per-share. Both are from the 10-K quarterly
    financial data section extracted by Cortex AI.

    Because net income and EPS in historical 10-K filings are on a pre-split basis,
    the derived share count must be multiplied by any subsequent split ratios so it
    matches the split-adjusted primary_df series.

    This is a fallback for derive_q4_from_fy when < 3 prior quarters exist
    (e.g. IPO-year companies).

    Args:
        primary_df: DataFrame with period_end, share_col columns.
        q4_eps_data: List of dicts with keys fy_end, q4_net_income, q4_eps.
        share_col: Column name for the share count.
        known_splits: List of dicts with keys date (str) and ratio (int)
            from verified split data. Used to adjust pre-split derived values.

    Returns a DataFrame of derived Q4 rows (may be empty).
    """
    if primary_df.empty or not q4_eps_data:
        return pd.DataFrame()

    # Pre-compute cumulative split multiplier for each possible date.
    # A value derived before a split needs to be multiplied by that split's ratio.
    forward_splits = []
    if known_splits:
        forward_splits = [s for s in known_splits if s.get("ratio", 0) > 1]

    derived_rows = []

    for item in q4_eps_data:
        fy_end = pd.to_datetime(item["fy_end"])
        net_income = item["q4_net_income"]  # in thousands
        eps = item["q4_eps"]

        # Skip if this period already exists in primary_df
        if (primary_df["period_end"] == fy_end).any():
            continue

        # Back-calculate: shares = |net_income_in_dollars| / |EPS|
        # net_income is in thousands, so multiply by 1000
        q4_shares = abs(net_income * 1000) / abs(eps)

        # Apply split adjustment: multiply by ratio of any splits after fy_end
        for split in forward_splits:
            if fy_end < pd.to_datetime(split["date"]):
                q4_shares *= split["ratio"]

        q4_shares = int(round(q4_shares))

        # Sanity check: shares should be positive and reasonable
        if q4_shares <= 0:
            continue

        # Check against local neighbors (2 quarters before and after fy_end)
        nearby = primary_df.loc[
            (primary_df["period_end"] >= fy_end - pd.DateOffset(months=9))
            & (primary_df["period_end"] <= fy_end + pd.DateOffset(months=9))
            & (primary_df["period_end"] != fy_end)
        ]
        if not nearby.empty:
            local_median = nearby[share_col].median()
            if local_median > 0:
                ratio = q4_shares / local_median
                if ratio < 0.3 or ratio > 3.0:
                    continue

        derived_rows.append({
            "period_end": fy_end,
            share_col: q4_shares,
            "fiscal_year": fy_end.year,
            "fiscal_period": "Q4",
            "form_type": "Derived (EPS)",
            "is_derived": True,
        })

    if not derived_rows:
        return pd.DataFrame()

    return pd.DataFrame(derived_rows)


def backfill_from_filing_facts(
    xbrl_rows: list[dict],
    ai_rows: list[dict],
    share_key: str = "DILUTED_SHARES",
) -> list[dict]:
    """Merge Cortex AI-extracted quarterly rows into XBRL rows for missing periods.

    Only adds AI rows whose PERIOD_END is not already present in xbrl_rows.
    This runs BEFORE compute_dilution_metrics, so the existing adjust_for_splits
    logic will auto-correct any *1000 scale issues in the AI data.

    Args:
        xbrl_rows: Existing rows from DILUTED_SHARES or BASIC_SHARES table.
        ai_rows: Rows from get_filing_facts_quarterly() shaped like the XBRL rows.
        share_key: "DILUTED_SHARES" or "BASIC_SHARES" — the value column name.

    Returns a new combined list (does not mutate inputs).
    """
    if not ai_rows:
        return list(xbrl_rows)

    existing_periods = {str(r.get("PERIOD_END", "")) for r in xbrl_rows}
    backfilled = list(xbrl_rows)

    for row in ai_rows:
        period = str(row.get("PERIOD_END", ""))
        if period in existing_periods:
            continue
        # Only include rows that actually have a share value
        if row.get(share_key) is None:
            continue
        backfilled.append(row)
        existing_periods.add(period)

    return backfilled


def compute_buyback_metrics(
    activity_rows: list[dict], program_rows: list[dict],
    split_events: Optional[list] = None,
) -> pd.DataFrame:
    """Build a DataFrame of buyback activity with running totals and remaining.

    V1 logic for buyback remaining:
      remaining = latest_authorization - cumulative_spend_since_that_authorization

    If split_events is provided (from diluted/basic share detection), the same
    adjustments are applied to shares_repurchased. Dollar columns are untouched.

    Returns DataFrame with columns:
        period_end, fiscal_year, fiscal_period, shares_repurchased,
        repurchase_value_usd, cumulative_shares, cumulative_value,
        authorization_usd, estimated_remaining_usd
    """
    if not activity_rows:
        return pd.DataFrame()

    df = pd.DataFrame(activity_rows)
    df.columns = [c.lower() for c in df.columns]
    df["period_end"] = pd.to_datetime(df["period_end"])
    df = df.sort_values("period_end").reset_index(drop=True)

    # Adjust buyback share counts for splits using externally detected events.
    # We can't reliably detect splits from buyback data alone (too sparse),
    # so we use the splits detected from the diluted/basic shares series.
    if split_events and "shares_repurchased" in df.columns:
        multipliers = pd.Series(1.0, index=df.index)
        for split in sorted(split_events, key=lambda s: s["date"], reverse=True):
            split_date = pd.to_datetime(split["date"])
            mask = df["period_end"] < split_date
            multipliers[mask] *= split["ratio"]
        df["shares_repurchased"] = (
            df["shares_repurchased"] * multipliers
        ).round(0).astype(int)

    # Build authorization timeline
    auth_df = None
    if program_rows:
        auth_df = pd.DataFrame(program_rows)
        auth_df.columns = [c.lower() for c in auth_df.columns]
        auth_df["period_end"] = pd.to_datetime(auth_df["period_end"])
        auth_df = auth_df.sort_values("period_end").reset_index(drop=True)

    # For each buyback quarter, find the most recent authorization that
    # precedes or matches it, then compute cumulative spend since that auth date.
    auth_amounts = []
    remaining_amounts = []

    for idx, row in df.iterrows():
        current_auth = None
        auth_date = None

        if auth_df is not None and not auth_df.empty:
            # Find latest authorization on or before this period
            prior = auth_df[auth_df["period_end"] <= row["period_end"]]
            if not prior.empty:
                latest_auth = prior.iloc[-1]
                current_auth = latest_auth["authorized_amount_usd"]
                auth_date = latest_auth["period_end"]

        auth_amounts.append(current_auth)

        if current_auth is not None and auth_date is not None:
            # Sum spend from auth_date through current period
            mask = (df["period_end"] >= auth_date) & (
                df["period_end"] <= row["period_end"]
            )
            cumulative_spend = df.loc[mask, "repurchase_value_usd"].sum()
            remaining_amounts.append(max(0, current_auth - cumulative_spend))
        else:
            remaining_amounts.append(None)

    df["authorization_usd"] = auth_amounts
    df["estimated_remaining_usd"] = remaining_amounts

    # Running totals (all-time)
    df["cumulative_shares"] = df["shares_repurchased"].cumsum()
    df["cumulative_value"] = df["repurchase_value_usd"].cumsum()

    return df


def compute_summary(
    dilution_df: pd.DataFrame,
    basic_df: pd.DataFrame,
    buyback_df: pd.DataFrame,
) -> dict:
    """Compute headline summary statistics for a company."""
    summary = {}

    if not dilution_df.empty:
        latest = dilution_df.iloc[-1]
        first = dilution_df.iloc[0]
        summary["latest_diluted_shares"] = int(latest["diluted_shares"])
        summary["latest_period"] = str(latest["period_end"].date())
        summary["total_periods"] = len(dilution_df)
        total_change = latest["diluted_shares"] - first["diluted_shares"]
        summary["total_dilution_change"] = int(total_change)
        summary["total_dilution_pct"] = (
            (total_change / first["diluted_shares"]) * 100
            if first["diluted_shares"] > 0
            else 0
        )

    if not basic_df.empty:
        latest = basic_df.iloc[-1]
        summary["latest_basic_shares"] = int(latest["basic_shares"])

    if not buyback_df.empty:
        summary["total_shares_repurchased"] = int(
            buyback_df["shares_repurchased"].sum()
        )
        summary["total_repurchase_value"] = float(
            buyback_df["repurchase_value_usd"].sum()
        )
        latest = buyback_df.iloc[-1]
        if latest.get("estimated_remaining_usd") is not None:
            summary["buyback_remaining_usd"] = float(
                latest["estimated_remaining_usd"]
            )
        if latest.get("authorization_usd") is not None:
            summary["latest_authorization_usd"] = float(
                latest["authorization_usd"]
            )

    return summary
