"""
data_loader.py — S3 Parquet fetching layer for the EIA dashboard.

Architecture:
  - DATASETS registry: one entry per processed dataset. Adding a new dataset
    (e.g. from a partner) requires only adding a new dict entry here.
  - load_dataset(): reads parquet from S3 via boto3 + pyarrow, cached by
    Streamlit so repeated UI interactions don't re-fetch from S3.
  - normalize_state_col(): unifies the state column name across datasets so
    the map layer always gets a consistent "state" column regardless of whether
    the raw parquet uses "state_name" or "refinery_state".
"""

import io
import boto3
import pandas as pd
import pyarrow.parquet as pq
import streamlit as st

import config

# ── State name normalization ───────────────────────────────────────────────────
# EIA encodes state names in several formats across datasets:
#   "USA-NM"  (duoarea prefix style, natural gas production)
#   "USA-NM"  (same)
#   "TEXAS"   (all-caps alias, natural gas production for some states)
#   "OHIO"    (same)
#   "California" (proper name, crude oil datasets)
# All formats are normalized to proper full names here so that every
# dataset loaded by load_dataset() uses consistent state names.

STATE_FULL_NAMES: dict[str, str] = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
}

# Reverse: full name → abbreviation (used by the map layer in app.py)
STATE_ABBREV: dict[str, str] = {v: k for k, v in STATE_FULL_NAMES.items()}

# Set of canonical full names for fast O(1) lookup
_VALID_FULL_NAMES: set[str] = set(STATE_FULL_NAMES.values())


def normalize_eia_state(name: str) -> str:
    """
    Convert any EIA state name variant to a proper full state name.

    Handled formats:
      - Already a canonical full name: "California" → "California"
      - ALL-CAPS:                      "TEXAS"      → "Texas"
      - USA- prefix:                   "USA-NM"     → "New Mexico"
      - 2-letter abbreviation:         "NM"         → "New Mexico"
      - Title-case attempt as fallback

    Non-state strings (country names, NaN, etc.) are returned unchanged.
    """
    if not isinstance(name, str):
        return name
    name = name.strip()
    if not name:
        return name
    # Already canonical
    if name in _VALID_FULL_NAMES:
        return name
    # USA-XX prefix style
    if name.startswith("USA-") and len(name) == 6:
        abbrev = name[4:]
        return STATE_FULL_NAMES.get(abbrev, name)
    # 2-letter abbreviation
    if len(name) == 2 and name.isupper():
        return STATE_FULL_NAMES.get(name, name)
    # ALL-CAPS (e.g. TEXAS, OHIO, COLORADO)
    titled = name.title()
    if titled in _VALID_FULL_NAMES:
        return titled
    # Nothing matched — return as-is (country names etc. fall through here)
    return name

# ── Dataset Registry ───────────────────────────────────────────────────────────
# Each entry describes one processed Parquet prefix in S3.
# Key design rule: adding a new dataset = adding one dict entry here only.
#
# Required keys per entry:
#   label           — Human-readable name shown in the UI
#   s3_prefix       — Path inside the bucket (no leading slash)
#   state_col       — Column that holds the state name in the parquet file
#   value_col       — Primary numeric column used to color the choropleth map
#   value_label     — Human-readable label for the primary value
#   unit            — Unit string shown in tooltips (e.g. "MMcf", "Thousand Bbl")
#   time_col        — Column holding the time period
#   time_granularity — "monthly" | "annual"
#   category        — Grouping label for the sidebar category selector
#   extra_cols      — Additional numeric columns to show in the state detail panel
#
# Optional keys:
#   breakdown_col   — If set, a secondary breakdown column (e.g. origin_country,
#                     crude_grade) is available for bar charts in the detail panel.
#   breakdown_value — The value column that pairs with breakdown_col.

DATASETS: dict[str, dict] = {
    # ── Natural Gas ────────────────────────────────────────────────────────────
    "ng_production": {
        "label": "Natural Gas — State Production",
        "s3_prefix": "processed/natural_gas_production/",
        "state_col": "state_name",
        "value_col": "production_mmcf",
        "value_label": "Production",
        "unit": "MMcf",
        "time_col": "period",
        "time_granularity": "monthly",
        "category": "Natural Gas",
        "extra_cols": {
            "us_total_mmcf": "US Total (MMcf)",
            "pct_us_production": "% of US Production",
        },
    },
    "ng_intl_trade": {
        "label": "Natural Gas — International Trade",
        "s3_prefix": "processed/natural_gas_trade_international/",
        "state_col": "state_name",
        "value_col": "exports_mmcf",
        "value_label": "Exports",
        "unit": "MMcf",
        "time_col": "year",
        "time_granularity": "annual",
        "category": "Natural Gas",
        "extra_cols": {
            "imports_mmcf": "Imports (MMcf)",
            "net_intl_mmcf": "Net Intl Movement (MMcf)",
        },
    },
    "ng_interstate": {
        "label": "Natural Gas — Interstate Movements",
        "s3_prefix": "processed/natural_gas_trade_interstate/",
        "state_col": "state_name",
        "value_col": "interstate_delivered_mmcf",
        "value_label": "Delivered",
        "unit": "MMcf",
        "time_col": "year",
        "time_granularity": "annual",
        "category": "Natural Gas",
        "extra_cols": {
            "net_interstate_received_mmcf": "Net Interstate Received (MMcf)",
        },
    },
    # ── Crude Oil ──────────────────────────────────────────────────────────────
    "crude_by_state": {
        "label": "Crude Oil — Imports by State",
        "s3_prefix": "processed/crude_oil_imports_by_state/",
        "state_col": "refinery_state",
        "value_col": "total_thousand_bbl",
        "value_label": "Imports",
        "unit": "Thousand Bbl",
        "time_col": "period",
        "time_granularity": "monthly",
        "category": "Crude Oil",
        "extra_cols": {
            "us_total_thousand_bbl": "US Total (Thousand Bbl)",
            "pct_us_imports": "% of US Imports",
        },
    },
    "crude_by_origin": {
        "label": "Crude Oil — Imports by Origin Country",
        "s3_prefix": "processed/crude_oil_imports_by_state_country/",
        "state_col": "refinery_state",
        "value_col": "total_thousand_bbl",
        "value_label": "Imports",
        "unit": "Thousand Bbl",
        "time_col": "period",
        "time_granularity": "monthly",
        "category": "Crude Oil",
        "extra_cols": {},
        "breakdown_col": "origin_country",
        "breakdown_value": "total_thousand_bbl",
    },
    "crude_grade": {
        "label": "Crude Oil — Grade Breakdown",
        "s3_prefix": "processed/crude_oil_imports_grade_breakdown/",
        "state_col": "refinery_state",
        "value_col": "state_total_thousand_bbl",
        "value_label": "Total Imports",
        "unit": "Thousand Bbl",
        "time_col": "period",
        "time_granularity": "monthly",
        "category": "Crude Oil",
        "extra_cols": {},
        "breakdown_col": "crude_grade",
        "breakdown_value": "grade_quantity_thousand_bbl",
    },
    # ── Future partner datasets go here ───────────────────────────────────────
    # Example (uncomment and fill in once partner uploads the data):
    # "electricity_generation": {
    #     "label": "Electricity — Generation by State",
    #     "s3_prefix": "processed/electricity_generation/",
    #     "state_col": "state_name",
    #     "value_col": "generation_mwh",
    #     "value_label": "Generation",
    #     "unit": "MWh",
    #     "time_col": "period",
    #     "time_granularity": "monthly",
    #     "category": "Electricity",
    #     "extra_cols": {},
    # },
}


def _build_s3_client():
    """Create a boto3 S3 client using credentials from config.py."""
    kwargs = {"region_name": config.AWS_REGION}
    if config.AWS_ACCESS_KEY and config.AWS_SECRET_KEY:
        kwargs["aws_access_key_id"] = config.AWS_ACCESS_KEY
        kwargs["aws_secret_access_key"] = config.AWS_SECRET_KEY
    return boto3.client("s3", **kwargs)


def _list_parquet_keys(s3_client, prefix: str) -> list[str]:
    """Return all .parquet object keys under a given S3 prefix."""
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=config.BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet") or key.endswith(".snappy.parquet"):
                keys.append(key)
    return keys


@st.cache_data(ttl=600, show_spinner=False)
def load_dataset(dataset_key: str) -> pd.DataFrame:
    """
    Load a full dataset from S3 as a pandas DataFrame.
    Results are cached for 10 minutes (ttl=600).

    Parameters
    ----------
    dataset_key : str
        Must match a key in DATASETS.

    Returns
    -------
    pd.DataFrame
        The full dataset with a unified 'state' column added.
    """
    meta = DATASETS[dataset_key]
    s3 = _build_s3_client()
    keys = _list_parquet_keys(s3, meta["s3_prefix"])

    if not keys:
        return pd.DataFrame()

    frames = []
    for key in keys:
        obj = s3.get_object(Bucket=config.BUCKET_NAME, Key=key)
        buf = io.BytesIO(obj["Body"].read())
        frames.append(pq.read_table(buf).to_pandas())

    df = pd.concat(frames, ignore_index=True)

    # Normalize state column → always called 'state' for the map layer
    state_col = meta["state_col"]
    if state_col in df.columns and state_col != "state":
        df = df.rename(columns={state_col: "state"})

    # Parse time columns
    if meta["time_granularity"] == "monthly" and "period" in df.columns:
        df["period"] = pd.to_datetime(df["period"])
        df["year"] = df["period"].dt.year
        df["month"] = df["period"].dt.month
    elif meta["time_granularity"] == "annual" and "year" in df.columns:
        df["year"] = df["year"].astype(int)

    # Normalize state names → canonical full names (e.g. "USA-NM" → "New Mexico").
    # This is the single source of truth: every dataset emits consistent state names
    # so that UI lookups (df[df["state"] == selected_state]) always work.
    if "state" in df.columns:
        df["state"] = df["state"].astype(str).str.strip().apply(normalize_eia_state)

    return df


def get_years(df: pd.DataFrame) -> list[int]:
    """Return sorted list of unique years present in the DataFrame."""
    if "year" in df.columns:
        return sorted(df["year"].dropna().unique().tolist())
    return []


def get_months(df: pd.DataFrame) -> list[int]:
    """Return sorted list of unique months (1-12) present in the DataFrame."""
    if "month" in df.columns:
        return sorted(df["month"].dropna().unique().tolist())
    return []


def filter_df(
    df: pd.DataFrame,
    dataset_key: str,
    year: int,
    month: int | None = None,
) -> pd.DataFrame:
    """
    Filter the dataset to a specific year (and optionally month).
    For datasets with a breakdown_col (e.g. crude_by_origin, crude_grade),
    this also aggregates to one row per state so the map gets scalar values.
    """
    meta = DATASETS[dataset_key]
    out = df[df["year"] == year].copy()

    if meta["time_granularity"] == "monthly" and month is not None:
        out = out[out["month"] == month]

    # If the dataset has a breakdown column, we need to aggregate for the map
    # (one row per state), but keep the raw filtered frame too — callers that
    # need the breakdown detail will use the unfiltered frame themselves.
    breakdown_col = meta.get("breakdown_col")
    if breakdown_col and breakdown_col in out.columns:
        value_col = meta["value_col"]
        out = (
            out.groupby("state", as_index=False)[value_col]
            .sum()
        )

    return out
