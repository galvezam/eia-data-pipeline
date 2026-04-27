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
  - filter_df(): filters the dataset to a specific year (and optionally month).
    For datasets with a breakdown_col (e.g. crude_by_origin, crude_grade),
    this also aggregates to one row per state so the map gets scalar values.
  - agg_for_map(): aggregates a filtered DataFrame to one row per state for
    choropleth rendering. Only called by app.py for state-level datasets;
    no-map datasets skip this.
  - get_years(), get_months(), get_weeks(): utility helpers to get sorted lists
    of unique years, months, and weeks present in a DataFrame.
  - seds_col_label(): converts SEDS pivot column names to human-readable labels.
    e.g. "TE" --> "Total Energy (All Sources)"
         "CO" --> "Coal"
         "av_pct" --> "Aviation Gasoline (% of Total)"
         "wy_pct" --> "Wind Energy (% of Total)"
  - normalize_eia_state(): converts EIA state/area names to a canonical display name.
    e.g. "California" --> "California"
         "TEXAS" --> "Texas"
         "USA-NM" --> "New Mexico"
         "NM" --> "New Mexico"
         "R20", "NUS" --> "PADD 2 (Midwest)" etc.
         "Baltimore, MD" --> returned as-is (no state)
         "AK" --> "Alaska"
"""

import io
import boto3
import pandas as pd
import pyarrow.parquet as pq
import streamlit as st

import config

# State name normalization:
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

STATE_ABBREV: dict[str, str] = {v: k for k, v in STATE_FULL_NAMES.items()}
_VALID_FULL_NAMES: set[str] = set(STATE_FULL_NAMES.values())

# SEDS fuel prefix → readable label (used for metric selector display names)
# Source: EIA SEDS MSN documentation — first 2 chars of each series code.
SEDS_FUEL_LABELS: dict[str, str] = {
    "AB": "Aviation Gasoline Blending Components",
    "AV": "Aviation Gasoline",
    "AR": "Asphalt & Road Oil",
    "CL": "Coal (All Sectors)",
    "CO": "Coal",
    "DF": "Distillate Fuel Oil",
    "EL": "Electricity (Retail Sales)",
    "ES": "Electricity (All Sectors)",
    "GE": "Geothermal Energy",
    "HY": "Hydroelectric Power",
    "JF": "Jet Fuel",
    "KS": "Kerosene",
    "LG": "LPG (Liquefied Petroleum Gas)",
    "LU": "Lubricants",
    "MG": "Motor Gasoline",
    "NG": "Natural Gas",
    "NU": "Nuclear Energy",
    "PA": "Petroleum (All Products)",
    "PC": "Petroleum Coke",
    "PQ": "Petroleum & Natural Gas (Combined)",
    "RE": "Renewables (Total)",
    "RF": "Residual Fuel Oil",
    "SO": "Solar Energy",
    "TE": "Total Energy (All Sources)",
    "WD": "Wood & Wood Waste",
    "WS": "Waste Energy",
    "WY": "Wind Energy",
    "NF": "Non-Fossil Fuels",
    "FF": "Fossil Fuels (Total)",
    "EN": "Energy Intensity",
    "PR": "Prices",
    "EX": "Expenditures",
    "EM": "Emissions (CO2)",
}

# Suffix patterns in SEDS pivot columns → readable suffixes
SEDS_SUFFIX_LABELS: dict[str, str] = {
    "_pct": " (% of Total)",
    "_btu": " (Billion BTU)",
    "_mbtu": " (Million BTU)",
}

def seds_col_label(col: str) -> str:
    """
    Convert a SEDS pivot column name to a human-readable label.
    e.g. "TE"     → "Total Energy (All Sources)"
         "CO"     → "Coal"
         "av_pct" → "Aviation Gasoline (% of Total)"
         "wy_pct" → "Wind Energy (% of Total)"
    """
    # Check suffix patterns first (e.g. co_pct, av_pct)
    for suffix, suffix_label in SEDS_SUFFIX_LABELS.items():
        if col.lower().endswith(suffix):
            prefix = col[: -len(suffix)].upper()
            base = SEDS_FUEL_LABELS.get(prefix, prefix)
            return base + suffix_label
    # Direct 2-letter prefix lookup (e.g. "TE", "CO", "AV")
    upper = col.upper()
    if upper in SEDS_FUEL_LABELS:
        return SEDS_FUEL_LABELS[upper]
    # Fallback: title-case the column name
    return col.replace("_", " ").title()

# PADD region codes → readable labels (petroleum datasets use these)
PADD_LABELS: dict[str, str] = {
    "NUS":     "U.S. Total",
    "NUS-Z00": "U.S. Total",
    "R10":     "PADD 1 (East Coast)",
    "R1X":     "PADD 1A (New England)",
    "R1Y":     "PADD 1B (Central Atlantic)",
    "R1Z":     "PADD 1C (Lower Atlantic)",
    "R20":     "PADD 2 (Midwest)",
    "R30":     "PADD 3 (Gulf Coast)",
    "R40":     "PADD 4 (Rocky Mountain)",
    "R50":     "PADD 5 (West Coast)",
}


def normalize_eia_state(name: str) -> str:
    """
    Convert any EIA state/area name variant to a canonical display name.

    Handled formats:
      - Canonical full name:   "California"  → "California"
      - ALL-CAPS:              "TEXAS"        → "Texas"
      - USA- prefix:           "USA-NM"       → "New Mexico"
      - 2-letter abbreviation: "NM"           → "New Mexico"
      - PADD / NUS codes:      "R20", "NUS"   → "PADD 2 (Midwest)" etc.
      - Customs district:      "Baltimore, MD" → returned as-is (no state)
      - stateId (SEDS/elec):   "AK"           → "Alaska"
    """
    if not isinstance(name, str):
        return name
    name = name.strip()
    if not name:
        return name
    # Already canonical
    if name in _VALID_FULL_NAMES:
        return name
    # PADD / NUS codes
    if name in PADD_LABELS:
        return PADD_LABELS[name]
    # USA-XX prefix
    if name.startswith("USA-") and len(name) == 6:
        abbrev = name[4:]
        return STATE_FULL_NAMES.get(abbrev, name)
    # 2-letter abbreviation
    if len(name) == 2 and name.isupper():
        return STATE_FULL_NAMES.get(name, name)
    # ALL-CAPS full name (e.g. TEXAS, OHIO)
    titled = name.title()
    if titled in _VALID_FULL_NAMES:
        return titled
    # Nothing matched — return as-is (customs districts, country names, etc.)
    return name


# Dataset Registry:
DATASETS: dict[str, dict] = {

    # Natural Gas:
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

    # Crude Oil 
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

    # Petroleum
    # NOTE: petroleum datasets use duoarea codes (NUS, R10-R50, state abbrevs).
    # normalize_eia_state() maps these to readable PADD labels or state names.
    # The choropleth map will only render rows that resolve to a US state abbrev;
    # PADD/NUS aggregate rows are shown in charts and tables but not on the map.
    "petroleum_production": {
        "label": "Petroleum — Production by Area & Product",
        "no_map": True,
        "s3_prefix": "processed/petroleum_production/",
        "state_col": "area_name",
        "value_col": "value_kbd",
        "value_label": "Production",
        "unit": "MBBL/D",
        "time_col": "period",
        "time_granularity": "weekly",
        "category": "Petroleum",
        "extra_cols": {
            "product_name": "Product",
            "process_name": "Process",
            "series_description": "Series Description",
        },
        "breakdown_col": "product_id",
        "breakdown_value": "value_kbd",
    },
    "petroleum_movements": {
        "label": "Petroleum — Movements (Imports / Exports)",
        "no_map": True,
        "s3_prefix": "processed/petroleum_movements/",
        "state_col": "area_name",
        "value_col": "value_kbd",
        "value_label": "Volume",
        "unit": "MBBL/D",
        "time_col": "period",
        "time_granularity": "weekly",
        "category": "Petroleum",
        "extra_cols": {
            "product_name": "Product",
            "process_label": "Movement Type",
            "series_description": "Series Description",
        },
        "breakdown_col": "process_id",
        "breakdown_value": "value_kbd",
    },
    "petroleum_movements_wide": {
        "label": "Petroleum — Imports vs Exports (Wide)",
        "no_map": True,
        "s3_prefix": "processed/petroleum_movements/wide/",
        "state_col": "area_name",
        "value_col": "exports_kbd",
        "value_label": "Exports",
        "unit": "MBBL/D",
        "time_col": "period",
        "time_granularity": "weekly",
        "category": "Petroleum",
        "extra_cols": {
            "imports_kbd": "Imports (MBBL/D)",
            "net_imports_kbd": "Net Imports (MBBL/D)",
            "trade_balance_kbd": "Trade Balance (MBBL/D)",
        },
        "breakdown_col": "product_id",
        "breakdown_value": "exports_kbd",
    },

    # Coal:
    # NOTE: coal datasets use customs_district as the "state" dimension
    # (e.g. "Baltimore, MD"). These don't map to US state abbreviations so
    # the choropleth map is skipped; charts and tables still render normally.
    "coal_trade": {
        "label": "Coal — Raw Trade (by Country & District)",
        "s3_prefix": "processed/coal_trade/",
        "state_col": "customs_district",
        "value_col": "quantity_short_tons",
        "value_label": "Quantity",
        "unit": "Short Tons",
        "time_col": "year",
        "time_granularity": "annual",
        "category": "Coal",
        "extra_cols": {
            "price_usd_per_ton": "Price (USD/Ton)",
            "coal_rank_desc": "Coal Rank",
            "export_import_type": "Direction",
        },
        "breakdown_col": "country_desc",
        "breakdown_value": "quantity_short_tons",
        "no_map": True,
    },
    "coal_trade_summary": {
        "label": "Coal — Trade Summary w/ % of US",
        "s3_prefix": "processed/coal_trade/summary/",
        "state_col": "customs_district",
        "value_col": "total_short_tons",
        "value_label": "Total Quantity",
        "unit": "Short Tons",
        "time_col": "year",
        "time_granularity": "annual",
        "category": "Coal",
        "extra_cols": {
            "avg_price_usd_per_ton": "Avg Price (USD/Ton)",
            "min_price_usd_per_ton": "Min Price (USD/Ton)",
            "max_price_usd_per_ton": "Max Price (USD/Ton)",
            "us_total_short_tons": "US Total (Short Tons)",
            "us_avg_price_usd_per_ton": "US Avg Price (USD/Ton)",
            "pct_us_direction": "% of US Direction",
        },
        "breakdown_col": "country_desc",
        "breakdown_value": "total_short_tons",
        "no_map": True,
    },

    # Electricity:
    # NOTE: electricity datasets use state_id (2-letter) + state_name.
    # normalize_eia_state() maps state_id "AK" → "Alaska" etc.
    "electricity_by_fuel": {
        "label": "Electricity — Consumption by Fuel & State",
        "s3_prefix": "processed/electricity_by_fuel_state/",
        "state_col": "state_name",       # already full name from processing
        "value_col": "consumption_thousand_mwh",
        "value_label": "Consumption",
        "unit": "Thousand MWh",
        "time_col": "period",
        "time_granularity": "monthly",
        "category": "Electricity",
        "extra_cols": {
            "fuel_type_label": "Fuel Type",
            "consumption_btu": "Consumption (BTU)",
            "us_fuel_thousand_mwh": "US Total by Fuel (Thousand MWh)",
            "pct_us_fuel_consumption": "% of US Fuel Consumption",
        },
        "breakdown_col": "fuel_type_id",
        "breakdown_value": "consumption_thousand_mwh",
    },
    "electricity_rankings": {
        "label": "Electricity — State Rankings",
        "s3_prefix": "processed/electricity_state_rankings/",
        "state_col": "state_name",
        "value_col": "net_generation_rank",
        "value_label": "Net Generation Rank",
        "unit": "Rank",
        "time_col": "year",
        "time_granularity": "annual",
        "category": "Electricity",
        "extra_cols": {
            "average_retail_price_rank": "Avg Retail Price Rank",
            "carbon_dioxide_rank": "CO2 Rank",
            "direct_use_rank": "Direct Use Rank",
            "fsp_sales_rank": "FSP Sales Rank",
            "generation_elect_utils_rank": "Generation (Elec Utils) Rank",
            "nitrogen_oxide_rank": "NOx Rank",
            "sulfer_dioxide_rank": "SO2 Rank",
        },
    },
    "electricity_net_metering": {
        "label": "Electricity — Net Metering by State",
        "s3_prefix": "processed/electricity_net_metering/",
        "state_col": "state_name",
        "value_col": "capacity_mw",
        "value_label": "Capacity",
        "unit": "MW",
        "time_col": "year",
        "time_granularity": "annual",
        "category": "Electricity",
        "extra_cols": {
            "customers_count": "Number of Customers",
            "sectorName": "Sector",
        },
        "breakdown_col": "sectorid",
        "breakdown_value": "capacity_mw",
    },
    "electricity_capacity": {
        "label": "Electricity — Generating Capacity by State",
        "s3_prefix": "processed/electricity_generating_capacity/",
        "state_col": "state_name",
        "value_col": "state_total_mw",
        "value_label": "Capacity",
        "unit": "MW",
        "time_col": "year",
        "time_granularity": "annual",
        "category": "Electricity",
        "extra_cols": {
            "us_total_mw": "US Total (MW)",
            "pct_us_capacity": "% of US Capacity",
            "num_producer_types": "# Producer Types",
        },
        "breakdown_col": "energy_source",
        "breakdown_value": "state_total_mw",
    },

    # Total Energy:
    "total_energy": {
        "label": "Total Energy — Monthly Series",
        "s3_prefix": "processed/total_energy/",
        "state_col": None,
        "value_col": "value_quad_btu",
        "value_label": "Value",
        "unit": "Quad BTU",
        "time_col": "period",
        "time_granularity": "monthly",
        "category": "Total Energy",
        "extra_cols": {
            "series_description": "Series Description",
            "unit": "Unit",
        },
        "breakdown_col": "series_code",
        "breakdown_value": "value_quad_btu",
        "no_map": True,
    },

    # SEDS:
    # NOTE: SEDS uses state_id (2-letter code, e.g. "AK") + state_name.
    # state_name is already a full name from processing; state_id is used
    # as the normalize target where state_name is absent.
    "seds_state_consumption": {
        "label": "SEDS — State Consumption by Fuel",
        "s3_prefix": "processed/seds_state_consumption/",
        "state_col": "state_name",
        "value_col": "total_btu",
        "value_label": "Consumption",
        "unit": "Billion BTU",
        "time_col": "year",
        "time_granularity": "annual",
        "category": "SEDS",
        "extra_cols": {
            "fuel_prefix": "Fuel Code",
            "us_total_btu": "US Total (Billion BTU)",
            "pct_us_fuel_consumption": "% of US Fuel Consumption",
            "num_series": "# Series",
        },
        "breakdown_col": "fuel_prefix",
        "breakdown_value": "total_btu",
    },
    "seds_state_pct_us": {
        "label": "SEDS — State % of US Total Energy",
        "s3_prefix": "processed/seds_state_pct_us/",
        "state_col": "state_name",
        "value_col": "total_btu",
        "value_label": "Total Consumption",
        "unit": "Billion BTU",
        "time_col": "year",
        "time_granularity": "annual",
        "category": "SEDS",
        "extra_cols": {
            "us_all_btu": "US All Fuels (Billion BTU)",
            "pct_us_total_consumption": "% of US Total",
            "yoy_change_btu": "YoY Change (Billion BTU)",
            "yoy_change_pct": "YoY Change (%)",
        },
    },
    "seds_fuel_pivot": {
        "label": "SEDS — Fuel Mix by State (Wide)",
        "s3_prefix": "processed/seds_fuel_pivot/",
        "state_col": "state_name",
        "value_col": None,
        "value_label": None,
        "unit": "Billion BTU / %",
        "time_col": "year",
        "time_granularity": "annual",
        "category": "SEDS",
        "extra_cols": {},
        "note": "Wide table — one column per fuel prefix with _pct share columns.",
        "no_map": True,
    },
}


# S3 helpers:
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
            # Skip Spark metadata files
            basename = key.rsplit("/", 1)[-1]
            if basename.startswith("_") or basename.startswith("."):
                continue
            # Match any parquet variant
            if ".parquet" in key:
                keys.append(key)
    return keys


# Per-schema post-load normalizers
def _normalize_petroleum(df: pd.DataFrame, meta: dict) -> pd.DataFrame:
    """
    Petroleum datasets use duoarea codes (NUS, R10-R50) as area identifiers.
    The state_col is area_name (a descriptive string like "U.S." or
    "Midwest (PADD 2)"). We keep these as-is since they don't map to US
    states — the no_map flag prevents choropleth rendering for national rows.
    """
    return df


def _normalize_coal(df: pd.DataFrame, meta: dict) -> pd.DataFrame:
    """
    Coal datasets use customs_district (e.g. "Baltimore, MD") as the location
    dimension. These are port districts, not states. Returned as-is;
    no_map=True prevents choropleth rendering.
    """
    return df


def _normalize_seds(df: pd.DataFrame, meta: dict) -> pd.DataFrame:
    """
    SEDS schema: state_id (2-letter) + state_name (full name from processing).
    If state_name is missing or null, fall back to normalizing state_id.
    """
    if "state_name" in df.columns:
        df["state_name"] = df["state_name"].fillna(
            df.get("state_id", pd.Series(dtype=str))
        ).apply(normalize_eia_state)
    elif "state_id" in df.columns:
        df["state_name"] = df["state_id"].apply(normalize_eia_state)
    return df


def _normalize_electricity(df: pd.DataFrame, meta: dict) -> pd.DataFrame:
    """
    Electricity datasets: state_id (2-letter) + state_name (full name).
    Normalize state_name using state_id as fallback.
    """
    return _normalize_seds(df, meta)   # same pattern


_SCHEMA_NORMALIZERS = {
    "Petroleum":    _normalize_petroleum,
    "Coal":         _normalize_coal,
    "SEDS":         _normalize_seds,
    "Electricity":  _normalize_electricity,
}


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
    df = df.drop_duplicates()

    # Schema-specific normalization
    category = meta.get("category", "")
    normalizer = _SCHEMA_NORMALIZERS.get(category)
    if normalizer:
        df = normalizer(df, meta)

    # Unify state column → always 'state' for the map layer
    state_col = meta["state_col"]
    if state_col and state_col in df.columns:
        if state_col != "state":
            df = df.rename(columns={state_col: "state"})
        # Generic normalize pass for any remaining non-normalized values
        df["state"] = df["state"].astype(str).str.strip().apply(normalize_eia_state)

    # Parse time columns
    if meta["time_granularity"] == "weekly" and "period" in df.columns:
        df["period"] = pd.to_datetime(df["period"], errors="coerce")
        df = df[df["period"].notna()].copy()
        iso = df["period"].dt.isocalendar()
        df["year"] = iso.year.astype("Int64").astype(int)
        df["week"] = iso.week.astype("Int64").astype(int)
    elif meta["time_granularity"] == "monthly" and "period" in df.columns:
        df["period"] = pd.to_datetime(df["period"], errors="coerce")
        df = df[df["period"].notna()].copy()
        df["year"]  = df["period"].dt.year.astype(int)
        df["month"] = df["period"].dt.month.astype(int)
    elif meta["time_granularity"] == "annual" and "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce")
        df = df[df["year"].notna()].copy()
        df["year"] = df["year"].astype(int)

    return df


def get_years(df: pd.DataFrame) -> list[int]:
    """Return sorted list of unique years present in the DataFrame."""
    if "year" in df.columns:
        return sorted(int(y) for y in df["year"].dropna().unique())
    return []


def get_months(df: pd.DataFrame) -> list[int]:
    """Return sorted list of unique months (1-12) present in the DataFrame."""
    if "month" in df.columns:
        return sorted(int(m) for m in df["month"].dropna().unique())
    return []


def get_weeks(df: pd.DataFrame) -> list[int]:
    if "week" in df.columns:
        return sorted(int(w) for w in df["week"].dropna().unique())
    return []


def filter_df(
    df: pd.DataFrame,
    dataset_key: str,
    year: int,
    month: int | None = None,
    week: int | None = None,
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
    elif meta["time_granularity"] == "weekly" and week is not None:
        out = out[out["week"] == week]

    return out


def agg_for_map(df: pd.DataFrame, dataset_key: str, metric_col: str) -> pd.DataFrame:
    """
    Aggregate a filtered DataFrame to one row per state for choropleth rendering.
    Only called by app.py for state-level datasets; no-map datasets skip this.
    """
    if "state" not in df.columns or not metric_col or metric_col not in df.columns:
        return df
    if not pd.api.types.is_numeric_dtype(df[metric_col]):
        return df
    return df.groupby("state", as_index=False)[metric_col].sum()