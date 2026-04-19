# dags/eia_ingest_dag.py
"""
Ingestion DAG — delegates all fetch/write logic to dags/eia_ingest_core.py,
which is the notebook logic extracted into an importable module.

Date parameters flow like this:
  • Airflow passes  logical_date  (the scheduled run date) into each task.
  • Each task calls core.ingest_dataset(name, cfg, start=..., end=..., incremental=...)
    where start/end are derived from the DAG run interval or Airflow Variables.

Override behaviour via Airflow Variables:
  eia_incremental_mode   true / false          (default: true)
  eia_start_override     YYYY-MM-DD            (optional, forces a fixed start)
  eia_end_override       YYYY-MM-DD            (optional, forces a fixed end)
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

# Dataset registry
# Mirrors total_ingest.ipynb DATASETS dict.  start/end here are the
# *historical* full-load defaults; incremental mode overrides them at runtime.
DATASETS = {
    "petroleum": {
        "url": "https://api.eia.gov/v2/petroleum/pnp/wprodrb/data/",
        "frequency": "weekly",
        "data": ["value"],
        "facets": {},
        "start": "2020-01-01",
        "end": None,
        "page_size": 5000,
        "max_page_workers": 3,
        "max_date_workers": 3,
        "date_partition": True,
        "incremental_frequency": "1 month",
    },
    "natural_gas": {
        "url": "https://api.eia.gov/v2/natural-gas/prod/whv/data/",
        "frequency": "monthly",
        "data": ["value"],
        "facets": {},
        "start": "2010-01",
        "end": None,
        "page_size": 5000,
        "max_page_workers": 3,
        "max_date_workers": 3,
        "date_partition": True,
        "incremental_frequency": "1 month",
    },
    "electricity": {
        "url": "https://api.eia.gov/v2/electricity/electric-power-operational-data/data/",
        "frequency": "monthly",
        "data": ["consumption-for-eg", "consumption-for-eg-btu"],
        "facets": {"fueltypeid": ["COL","NG","NUC","HYC","WND","SUN","GEO","ALL","AOR"]},
        "start": "2010-01",
        "end": None,
        "page_size": 5000,
        "max_page_workers": 3,
        "max_date_workers": 3,
        "date_partition": True,
        "incremental_frequency": "1 month",
    },
    "crude_oil_imports": {
        "url": "https://api.eia.gov/v2/crude-oil-imports/data/",
        "frequency": "monthly",
        "data": ["quantity"],
        "facets": {},
        "start": "2010-01",
        "end": None,
        "page_size": 5000,
        "max_page_workers": 3,
        "max_date_workers": 3,
        "date_partition": True,
        "incremental_frequency": "1 month",
    },
    "steo": {
        "url": "https://api.eia.gov/v2/steo/data/",
        "frequency": "monthly",
        "data": ["value"],
        "facets": {},
        "start": "2010-01",
        "end": None,
        "page_size": 5000,
        "max_page_workers": 3,
        "max_date_workers": 3,
        "date_partition": True,
        "incremental_frequency": "1 month",
    },
    "coal": {
        "url": "https://api.eia.gov/v2/coal/exports-imports-quantity-price/data/",
        "frequency": "annual",
        "data": ["price", "quantity"],
        "facets": {},
        "start": "2000",
        "end": None,
        "page_size": 5000,
        "max_page_workers": 3,
        "max_date_workers": 3,
        "date_partition": True,
        "incremental_frequency": "1 year",
    },
    "natural_gas_trade": {
        "url": "https://api.eia.gov/v2/natural-gas/move/ist/data/",
        "frequency": "annual",
        "data": ["value"],
        "facets": {},
        "start": "2000",
        "end": None,
        "page_size": 5000,
        "max_page_workers": 3,
        "max_date_workers": 3,
        "date_partition": True,
        "incremental_frequency": "1 year",
    },
    "seds": {
        "url": "https://api.eia.gov/v2/seds/data/",
        "frequency": "annual",
        "data": ["value"],
        "facets": {},
        "start": "1960",
        "end": None,
        "page_size": 5000,
        "max_page_workers": 1,   # reduced — SEDS is very large
        "max_date_workers": 1,   # process one year at a time to avoid OOM
        "date_partition": True,
        "incremental_frequency": "2 years",  # only fetch last 2 years incrementally
    },
    "total_energy": {
        "url": "https://api.eia.gov/v2/total-energy/data/",
        "frequency": "monthly",
        "data": ["value"],
        "facets": {},
        "start": "2000-01",
        "end": None,
        "page_size": 5000,
        "max_page_workers": 3,
        "max_date_workers": 3,
        "date_partition": True,
        "incremental_frequency": "2 months",
    },
    "petroleum_movements": {
        "url": "https://api.eia.gov/v2/petroleum/move/wkly/data/",
        "frequency": "weekly",
        "data": ["value"],
        "facets": {},
        "start": "2020-01-01",
        "end": None,
        "page_size": 5000,
        "max_page_workers": 3,
        "max_date_workers": 3,
        "date_partition": True,
        "incremental_frequency": "1 month",
    },
    "electricity_state_rankings": {
        "url": "https://api.eia.gov/v2/electricity/state-electricity-profiles/summary/data/",
        "frequency": "annual",
        "data": [
            "average-retail-price-rank","carbon-dioxide-rank",
            "net-generation-rank","nitrogen-oxide-rank","sulfer-dioxide-rank",
        ],
        "facets": {},
        "start": None,
        "end": None,
        "page_size": 5000,
        "max_page_workers": 3,
        "max_date_workers": 3,
        "date_partition": False,
        "incremental_frequency": "1 year",
    },
    "electricity_net_metering": {
        "url": "https://api.eia.gov/v2/electricity/state-electricity-profiles/net-metering/data/",
        "frequency": "annual",
        "data": ["capacity", "customers"],
        "facets": {},
        "start": "2010",
        "end": None,
        "page_size": 5000,
        "max_page_workers": 3,
        "max_date_workers": 3,
        "date_partition": True,
        "incremental_frequency": "1 year",
    },
    "electricity_generating_capacity": {
        "url": "https://api.eia.gov/v2/electricity/state-electricity-profiles/capability/data/",
        "frequency": "annual",
        "data": ["capability"],
        "facets": {},
        "start": "2010",
        "end": None,
        "page_size": 5000,
        "max_page_workers": 3,
        "max_date_workers": 3,
        "date_partition": True,
        "incremental_frequency": "1 year",
    },
    "densified_biomass": {
        "url": "https://api.eia.gov/v2/densified-biomass/export-sales-and-price/data/",
        "frequency": "monthly",
        "data": ["average-price", "quantity"],
        "facets": {},
        "start": "2015-01",
        "end": None,
        "page_size": 5000,
        "max_page_workers": 3,
        "max_date_workers": 3,
        "date_partition": True,
        "incremental_frequency": "2 months",
    },
}

# Default args
default_args = {
    "owner": "eia-pipeline",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
}


def _resolve_date_range(dataset_name: str, config: dict, logical_date: datetime) -> tuple[str, str]:
    """
    Determine (start, end) for this run.

    Priority:
      1. Airflow Variable overrides  (eia_start_override / eia_end_override)
      2. Incremental mode            (look up latest stored period, step back one increment)
      3. Config defaults             (full historical load)

    `logical_date` is the Airflow execution date passed in from the task context.
    It is used as the `end` ceiling when no override is set.
    """
    import io, csv, boto3
    from datetime import date
    from dateutil.relativedelta import relativedelta

    # Variable overrides (manual backfills / one-off runs)
    start_override = Variable.get("eia_start_override", default_var=None)
    end_override   = Variable.get("eia_end_override",   default_var=None)
    incremental    = Variable.get("eia_incremental_mode", default_var="true") == "true"

    if start_override and end_override:
        return start_override, end_override

    # Derive end from logical_date when not overridden
    freq = config["frequency"]
    if freq == "annual":
        run_end = str(logical_date.year)
    elif freq in ("monthly",):
        run_end = logical_date.strftime("%Y-%m")
    else:  # weekly
        run_end = logical_date.strftime("%Y-%m-%d")

    if end_override:
        run_end = end_override

    # Incremental: find latest already-stored period
    run_start = config["start"]

    if incremental and run_start is not None:
        BUCKET = os.environ.get("EIA_BUCKET", os.environ.get("BUCKET_NAME", ""))
        # Filenames now include date range — find latest full-load file by prefix
        try:
            s3 = boto3.client("s3")
            prefix = f"raw/{dataset_name}/{freq}/{dataset_name}_"
            resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
            objects = [o for o in resp.get("Contents", []) if "incremental" not in o["Key"]]
            if not objects:
                raise FileNotFoundError
            latest_key = sorted(objects, key=lambda o: o["LastModified"])[-1]["Key"]
            obj = s3.get_object(Bucket=BUCKET, Key=latest_key)
            body = obj["Body"].read().decode("utf-8")
            reader = csv.DictReader(io.StringIO(body))
            periods = [r["period"] for r in reader if r.get("period")]
            if periods:
                latest = sorted(periods)[-1]
                freq_inc = config["incremental_frequency"]
                amount, unit = freq_inc.split()
                amount = int(amount)

                if freq == "annual":
                    run_start = str(int(latest[:4]) - amount)
                else:
                    period_str = latest if len(latest) > 7 else latest + "-01"
                    dt = date.fromisoformat(period_str)
                    if "month" in unit:
                        dt -= relativedelta(months=amount)
                        run_start = dt.strftime("%Y-%m")
                    elif "week" in unit:
                        dt -= relativedelta(weeks=amount)
                        run_start = dt.strftime("%Y-%m-%d")
                    elif "year" in unit:
                        dt -= relativedelta(years=amount)
                        run_start = str(dt.year) if freq == "annual" else dt.strftime("%Y-%m")
        except Exception:
            pass  # No existing data → fall through to full-load start

    if start_override:
        run_start = start_override

    return run_start, run_end


def _run_ingest(dataset_name: str, config: dict, start: str, end: str, incremental: bool) -> str:
    """Delegates to eia_ingest_core.stream_data()."""
    import eia_ingest_core as core
    cfg = {**config, "start": start, "end": end}
    return core.stream_data(dataset_name, cfg, incremental=incremental)

# DAG definition
with DAG(
    dag_id="eia_ingest",
    default_args=default_args,
    description="Ingest EIA API datasets → S3 raw/  (date-parameterised)",
    schedule="0 6 * * *",   # daily at 06:00 UTC; logical_date = yesterday
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=2,
    tags=["eia", "ingest"],
    # Expose params so manual triggers can override dates without Variables
    params={
        "start_date": None,   # e.g. "2024-01-01"  — overrides all datasets
        "end_date":   None,   # e.g. "2024-03-31"
        "incremental": None,  # "true" / "false"   — overrides the Variable
    },
) as dag:

    @task(task_id="ingest_start")
    def ingest_start(**context):
        logical = context["logical_date"]
        params  = context.get("params", {})
        print(
            f"EIA ingestion run | logical_date={logical.date()} | "
            f"param overrides={params}"
        )

    @task(task_id="ingest_end")
    def ingest_end(**context):
        ti = context["ti"]
        results = {name: ti.xcom_pull(task_ids=f"ingest_{name}") for name in DATASETS}
        ok  = {k: v for k, v in results.items() if v}
        err = {k: v for k, v in results.items() if not v}
        print(f"completed ({len(ok)}): {list(ok)}")
        if err:
            print(f"missing  ({len(err)}): {list(err)}")

    start_task = ingest_start()
    end_task   = ingest_end()

    for dataset_name, dataset_config in DATASETS.items():

        @task(task_id=f"ingest_{dataset_name}")
        def ingest_task(name=dataset_name, cfg=dataset_config, **context):
            logical = context["logical_date"]
            # Access params safely — Airflow 2.9 stores them as ParamsDict
            params = context["params"] if "params" in context else {}

            p_start = params["start_date"] if "start_date" in params and params["start_date"] else None
            p_end   = params["end_date"]   if "end_date"   in params and params["end_date"]   else None

            # Param-level overrides (manual trigger UI) take top priority
            if p_start and p_end:
                run_start = str(p_start)
                run_end   = str(p_end)
            else:
                run_start, run_end = _resolve_date_range(name, cfg, logical)

            p_incremental = params["incremental"] if "incremental" in params and params["incremental"] is not None else None
            if p_incremental is not None:
                incremental = str(p_incremental).lower() == "true"
            else:
                incremental = Variable.get("eia_incremental_mode", default_var="true") == "true"

            print(
                f"[{name}] mode={'incremental' if incremental else 'full'} "
                f"start={run_start}  end={run_end}"
            )

            s3_key = _run_ingest(name, cfg, start=run_start, end=run_end, incremental=incremental)
            bucket = os.environ.get("EIA_BUCKET", os.environ.get("BUCKET_NAME", ""))
            print(f"{name}: s3://{bucket}/{s3_key}")
            return s3_key

        start_task >> ingest_task() >> end_task