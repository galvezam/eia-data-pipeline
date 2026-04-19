# dags/eia_processing_dag.py
"""
Processing DAG — calls natural_gas_crude_core.py and
petroleum_coal_electricity_core.py directly (no Papermill / notebooks).

Date parameter priority (same as eia_ingest_dag):
  1. Manual trigger params  (start_date / end_date in Trigger UI)
  2. Airflow Variables      (eia_start_override / eia_end_override)
  3. logical_date           (derived end) + module default_start
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "eia-pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

PROCESSORS = {
    "natural_gas_crude": {
        "module":        "natural_gas_crude_core",
        "default_start": "2010-01",
    },
    "petroleum_coal_electricity": {
        "module":        "petroleum_coal_electricity_core",
        "default_start": "2000-01",
    },
}


def _resolve_date_range(default_start, logical_date):
    start_override = Variable.get("eia_start_override", default_var=None)
    end_override   = Variable.get("eia_end_override",   default_var=None)
    run_end   = end_override   or logical_date.strftime("%Y-%m")
    run_start = start_override or default_start
    return run_start, run_end


def _run_processor(processor_key, processor_cfg, **context):
    import importlib
    logical_date = context["logical_date"]
    dag_params   = context["params"] if "params" in context else {}

    p_start = dag_params["start_date"] if "start_date" in dag_params and dag_params["start_date"] else None
    p_end   = dag_params["end_date"]   if "end_date"   in dag_params and dag_params["end_date"]   else None

    if p_start and p_end:
        run_start = str(p_start)
        run_end   = str(p_end)
    else:
        run_start, run_end = _resolve_date_range(processor_cfg["default_start"], logical_date)

    p_incremental = dag_params["incremental"] if "incremental" in dag_params and dag_params["incremental"] is not None else None
    incremental = (
        str(p_incremental).lower()
        if p_incremental is not None
        else Variable.get("eia_incremental_mode", default_var="true")
    )

    print(f"[{processor_key}] start={run_start}  end={run_end}  incremental={incremental}")

    core = importlib.import_module(processor_cfg["module"])
    core.run(
        aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID", ""),
        aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        aws_region     = os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        bucket_name    = os.environ.get("EIA_BUCKET", os.environ.get("BUCKET_NAME", "")),
        start_date     = run_start,
        end_date       = run_end,
        incremental    = incremental,
    )
    print(f"✓ {processor_key} complete")


with DAG(
    dag_id="eia_processing",
    default_args=default_args,
    description="Run EIA processing modules after ingestion (date-parameterised)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["eia", "processing"],
    params={
        "start_date":  None,
        "end_date":    None,
        "incremental": None,
    },
) as dag:

    prev_task = None

    for key, cfg in PROCESSORS.items():
        process_task = PythonOperator(
            task_id=f"process_{key}",
            python_callable=_run_processor,
            op_kwargs={
                "processor_key": key,
                "processor_cfg": cfg,
            },
        )
        if prev_task:
            prev_task >> process_task
        prev_task = process_task