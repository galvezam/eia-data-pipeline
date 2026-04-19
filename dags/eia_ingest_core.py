# dags/eia_ingest_core.py
"""
Extracted logic from total_ingest.ipynb.
Called directly by eia_ingest_dag — no notebook needed.
"""

import io
import csv
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from dateutil.relativedelta import relativedelta


def _date_slug(d: str) -> str:
    if d is None:
        return "all"
    return d if len(d) > 4 else d  # annual → "2024", monthly/weekly → keep as-is


def stream_data(dataset_name: str, config: dict, incremental: bool = False) -> str:
    """
    Fetch data from EIA API and stream to S3 as CSV.
    Returns the S3 key written.
    Mirrors total_ingest.ipynb stream_data() exactly.
    """
    import boto3
    import requests

    API_KEY = os.environ["EIA_API_KEY"]
    BUCKET  = os.environ.get("EIA_BUCKET", os.environ.get("BUCKET_NAME", ""))
    TODAY   = date.today().strftime("%Y%m%d")

    s3 = boto3.client("s3")

    # ── Resolve start/end ──────────────────────────────────────────────────────
    run_start = config["start"]
    run_end   = config.get("end") or date.today().strftime(
        "%Y" if config["frequency"] == "annual" else
        "%Y-%m-%d" if config["frequency"] == "weekly" else "%Y-%m"
    )

    if incremental:
        try:
            prefix = f"raw/{dataset_name}/{config['frequency']}/{dataset_name}_"
            resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
            objects = [o for o in resp.get("Contents", []) if "incremental" not in o["Key"]]
            if objects:
                latest_key = sorted(objects, key=lambda o: o["LastModified"])[-1]["Key"]
                obj = s3.get_object(Bucket=BUCKET, Key=latest_key)
                body = obj["Body"].read().decode("utf-8")
                reader = csv.DictReader(io.StringIO(body))
                periods = [r["period"] for r in reader if r.get("period")]
                if periods:
                    latest = sorted(periods)[-1]
                    run_start = _compute_incremental_start(latest, config)
        except Exception:
            pass  # No existing data → full ingest

    # ── Build S3 key ───────────────────────────────────────────────────────────
    start_slug = _date_slug(run_start) if run_start else "all"
    end_slug   = _date_slug(run_end)   if run_end   else "all"
    date_range = f"{start_slug}_to_{end_slug}"

    is_incremental_run = incremental and run_start != config["start"]
    if is_incremental_run:
        s3_key = f"raw/{dataset_name}/{config['frequency']}/incremental/{dataset_name}_{date_range}_{TODAY}.csv"
    else:
        s3_key = f"raw/{dataset_name}/{config['frequency']}/{dataset_name}_{date_range}.csv"

    # ── Partition generator ────────────────────────────────────────────────────
    def generate_partitions():
        if run_start is None:
            return [(None, None)]
        if config["frequency"] == "annual":
            return [(str(y), str(y)) for y in range(int(run_start[:4]), int(run_end[:4]) + 1)]
        parts, curr = [], date.fromisoformat(run_start[:7] + "-01")
        end_dt = date.fromisoformat(run_end[:7] + "-01")
        while curr <= end_dt:
            m = curr.strftime("%Y-%m")
            parts.append((m, m))
            curr += relativedelta(months=1)
        return parts

    # ── Page fetcher ───────────────────────────────────────────────────────────
    def fetch_page(offset, p_start, p_end):
        params = {
            "api_key": API_KEY,
            "frequency": config["frequency"],
            "offset": offset,
            "length": config["page_size"],
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
        }
        if p_start: params["start"] = p_start
        if p_end:   params["end"]   = p_end
        for i, d in enumerate(config["data"]):
            params[f"data[{i}]"] = d
        headers = {"X-Params": json.dumps({
            "frequency": config["frequency"], "data": config["data"],
            "facets": config.get("facets", {}), "start": p_start, "end": p_end,
            "sort": [{"column": "period", "direction": "desc"}],
            "offset": offset, "length": config["page_size"],
        })}
        for attempt in range(5):
            r = requests.get(config["url"], params=params, headers=headers, timeout=60)
            if r.status_code == 200:
                return r.json().get("response", {})
            if r.status_code == 429 or "OVER_RATE_LIMIT" in r.text:
                time.sleep(2 * (2 ** attempt))
                continue
            raise Exception(f"API {r.status_code}: {r.text[:200]}")
        raise Exception("Max retries exceeded")

    # ── Streaming buffer + S3 upload ──────────────────────────────────────────
    # Uses multipart upload for large data (parts must be >= 5MB each).
    # Falls back to a single put_object if total data is small.
    PART_SIZE = 5 * 1024 * 1024  # 5 MB minimum for S3 multipart parts

    buffer = io.StringIO()
    header_written = False
    fieldnames = None
    multipart = None
    parts, part_number = [], 1
    pending_chunks = []  # accumulate small chunks until >= PART_SIZE

    def _upload_part(data: str):
        nonlocal part_number, multipart
        if not data:
            return
        if multipart is None:
            multipart = s3.create_multipart_upload(Bucket=BUCKET, Key=s3_key)
        part = s3.upload_part(
            Bucket=BUCKET, Key=s3_key, PartNumber=part_number,
            UploadId=multipart["UploadId"], Body=data.encode("utf-8"),
        )
        parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
        part_number += 1

    def flush(force=False):
        nonlocal buffer, pending_chunks
        chunk = buffer.getvalue()
        buffer = io.StringIO()
        if chunk:
            pending_chunks.append(chunk)
        combined = "".join(pending_chunks)
        if force or len(combined.encode("utf-8")) >= PART_SIZE:
            _upload_part(combined)
            pending_chunks = []

    def write_records(records):
        nonlocal header_written, fieldnames, buffer
        if not records:
            return
        if not header_written:
            fieldnames = list(records[0].keys())
            csv.DictWriter(buffer, fieldnames=fieldnames).writeheader()
            header_written = True
        csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore").writerows(records)
        if buffer.tell() > PART_SIZE:
            flush()

    # ── Stream pages directly to S3 — never accumulate all records in RAM ────
    def stream_partition(p_start, p_end):
        first = fetch_page(0, p_start, p_end)
        total = int(first.get("total", 0))
        records = first.get("data", [])
        if records:
            write_records(records)
        offsets = list(range(config["page_size"], total, config["page_size"]))
        for offset in offsets:
            page = fetch_page(offset, p_start, p_end)
            write_records(page.get("data", []))

    try:
        if config.get("date_partition") and run_start is not None:
            for p_start, p_end in generate_partitions():
                stream_partition(p_start, p_end)
        else:
            stream_partition(run_start, run_end)

        # Final flush — force=True sends whatever remains regardless of size
        flush(force=True)

        if multipart and parts:
            s3.complete_multipart_upload(
                Bucket=BUCKET, Key=s3_key, UploadId=multipart["UploadId"],
                MultipartUpload={"Parts": parts},
            )
        elif multipart:
            s3.abort_multipart_upload(Bucket=BUCKET, Key=s3_key, UploadId=multipart["UploadId"])
        else:
            # Data was small enough to fit in pending_chunks without triggering
            # a multipart upload — use simple put_object instead
            combined = "".join(pending_chunks)
            if combined:
                s3.put_object(Bucket=BUCKET, Key=s3_key, Body=combined.encode("utf-8"))
    except Exception:
        if multipart:
            s3.abort_multipart_upload(Bucket=BUCKET, Key=s3_key, UploadId=multipart["UploadId"])
        raise

    print(f"✓ {dataset_name} → s3://{BUCKET}/{s3_key}")
    return s3_key


def _compute_incremental_start(latest_period: str, config: dict) -> str:
    freq   = config["incremental_frequency"]
    amount, unit = freq.split()
    amount = int(amount)

    if config["frequency"] == "annual":
        return str(int(latest_period[:4]) - amount)

    period_str = latest_period if len(latest_period) > 7 else latest_period + "-01"
    dt = date.fromisoformat(period_str)

    if "month" in unit:
        dt -= relativedelta(months=amount)
        return dt.strftime("%Y-%m")
    elif "week" in unit:
        dt -= relativedelta(weeks=amount)
        return dt.strftime("%Y-%m-%d")
    elif "year" in unit:
        dt -= relativedelta(years=amount)
        return str(dt.year) if config["frequency"] == "annual" else dt.strftime("%Y-%m")

    return latest_period