# dags/natural_gas_crude_core.py
"""
Extracted logic from natural_gas_crude_processing.ipynb.
"""

import os


def _build_spark(aws_access_key: str, aws_secret_key: str, aws_region: str):
    from pyspark.sql import SparkSession
    import os
    
    existing = SparkSession.getActiveSession()
    if existing:
        existing.stop()

    # Small delay to let the JVM fully shut down before restarting
    import time
    time.sleep(3)

    for key in (
        "PYSPARK_SUBMIT_ARGS",
        "SPARK_HADOOP_FS_S3A_CONNECTION_TIMEOUT",
        "SPARK_HADOOP_FS_S3A_SOCKET_TIMEOUT",
    ):
        os.environ.pop(key, None)

    SPARK_PACKAGES = (
        "org.apache.hadoop:hadoop-aws:3.2.2,"
        "com.amazonaws:aws-java-sdk-bundle:1.11.901"
    )
    builder = (
        SparkSession.builder
        .appName("EIA Natural Gas & Crude Oil Processing")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.hadoop.validateOutputSpecs", False)
        .config("spark.hadoop.fs.s3a.connection.timeout", "200000")
        .config("spark.hadoop.fs.s3a.socket.timeout",     "200000")
    )
    if aws_access_key and aws_secret_key and aws_region:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint",
                    f"s3.{aws_region}.amazonaws.com")
            .config("spark.hadoop.fs.s3a.access.key",  aws_access_key)
            .config("spark.hadoop.fs.s3a.secret.key",  aws_secret_key)
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def run(
    aws_access_key: str,
    aws_secret_key: str,
    aws_region: str,
    bucket_name: str,
    start_date: str,
    end_date: str,
    **kwargs,
):
    """
    Main entry point called by the DAG task.
    Reads raw CSVs from S3, cleans, analyses, and writes Parquet back to S3.
    start_date / end_date are used to locate the correct raw CSV files.
    """
    import glob as _glob
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType

    spark = _build_spark(aws_access_key, aws_secret_key, aws_region)

    S3_BASE = f"s3a://{bucket_name}"
    RAW     = f"{S3_BASE}/raw"
    OUT     = f"{S3_BASE}/processed"

    def all_s3_csvs(prefix):
        """Return a list of all non-incremental CSV paths under prefix (paginated)."""
        import boto3
        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".csv") and "/incremental/" not in key:
                    keys.append(key)
        if not keys:
            raise FileNotFoundError(f"No CSVs at s3://{bucket_name}/{prefix}")
        # Return a LIST — not a comma-joined string
        return [f"s3a://{bucket_name}/{k}" for k in keys]

    NAT_GAS_PATH  = all_s3_csvs("raw/natural_gas/monthly/")
    NG_TRADE_PATH = all_s3_csvs("raw/natural_gas_trade/annual/")
    CRUDE_PATH    = all_s3_csvs("raw/crude_oil_imports/monthly/")

    print(f"natural_gas   : {NAT_GAS_PATH}")
    print(f"ng_trade      : {NG_TRADE_PATH}")
    print(f"crude_imports : {CRUDE_PATH}")

    # S3 output paths
    S3_NG_PRODUCTION   = f"{OUT}/natural_gas_production/"
    S3_NG_INTL_TRADE   = f"{OUT}/natural_gas_trade_international/"
    S3_NG_INTERSTATE   = f"{OUT}/natural_gas_trade_interstate/"
    S3_CRUDE_BY_ORIGIN = f"{OUT}/crude_oil_imports_by_state_country/"
    S3_CRUDE_STATE     = f"{OUT}/crude_oil_imports_by_state/"
    S3_CRUDE_GRADE     = f"{OUT}/crude_oil_imports_grade_breakdown/"

    # Load
    def read_csv(path):
        return (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .option("multiLine", True)
            .option("escape", '"')
            .csv(path)
        )

    raw_ng    = read_csv(NAT_GAS_PATH)
    raw_trade = read_csv(NG_TRADE_PATH)
    raw_crude = read_csv(CRUDE_PATH)

    # Clean: Natural Gas Production
    ng_clean = (
        raw_ng
        .filter(F.col("value").isNotNull())
        .filter(F.col("duoarea").startswith("S"))
        .filter(F.col("process") == "VGM")
        .withColumn("period", F.to_date(F.col("period"), "yyyy-MM"))
        .withColumn("production_mmcf", F.col("value").cast("double"))
        .withColumnRenamed("area-name", "state_name")
        .select("period", "duoarea", "state_name", "production_mmcf")
        .dropDuplicates()
    )

    # Clean: Natural Gas Trade
    PROCESS_LABELS = {
        "EEI": "Exports (Intl)", "IMI": "Imports (Intl)", "IMB": "Net Intl Movements",
        "MID": "Interstate Deliveries", "MIN": "Net Interstate Receipts", "MIE": "Net Intl + Interstate",
    }
    process_map = F.create_map(*[item for pair in [(F.lit(k), F.lit(v)) for k, v in PROCESS_LABELS.items()] for item in pair])

    trade_clean = (
        raw_trade
        .filter(F.col("value").isNotNull())
        .filter(F.col("duoarea").rlike(r"^S[A-Z]{2}"))
        .filter(F.col("process").isin(list(PROCESS_LABELS.keys())))
        .withColumn("year", F.col("period").cast(IntegerType()))
        .withColumn("process_label", process_map[F.col("process")])
        .withColumnRenamed("area-name", "state_name")
        .withColumn("value_mmcf", F.col("value").cast("double"))
        .select("year", "duoarea", "state_name", "process", "process_label", "value_mmcf", "units")
        .dropDuplicates()
    )

    # Clean: Crude Oil Imports
    crude_clean = (
        raw_crude
        .filter(F.col("quantity").isNotNull())
        .filter(F.col("destinationTypeName") == "Refinery State")
        .filter(F.col("originType") == "CTY")
        .withColumn("period", F.to_date(F.col("period"), "yyyy-MM"))
        .withColumn("quantity_thousand_bbl", F.col("quantity").cast("double"))
        .withColumnRenamed("destinationName", "refinery_state")
        .withColumnRenamed("originName",       "origin_country")
        .withColumnRenamed("gradeName",        "crude_grade")
        .select("period", "origin_country", "originId", "refinery_state", "destinationId", "crude_grade", "quantity_thousand_bbl")
        .dropDuplicates()
    )

    # Analyse: NG State Production
    us_ng_total = ng_clean.groupBy("period").agg(F.sum("production_mmcf").alias("us_total_mmcf"))
    ng_state_production = (
        ng_clean.join(us_ng_total, on="period", how="left")
        .withColumn("pct_us_production", F.round(F.col("production_mmcf") / F.col("us_total_mmcf") * 100, 4))
        .select("period", "duoarea", "state_name", "production_mmcf", "us_total_mmcf", "pct_us_production")
        .orderBy("period", F.col("production_mmcf").desc())
    )

    # Analyse: NG International Trade
    intl_processes = ["EEI", "IMI", "IMB"]
    ng_intl_trade_wide = (
        trade_clean.filter(F.col("process").isin(intl_processes))
        .groupBy("year", "duoarea", "state_name", "process", "process_label")
        .agg(F.sum("value_mmcf").alias("total_mmcf"))
        .groupBy("year", "duoarea", "state_name")
        .pivot("process", intl_processes).agg(F.first("total_mmcf"))
        .withColumnRenamed("EEI", "exports_mmcf")
        .withColumnRenamed("IMI", "imports_mmcf")
        .withColumnRenamed("IMB", "net_intl_mmcf")
        .orderBy("year", "state_name")
    )

    # Analyse: NG Interstate Movements
    interstate_processes = ["MID", "MIN"]
    ng_interstate_wide = (
        trade_clean.filter(F.col("process").isin(interstate_processes))
        .groupBy("year", "duoarea", "state_name", "process", "process_label")
        .agg(F.sum("value_mmcf").alias("total_mmcf"))
        .groupBy("year", "duoarea", "state_name")
        .pivot("process", interstate_processes).agg(F.first("total_mmcf"))
        .withColumnRenamed("MID", "interstate_delivered_mmcf")
        .withColumnRenamed("MIN", "net_interstate_received_mmcf")
        .orderBy("year", "state_name")
    )

    # Analyse: Crude by Origin
    crude_by_origin = (
        crude_clean.groupBy("period", "refinery_state", "origin_country")
        .agg(F.sum("quantity_thousand_bbl").alias("total_thousand_bbl"))
        .orderBy("period", "refinery_state", F.col("total_thousand_bbl").desc())
    )

    # Analyse: Crude by State
    us_crude_total = crude_clean.groupBy("period").agg(F.sum("quantity_thousand_bbl").alias("us_total_thousand_bbl"))
    crude_by_state = (
        crude_clean.groupBy("period", "refinery_state")
        .agg(F.sum("quantity_thousand_bbl").alias("total_thousand_bbl"))
        .join(us_crude_total, on="period", how="left")
        .withColumn("pct_us_imports", F.round(F.col("total_thousand_bbl") / F.col("us_total_thousand_bbl") * 100, 4))
        .orderBy("period", F.col("total_thousand_bbl").desc())
    )

    # Analyse: Crude Grade Breakdown
    state_month_totals = crude_clean.groupBy("period", "refinery_state").agg(F.sum("quantity_thousand_bbl").alias("state_total_thousand_bbl"))
    crude_grade_breakdown = (
        crude_clean.groupBy("period", "refinery_state", "crude_grade")
        .agg(F.sum("quantity_thousand_bbl").alias("grade_quantity_thousand_bbl"))
        .join(state_month_totals, on=["period", "refinery_state"], how="left")
        .withColumn("pct_of_state_imports", F.round(F.col("grade_quantity_thousand_bbl") / F.col("state_total_thousand_bbl") * 100, 4))
        .orderBy("period", "refinery_state", F.col("grade_quantity_thousand_bbl").desc())
    )

    # Write Parquet to S3
    def write_parquet(df, path, label):
        print(f"Writing {label}: {path}")
        df.write.mode("overwrite").option("compression", "snappy").parquet(path)
        print(f"Done")

    write_parquet(ng_state_production,   S3_NG_PRODUCTION,   "Natural Gas State Production")
    write_parquet(ng_intl_trade_wide,    S3_NG_INTL_TRADE,   "Natural Gas International Trade")
    write_parquet(ng_interstate_wide,    S3_NG_INTERSTATE,   "Natural Gas Interstate Movements")
    write_parquet(crude_by_origin,       S3_CRUDE_BY_ORIGIN, "Crude Imports by State & Country")
    write_parquet(crude_by_state,        S3_CRUDE_STATE,     "Crude Imports by State")
    write_parquet(crude_grade_breakdown, S3_CRUDE_GRADE,     "Crude Imports Grade Breakdown")

    spark.stop()
    print("natural_gas_crude processing complete")