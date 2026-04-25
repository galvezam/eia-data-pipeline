# dags/petroleum_coal_electricity_core.py
"""
Extracted logic from petroleum_coal_electricity_total.ipynb.
"""


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
        .appName("EIA Petroleum, Coal, Electricity & Total Energy Processing")
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
    """
    import boto3
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType
    from pyspark.sql.window import Window

    spark = _build_spark(aws_access_key, aws_secret_key, aws_region)

    OUT = f"s3a://{bucket_name}/processed"


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

    PETROLEUM_PATH           = all_s3_csvs("raw/petroleum/weekly/")
    PETROLEUM_MOVEMENTS_PATH = all_s3_csvs("raw/petroleum_movements/weekly/")
    COAL_PATH                = all_s3_csvs("raw/coal/annual/")
    ELECTRICITY_PATH         = all_s3_csvs("raw/electricity/monthly/")
    ELEC_RANKINGS_PATH       = all_s3_csvs("raw/electricity_state_rankings/annual/")
    ELEC_NET_METERING_PATH   = all_s3_csvs("raw/electricity_net_metering/annual/")
    ELEC_CAPACITY_PATH       = all_s3_csvs("raw/electricity_generating_capacity/annual/")
    TOTAL_ENERGY_PATH        = all_s3_csvs("raw/total_energy/monthly/")
    SEDS_PATH                = all_s3_csvs("raw/seds/annual/")

    for label, path in [
        ("petroleum",           PETROLEUM_PATH),
        ("petroleum_movements", PETROLEUM_MOVEMENTS_PATH),
        ("coal",                COAL_PATH),
        ("electricity",         ELECTRICITY_PATH),
        ("elec_rankings",       ELEC_RANKINGS_PATH),
        ("elec_net_metering",   ELEC_NET_METERING_PATH),
        ("elec_capacity",       ELEC_CAPACITY_PATH),
        ("total_energy",        TOTAL_ENERGY_PATH),
        ("seds",                SEDS_PATH),
    ]:
        print(f"{label:<25}: {path}")

    # S3 output paths
    S3_PETROLEUM_PRODUCTION  = f"{OUT}/petroleum_production/"
    S3_PETROLEUM_MOVEMENTS   = f"{OUT}/petroleum_movements/"
    S3_COAL_TRADE            = f"{OUT}/coal_trade/"
    S3_ELECTRICITY_BY_FUEL   = f"{OUT}/electricity_by_fuel_state/"
    S3_ELECTRICITY_RANKINGS  = f"{OUT}/electricity_state_rankings/"
    S3_ELECTRICITY_NET_METER = f"{OUT}/electricity_net_metering/"
    S3_ELECTRICITY_CAPACITY  = f"{OUT}/electricity_generating_capacity/"
    S3_TOTAL_ENERGY          = f"{OUT}/total_energy/"
    S3_SEDS_STATE_CONSUMPTION= f"{OUT}/seds_state_consumption/"
    S3_SEDS_STATE_PCT        = f"{OUT}/seds_state_pct_us/"
    S3_SEDS_FUEL_PIVOT       = f"{OUT}/seds_fuel_pivot/"

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

    raw_petroleum       = read_csv(PETROLEUM_PATH)
    raw_petro_movements = read_csv(PETROLEUM_MOVEMENTS_PATH)
    raw_coal            = read_csv(COAL_PATH)
    raw_electricity     = read_csv(ELECTRICITY_PATH)
    raw_elec_rankings   = read_csv(ELEC_RANKINGS_PATH)
    raw_elec_net_meter  = read_csv(ELEC_NET_METERING_PATH)
    raw_elec_capacity   = read_csv(ELEC_CAPACITY_PATH)
    raw_total_energy    = read_csv(TOTAL_ENERGY_PATH)
    raw_seds            = read_csv(SEDS_PATH)

    # Clean: Petroleum Production
    petroleum_clean = (
        raw_petroleum
        .filter(F.col("value").isNotNull())
        .withColumn("period", F.to_date(F.col("period"), "yyyy-MM-dd"))
        .withColumn("value_kbd", F.col("value").cast("double"))
        .withColumnRenamed("area-name", "area_name")
        .withColumn("area_name",         F.trim(F.col("area_name")))
        .withColumn("product_name",      F.trim(F.col("product-name")))
        .withColumn("process_name",      F.trim(F.col("process-name")))
        .withColumn("series_description",F.trim(F.col("series-description")))
        .select(
            "period", "duoarea", "area_name",
            F.col("product").alias("product_id"), "product_name",
            F.col("process").alias("process_id"), "process_name",
            F.col("series").alias("series_id"),  "series_description",
            "value_kbd", "units"
        )
        .dropDuplicates()
        .orderBy("period", "duoarea", "product_id")
    )

    # Clean: Petroleum Movements
    PETRO_MOVEMENT_LABELS = {
        "EEX": "Exports", "EIM": "Imports", "ENT": "Net Imports",
        "STK": "Stock Change", "VUA": "Supply Adjustment", "YPA": "Net Production",
    }
    petro_process_map = F.create_map(*[item for pair in [(F.lit(k), F.lit(v)) for k, v in PETRO_MOVEMENT_LABELS.items()] for item in pair])

    petro_movements_clean = (
        raw_petro_movements
        .filter(F.col("value").isNotNull())
        .withColumn("period", F.to_date(F.col("period"), "yyyy-MM-dd"))
        .withColumn("value_kbd", F.col("value").cast("double"))
        .withColumn("process_label", F.coalesce(petro_process_map[F.col("process")], F.trim(F.col("process-name"))))
        .withColumnRenamed("area-name", "area_name")
        .withColumn("area_name",         F.trim(F.col("area_name")))
        .withColumn("product_name",      F.trim(F.col("product-name")))
        .withColumn("series_description",F.trim(F.col("series-description")))
        .select(
            "period", "duoarea", "area_name",
            F.col("product").alias("product_id"), "product_name",
            F.col("process").alias("process_id"), "process_label",
            F.col("series").alias("series_id"),  "series_description",
            "value_kbd", "units"
        )
        .dropDuplicates()
        .orderBy("period", "duoarea", "product_id")
    )

    # Clean: Coal
    coal_clean = (
        raw_coal
        .filter(F.col("quantity").isNotNull() | F.col("price").isNotNull())
        .withColumn("year",               F.col("period").cast(IntegerType()))
        .withColumn("quantity_short_tons",F.col("quantity").cast("double"))
        .withColumn("price_usd_per_ton",  F.col("price").cast("double"))
        .withColumn("export_import_type", F.trim(F.col("exportImportType")))
        .withColumn("coal_rank_id",       F.trim(F.col("coalRankId")))
        .withColumn("coal_rank_desc",     F.trim(F.col("coalRankDescription")))
        .withColumn("country_id",         F.trim(F.col("countryId")))
        .withColumn("country_desc",       F.trim(F.col("countryDescription")))
        .withColumn("customs_district_id",F.trim(F.col("customsDistrictId")))
        .withColumn("customs_district",   F.trim(F.col("customsDistrictDescription")))
        .filter(F.col("country_id") != "TOT")
        .select("year", "export_import_type", "coal_rank_id", "coal_rank_desc",
                "country_id", "country_desc", "customs_district_id", "customs_district",
                "quantity_short_tons", "price_usd_per_ton")
        .dropDuplicates()
        .orderBy("year", "export_import_type", "country_id")
    )

    us_coal_total = (
        coal_clean.groupBy("year", "export_import_type", "coal_rank_id")
        .agg(F.sum("quantity_short_tons").alias("us_total_short_tons"),
             F.avg("price_usd_per_ton").alias("us_avg_price_usd_per_ton"))
    )
    coal_trade_summary = (
        coal_clean
        .groupBy("year", "export_import_type", "coal_rank_id", "coal_rank_desc",
                 "country_id", "country_desc", "customs_district_id", "customs_district")
        .agg(F.sum("quantity_short_tons").alias("total_short_tons"),
             F.avg("price_usd_per_ton").alias("avg_price_usd_per_ton"),
             F.min("price_usd_per_ton").alias("min_price_usd_per_ton"),
             F.max("price_usd_per_ton").alias("max_price_usd_per_ton"))
        .join(us_coal_total, on=["year", "export_import_type", "coal_rank_id"], how="left")
        .withColumn("pct_us_direction", F.round(F.col("total_short_tons") / F.col("us_total_short_tons") * 100, 4))
        .orderBy("year", "export_import_type", F.col("total_short_tons").desc())
    )

    # Clean: Electricity
    FUEL_TYPE_LABELS = {
        "COL": "Coal", "NG": "Natural Gas", "NUC": "Nuclear",
        "HYC": "Conventional Hydroelectric", "WND": "Wind", "SUN": "Solar",
        "GEO": "Geothermal", "ALL": "All Fuels", "AOR": "All Renewables",
    }
    fuel_map = F.create_map(*[item for pair in [(F.lit(k), F.lit(v)) for k, v in FUEL_TYPE_LABELS.items()] for item in pair])

    electricity_clean = (
        raw_electricity
        .filter(F.col("consumption-for-eg").isNotNull() | F.col("consumption-for-eg-btu").isNotNull())
        .filter(F.col("fueltypeid").isin(list(FUEL_TYPE_LABELS.keys())))
        .withColumn("period", F.to_date(F.col("period"), "yyyy-MM"))
        .withColumn("consumption_thousand_mwh", F.col("consumption-for-eg").cast("double"))
        .withColumn("consumption_btu",          F.col("consumption-for-eg-btu").cast("double"))
        .withColumn("fuel_type_label", F.coalesce(fuel_map[F.col("fueltypeid")], F.col("fueltypeid")))
        .withColumn("state_name", F.trim(F.col("stateDescription")))
        .select("period", "state_name", F.col("fueltypeid").alias("fuel_type_id"), "fuel_type_label",
                "consumption_thousand_mwh", "consumption_btu")
        .dropDuplicates()
        .orderBy("period", "fuel_type_id")
    )

    us_elec_by_fuel = (
        electricity_clean.filter(F.col("fuel_type_id") != "ALL")
        .groupBy("period", "fuel_type_id", "fuel_type_label")
        .agg(F.sum("consumption_thousand_mwh").alias("us_fuel_thousand_mwh"))
    )
    elec_state_by_fuel = (
        electricity_clean.filter(F.col("fuel_type_id") != "ALL")
        .join(us_elec_by_fuel.select("period", "fuel_type_id", "us_fuel_thousand_mwh"), on=["period", "fuel_type_id"], how="left")
        .withColumn("pct_us_fuel_consumption", F.round(F.col("consumption_thousand_mwh") / F.col("us_fuel_thousand_mwh") * 100, 4))
        .select("period", "state_name", "fuel_type_id", "fuel_type_label",
                "consumption_thousand_mwh", "consumption_btu", "us_fuel_thousand_mwh", "pct_us_fuel_consumption")
        .orderBy("period", "state_name", "fuel_type_id")
    )

    # Clean: Electricity State Rankings
    RANKING_COLS = [
        "average-retail-price-rank", "carbon-dioxide-rank",
        "net-generation-rank", "nitrogen-oxide-rank", "sulfer-dioxide-rank",
    ]
    elec_rankings_clean = raw_elec_rankings
    for col in RANKING_COLS:
        elec_rankings_clean = elec_rankings_clean.withColumn(col.replace("-", "_"), F.col(f"`{col}`").cast(IntegerType()))
    elec_rankings_clean = (
        elec_rankings_clean
        .filter(F.greatest(*[F.col(c.replace("-", "_")).isNotNull() for c in RANKING_COLS]))
        .withColumn("year", F.col("period").cast(IntegerType()))
        .withColumn("state_id",   F.trim(F.col("stateID")))
        .withColumn("state_name", F.trim(F.col("stateDescription")))
        .select("year", "state_id", "state_name", *[c.replace("-", "_") for c in RANKING_COLS])
        .dropDuplicates().orderBy("year", "state_id")
    )

    # Clean: Electricity Net Metering
    elec_net_meter_clean = (
        raw_elec_net_meter
        .filter(F.col("capacity").isNotNull() | F.col("customers").isNotNull())
        .withColumn("year",            F.col("period").cast(IntegerType()))
        .withColumn("capacity_mw",     F.col("capacity").cast("double"))
        .withColumn("customers_count", F.col("customers").cast("double").cast(IntegerType()))
        .withColumn("state_id",   F.trim(F.col("state")))
        .withColumn("state_name", F.trim(F.col("stateName")))
        .filter(F.col("state_id").rlike(r"^[A-Z]{2}$"))
        .select("year", "state_id", "state_name", "sector", "sectorName", "capacity_mw", "customers_count")
        .dropDuplicates().orderBy("year", "state_id")
    )

    # Clean: Electricity Generating Capacity
    elec_capacity_clean = (
        raw_elec_capacity
        .filter(F.col("capability").isNotNull())
        .withColumn("year",         F.col("period").cast(IntegerType()))
        .withColumn("capability_mw",F.col("capability").cast("double"))
        .withColumn("state_id",   F.trim(F.col("stateID")))
        .withColumn("state_name", F.trim(F.col("stateDescription")))
        .filter(F.col("state_id").rlike(r"^[A-Z]{2}$"))
        .select("year", "state_id", "state_name", "producertypeid",
                F.col("producerTypeDescription").alias("producer_type"),
                "energysourceid", F.col("energySourceDescription").alias("energy_source"), "capability_mw")
        .dropDuplicates().orderBy("year", "state_id", "energysourceid")
    )
    us_capacity_total = elec_capacity_clean.groupBy("year", "energy_source").agg(F.sum("capability_mw").alias("us_total_mw"))
    elec_capacity_state = (
        elec_capacity_clean.groupBy("year", "state_id", "state_name", "energy_source")
        .agg(F.sum("capability_mw").alias("state_total_mw"), F.countDistinct("producer_type").alias("num_producer_types"))
        .join(us_capacity_total, on=["year", "energy_source"], how="left")
        .withColumn("pct_us_capacity", F.round(F.col("state_total_mw") / F.col("us_total_mw") * 100, 4))
        .select("year", "state_id", "state_name", "energy_source",
                "state_total_mw", "us_total_mw", "pct_us_capacity", "num_producer_types")
        .orderBy("year", "state_id", "energy_source")
    )

    # Clean: Total Energy
    total_energy_clean = (
        raw_total_energy
        .filter(F.col("value").isNotNull())
        .withColumn("period", F.to_date(F.col("period"), "yyyy-MM"))
        .withColumn("value_quad_btu", F.col("value").cast("double"))
        .withColumn("series-description", F.trim(F.col("seriesDescription")))
        .select("period", F.col("msn").alias("series_code"),
                F.col("series-description").alias("series_description"), "value_quad_btu", "unit")
        .dropDuplicates().orderBy("period", "series_code")
    )

    # Clean: SEDS
    SEDS_FUEL_PREFIXES = {
        "CO": "Coal", "PA": "Petroleum", "NG": "Natural Gas", "ES": "Electricity",
        "NU": "Nuclear", "RE": "Renewables", "TE": "Total Energy", "GE": "Geothermal",
        "HY": "Hydroelectric", "SO": "Solar", "WY": "Wind", "WD": "Wood", "WS": "Waste",
    }
    seds_clean = (
        raw_seds
        .filter(F.col("value").isNotNull())
        .filter(F.col("stateId").rlike(r"^[A-Z]{2}$"))
        .filter(F.col("stateId") != "US")
        .withColumn("year",    F.col("period").cast(IntegerType()))
        .withColumn("value_d", F.col("value").cast("double"))
        .filter(F.col("value_d").isNotNull())
        .withColumn("fuel_prefix",         F.substring(F.col("seriesId"), 1, 2))
        .withColumn("sector_code",         F.substring(F.col("seriesId"), 3, 2))
        .withColumn("unit_type",           F.substring(F.col("seriesId"), 5, 1))
        .withColumn("state_name",          F.trim(F.col("stateDescription")))
        .withColumn("series_description",  F.trim(F.col("seriesDescription")))
        .select("year", F.col("stateId").alias("state_id"), "state_name",
                F.col("seriesId").alias("series_code"), "series_description",
                "fuel_prefix", "sector_code", "unit_type", F.col("value_d").alias("value"), "unit")
        .dropDuplicates().orderBy("year", "state_id", "series_code")
    )

    seds_state_fuel = (
        seds_clean.filter(F.col("unit_type") == "B")
        .groupBy("year", "state_id", "state_name", "fuel_prefix")
        .agg(F.sum("value").alias("total_btu"))
    )
    us_seds_total = seds_state_fuel.groupBy("year", "fuel_prefix").agg(F.sum("total_btu").alias("us_total_btu"))
    seds_state_consumption = (
        seds_state_fuel.join(us_seds_total, on=["year", "fuel_prefix"], how="left")
        .withColumn("pct_us_fuel", F.round(F.col("total_btu") / F.col("us_total_btu") * 100, 4))
        .orderBy("year", "state_id", "fuel_prefix")
    )

    us_seds_all = seds_state_fuel.filter(F.col("fuel_prefix") == "TE").groupBy("year").agg(F.sum("total_btu").alias("us_all_btu"))
    state_year_window = Window.partitionBy("state_id").orderBy("year")
    seds_state_pct_us = (
        seds_state_fuel.filter(F.col("fuel_prefix") == "TE")
        .join(us_seds_all, on="year", how="left")
        .withColumn("pct_us_total",     F.round(F.col("total_btu") / F.col("us_all_btu") * 100, 4))
        .withColumn("yoy_change_btu",   F.col("total_btu") - F.lag("total_btu", 1).over(state_year_window))
        .withColumn("yoy_change_pct",   F.round(F.col("yoy_change_btu") / F.lag("total_btu", 1).over(state_year_window) * 100, 2))
        .select("year", "state_id", "state_name", "total_btu", "us_all_btu", "pct_us_total", "yoy_change_btu", "yoy_change_pct")
        .orderBy("year", F.col("total_btu").desc())
    )

    fuel_prefixes = [r["fuel_prefix"] for r in seds_state_fuel.select("fuel_prefix").distinct().orderBy("fuel_prefix").collect()]
    seds_fuel_pivot = (
        seds_state_fuel.groupBy("year", "state_id", "state_name")
        .pivot("fuel_prefix", fuel_prefixes).agg(F.first("total_btu"))
        .orderBy("year", "state_id")
    )
    if "TE" in fuel_prefixes and "TE" in seds_fuel_pivot.columns:
        for prefix in fuel_prefixes:
            if prefix != "TE":
                seds_fuel_pivot = seds_fuel_pivot.withColumn(
                    f"{prefix.lower()}_pct", F.round(F.col(f"`{prefix}`") / F.col("TE") * 100, 2)
                )

    # Petroleum Movements Wide
    MOVEMENT_PROCESSES = ["EEX", "EIM", "ENT"]
    petro_import_export = (
        petro_movements_clean.filter(F.col("process_id").isin(MOVEMENT_PROCESSES))
        .groupBy("period", "duoarea", "area_name", "product_id", "product_name", "process_id")
        .agg(F.sum("value_kbd").alias("total_kbd"))
    )
    petro_movements_wide = (
        petro_import_export.groupBy("period", "duoarea", "area_name", "product_id", "product_name")
        .pivot("process_id", MOVEMENT_PROCESSES).agg(F.first("total_kbd"))
        .withColumnRenamed("EEX", "exports_kbd")
        .withColumnRenamed("EIM", "imports_kbd")
        .withColumnRenamed("ENT", "net_imports_kbd")
        .withColumn("trade_balance_kbd", F.col("exports_kbd") - F.col("imports_kbd"))
        .orderBy("period", "duoarea", "product_id")
    )

    # Write Parquet to S3
    def write_parquet(df, path, label):
        print(f"Writing {label}: {path}")
        df.write.mode("overwrite").option("compression", "snappy").parquet(path)
        print(f"Done")

    write_parquet(petroleum_clean,       S3_PETROLEUM_PRODUCTION,          "Petroleum Production")
    write_parquet(petro_movements_clean, S3_PETROLEUM_MOVEMENTS,           "Petroleum Movements")
    write_parquet(coal_clean,            S3_COAL_TRADE,                    "Coal Trade")
    write_parquet(elec_rankings_clean,   S3_ELECTRICITY_RANKINGS,          "Electricity State Rankings")
    write_parquet(elec_net_meter_clean,  S3_ELECTRICITY_NET_METER,         "Electricity Net Metering")
    write_parquet(total_energy_clean,    S3_TOTAL_ENERGY,                  "Total Energy")
    write_parquet(elec_state_by_fuel,    S3_ELECTRICITY_BY_FUEL,           "Electricity by Fuel & State")
    write_parquet(elec_capacity_state,   S3_ELECTRICITY_CAPACITY,          "Electricity Generating Capacity")
    write_parquet(coal_trade_summary,    S3_COAL_TRADE + "summary/",       "Coal Trade Summary")
    write_parquet(seds_state_consumption,S3_SEDS_STATE_CONSUMPTION,        "SEDS State Consumption")
    write_parquet(seds_state_pct_us,     S3_SEDS_STATE_PCT,                "SEDS State % of US")
    write_parquet(seds_fuel_pivot,       S3_SEDS_FUEL_PIVOT,               "SEDS Fuel Pivot")
    write_parquet(petro_movements_wide,  S3_PETROLEUM_MOVEMENTS + "wide/", "Petroleum Movements Wide")

    spark.stop()
    print("petroleum_coal_electricity processing complete")