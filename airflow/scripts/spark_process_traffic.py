import os
import re

from datetime import datetime, timezone

import psycopg2

from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    avg,
    col,
    count,
    lit,
    max as _max,
    min as _min,
    round,
    sum as _sum,
    to_timestamp,
    when,
)


DATA_DIR = "/opt/airflow/data"


def batch_raw_path():

    run_id = os.getenv(
        "AIRFLOW_CTX_DAG_RUN_ID",
        "manual_batch"
    )

    safe_run_id = re.sub(

        r"[^A-Za-z0-9_.-]+",

        "_",

        run_id

    )

    return os.path.join(

        DATA_DIR,

        f"raw_{safe_run_id}.json"

    )


RAW_PATH = batch_raw_path()


DB_HOST = os.getenv(
    "DB_HOST",
    "postgres"
)

DB_NAME = os.getenv(
    "DB_NAME",
    "traffic_dwh"
)

DB_USER = os.getenv(
    "DB_USER",
    "airflow"
)

DB_PASSWORD = os.getenv(
    "DB_PASSWORD",
    "airflow_password"
)

DB_PORT = int(
    os.getenv(
        "DB_PORT",
        "5432"
    )
)


BATCH_ID = os.getenv(

    "AIRFLOW_CTX_DAG_RUN_ID",

    datetime.now(
        timezone.utc
    ).strftime(
        "manual_%Y%m%dT%H%M%S"
    )

)


JDBC_URL = (

    f"jdbc:postgresql://"
    f"{DB_HOST}:"
    f"{DB_PORT}/"
    f"{DB_NAME}"

)


DB_PROPERTIES = {

    "user":
        DB_USER,

    "password":
        DB_PASSWORD,

    "driver":
        "org.postgresql.Driver",

}


def clear_staging(
    batch_id
):

    conn = psycopg2.connect(

        host=DB_HOST,

        database=DB_NAME,

        user=DB_USER,

        password=DB_PASSWORD,

        port=DB_PORT,

    )


    try:

        with conn.cursor() as cur:

            cur.execute(

                """

                DELETE FROM
                    staging_traffic_summary

                WHERE batch_id = %s;

                """,

                (
                    batch_id,
                )

            )


        conn.commit()


    finally:

        conn.close()


def publish_batch(
    batch_id
):

    conn = psycopg2.connect(

        host=DB_HOST,

        database=DB_NAME,

        user=DB_USER,

        password=DB_PASSWORD,

        port=DB_PORT,

    )


    try:

        with conn.cursor() as cur:

            cur.execute(

                """

                DELETE FROM
                    hourly_traffic_summary

                WHERE batch_id = %s;

                """,

                (
                    batch_id,
                )

            )


            cur.execute(

                """

                INSERT INTO
                    hourly_traffic_summary

                (
                    batch_id,
                    camera_id,
                    location,
                    latitude,
                    longitude,
                    total_vehicles,
                    avg_speed,
                    speeding_violations,
                    congestion_level,
                    window_start,
                    window_end,
                    processed_at
                )

                SELECT

                    batch_id,
                    camera_id,
                    location,
                    latitude,
                    longitude,
                    total_vehicles,
                    avg_speed,
                    speeding_violations,
                    congestion_level,
                    window_start,
                    window_end,
                    CURRENT_TIMESTAMP

                FROM staging_traffic_summary

                WHERE batch_id = %s;

                """,

                (
                    batch_id,
                )

            )


            inserted = cur.rowcount


            cur.execute(

                """

                DELETE FROM
                    staging_traffic_summary

                WHERE batch_id = %s;

                """,

                (
                    batch_id,
                )

            )


        conn.commit()


        return inserted


    except Exception:

        conn.rollback()

        raise


    finally:

        conn.close()


def run_spark_batch():

    print(

        "🚀 Starting PySpark "
        f"batch processing | "
        f"batch_id={BATCH_ID}"

    )


    if not os.path.exists(
        RAW_PATH
    ):

        raise FileNotFoundError(

            f"Raw batch file not found: "
            f"{RAW_PATH}"

        )


    spark = (

        SparkSession.builder

        .appName(
            "TrafficTelemetryBatchProcessing"
        )

        .config(

            "spark.jars.packages",

            "org.postgresql:"
            "postgresql:42.6.0"

        )

        .getOrCreate()

    )


    spark.sparkContext.setLogLevel(
        "WARN"
    )


    try:

        df = spark.read.json(
            RAW_PATH
        )


        if df.rdd.isEmpty():

            raise ValueError(

                "Raw batch file "
                "contains no records."

            )


        required = [

            "event_id",
            "camera_id",
            "location",
            "speed_kmh",
            "is_speeding",

        ]


        missing = [

            name

            for name in required

            if name not in df.columns

        ]


        if missing:

            raise ValueError(

                f"Missing required columns: "
                f"{missing}"

            )


        if "latitude" not in df.columns:

            df = df.withColumn(

                "latitude",

                lit(None).cast(
                    "double"
                )

            )


        if "longitude" not in df.columns:

            df = df.withColumn(

                "longitude",

                lit(None).cast(
                    "double"
                )

            )


        if "timestamp" not in df.columns:

            df = df.withColumn(

                "timestamp",

                lit(None).cast(
                    "string"
                )

            )


        clean_df = (

            df.select(

                "event_id",
                "timestamp",
                "camera_id",
                "location",
                "latitude",
                "longitude",
                "speed_kmh",
                "is_speeding",

            )

            .dropna(

                subset=[
                    "event_id",
                    "camera_id",
                    "location",
                    "speed_kmh",
                ]

            )

            .withColumn(

                "speed_kmh",

                col(
                    "speed_kmh"
                ).cast(
                    "double"
                )

            )

            .withColumn(

                "event_ts",

                to_timestamp(
                    col("timestamp")
                )

            )

            .filter(

                (
                    col("speed_kmh")
                    >= 0
                )

                &

                (
                    col("speed_kmh")
                    <= 250
                )

            )

        )


        if clean_df.rdd.isEmpty():

            raise ValueError(

                "No valid records remained "
                "after basic validation."

            )


        aggregated_df = (

            clean_df

            .groupBy(

                "camera_id",
                "location",
                "latitude",
                "longitude"

            )

            .agg(

                count(
                    "event_id"
                ).alias(
                    "total_vehicles"
                ),

                round(

                    avg(
                        "speed_kmh"
                    ),

                    2

                ).alias(
                    "avg_speed"
                ),

                _sum(

                    when(

                        col(
                            "is_speeding"
                        ) == True,

                        1

                    ).otherwise(0)

                ).alias(
                    "speeding_violations"
                ),

                _min(
                    "event_ts"
                ).alias(
                    "window_start"
                ),

                _max(
                    "event_ts"
                ).alias(
                    "window_end"
                ),

            )

            .withColumn(

                "congestion_level",

                when(

                    col(
                        "total_vehicles"
                    ) > 50,

                    "HIGH"

                )

                .when(

                    col(
                        "total_vehicles"
                    ) > 20,

                    "MEDIUM"

                )

                .otherwise(
                    "LOW"
                )

            )

            .withColumn(

                "batch_id",

                lit(BATCH_ID)

            )

            .select(

                "batch_id",
                "camera_id",
                "location",
                "latitude",
                "longitude",
                "total_vehicles",
                "avg_speed",
                "speeding_violations",
                "congestion_level",
                "window_start",
                "window_end",

            )

        )


        print(
            "📊 Batch aggregation:"
        )


        aggregated_df.show(
            truncate=False
        )


        clear_staging(
            BATCH_ID
        )


        (

            aggregated_df.write

            .mode("append")

            .jdbc(

                url=JDBC_URL,

                table=
                    "staging_traffic_summary",

                properties=
                    DB_PROPERTIES,

            )

        )


        inserted = publish_batch(
            BATCH_ID
        )


        print(

            f"✅ Published "
            f"{inserted} summary rows "
            f"to PostgreSQL DWH."

        )


    finally:

        spark.stop()


if __name__ == "__main__":

    run_spark_batch()