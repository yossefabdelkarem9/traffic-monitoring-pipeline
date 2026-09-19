import os
import sys

import psycopg2


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
    "AIRFLOW_CTX_DAG_RUN_ID"
)


def run_data_quality_checks():

    if not BATCH_ID:

        raise RuntimeError(

            "AIRFLOW_CTX_DAG_RUN_ID "
            "is required for "
            "batch-level DQ checks."

        )


    print(

        "🧪 Starting Data Quality Checks | "
        f"batch_id={BATCH_ID}"

    )


    conn = psycopg2.connect(

        host=DB_HOST,

        database=DB_NAME,

        user=DB_USER,

        password=DB_PASSWORD,

        port=DB_PORT,

    )


    try:

        with conn.cursor() as cursor:


            # --------------------------------------------------
            # Check 1
            # --------------------------------------------------

            cursor.execute(

                """

                SELECT COUNT(*)

                FROM hourly_traffic_summary

                WHERE batch_id = %s;

                """,

                (
                    BATCH_ID,
                )

            )


            row_count = cursor.fetchone()[0] # type: ignore


            if row_count == 0:

                raise ValueError(

                    "DQ failed: "
                    "current batch produced "
                    "no warehouse rows."

                )


            print(

                f"✅ Check 1: "
                f"{row_count} summary rows "
                f"exist for the current batch."

            )


            # --------------------------------------------------
            # Check 2
            # --------------------------------------------------

            cursor.execute(

                """

                SELECT COUNT(*)

                FROM hourly_traffic_summary

                WHERE batch_id = %s

                AND (

                    camera_id IS NULL

                    OR location IS NULL

                    OR total_vehicles IS NULL

                    OR avg_speed IS NULL

                    OR speeding_violations IS NULL

                    OR congestion_level IS NULL

                );

                """,

                (
                    BATCH_ID,
                )

            )


            null_rows = cursor.fetchone()[0]  # type: ignore


            if null_rows:

                raise ValueError(

                    f"DQ failed: "
                    f"{null_rows} rows contain "
                    f"NULL in required fields."

                )


            print(

                "✅ Check 2: "
                "Required warehouse fields "
                "contain no NULL values."

            )


            # --------------------------------------------------
            # Check 3
            # --------------------------------------------------

            cursor.execute(

                """

                SELECT COUNT(*)

                FROM hourly_traffic_summary

                WHERE batch_id = %s

                AND (

                    total_vehicles <= 0

                    OR avg_speed < 0

                    OR avg_speed > 250

                    OR speeding_violations < 0

                    OR speeding_violations >
                       total_vehicles

                );

                """,

                (
                    BATCH_ID,
                )

            )


            invalid_metrics = (
                cursor.fetchone()[0]  # type: ignore
            )


            if invalid_metrics:

                raise ValueError(

                    f"DQ failed: "
                    f"{invalid_metrics} rows "
                    f"contain invalid "
                    f"traffic metrics."

                )


            print(

                "✅ Check 3: "
                "Traffic metrics are "
                "within valid ranges."

            )


            # --------------------------------------------------
            # Check 4
            # --------------------------------------------------

            cursor.execute(

                """

                SELECT COUNT(*)

                FROM (

                    SELECT

                        camera_id,
                        location,
                        COUNT(*) AS duplicates

                    FROM hourly_traffic_summary

                    WHERE batch_id = %s

                    GROUP BY
                        camera_id,
                        location

                    HAVING COUNT(*) > 1

                ) d;

                """,

                (
                    BATCH_ID,
                )

            )


            duplicate_groups = (
                cursor.fetchone()[0]  # type: ignore
            )


            if duplicate_groups:

                raise ValueError(

                    f"DQ failed: "
                    f"{duplicate_groups} "
                    f"duplicate camera/location "
                    f"groups found."

                )


            print(

                "✅ Check 4: "
                "No duplicate "
                "camera/location "
                "summary groups."

            )


            # --------------------------------------------------
            # Check 5
            # --------------------------------------------------

            cursor.execute(

                """

                SELECT COUNT(*)

                FROM hourly_traffic_summary

                WHERE batch_id = %s

                AND congestion_level NOT IN (

                    'LOW',
                    'MEDIUM',
                    'HIGH'

                );

                """,

                (
                    BATCH_ID,
                )

            )


            invalid_congestion = (
                cursor.fetchone()[0]  # type: ignore
            )


            if invalid_congestion:

                raise ValueError(

                    f"DQ failed: "
                    f"{invalid_congestion} "
                    f"rows have invalid "
                    f"congestion levels."

                )


            print(

                "✅ Check 5: "
                "Congestion levels "
                "are valid."

            )


        print(

            "🎉 All batch-level "
            "Data Quality Checks "
            "Passed Successfully!"

        )


    finally:

        conn.close()


if __name__ == "__main__":

    try:

        run_data_quality_checks()

    except Exception as exc:

        print(

            f"❌ Data Quality Checks "
            f"Failed: {exc}"

        )

        sys.exit(1)