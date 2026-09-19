from datetime import datetime, timedelta

import json
import os
import re

from airflow import DAG

from airflow.exceptions import (
    AirflowSkipException
)

from airflow.operators.bash import (
    BashOperator
)

from airflow.operators.python import (
    PythonOperator
)


KAFKA_BROKER = os.getenv(
    "KAFKA_BROKER",
    "kafka:29092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "traffic-telemetry"
)

KAFKA_GROUP = os.getenv(
    "KAFKA_BATCH_GROUP",
    "traffic-batch-consumer"
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


def check_kafka_connection():

    from kafka import KafkaConsumer


    consumer = None


    try:

        consumer = KafkaConsumer(

            bootstrap_servers=
                KAFKA_BROKER,

            request_timeout_ms=5000,



        )


        topics = consumer.topics()


        if KAFKA_TOPIC not in topics:

            raise RuntimeError(

                f"Kafka topic "
                f"'{KAFKA_TOPIC}' "
                f"does not exist."

            )


        print(

            "Kafka connection OK. "
            f"Available topics: "
            f"{sorted(topics)}"

        )


    except Exception as exc:

        raise RuntimeError(

            f"Kafka health check failed: "
            f"{exc}"

        ) from exc


    finally:

        if consumer:

            consumer.close()


def extract_kafka_to_json():

    from kafka import KafkaConsumer


    consumer = KafkaConsumer(

        KAFKA_TOPIC,

        bootstrap_servers=
            KAFKA_BROKER,

        group_id=
            KAFKA_GROUP,

        auto_offset_reset="earliest",

        enable_auto_commit=False,

        value_deserializer=lambda raw: (
            json.loads(raw.decode("utf-8"))
            if raw is not None
            else None
        ),

    )


    records = []


    try:

        msg_pack = consumer.poll(

            timeout_ms=5000,

            max_records=5000

        )


        for _, messages in msg_pack.items():

            for message in messages:

                if message.value is None:
                    continue

                records.append(
                    message.value
                )


        if not records:

            print(
                "No new Kafka records "
                "available for this batch."
            )

            raise AirflowSkipException(

                "No new Kafka records available."

            )


        raw_path = batch_raw_path()


        os.makedirs(

            os.path.dirname(
                raw_path
            ),

            exist_ok=True

        )


        temp_path = (
            f"{raw_path}.tmp"
        )


        with open(

            temp_path,

            "w",

            encoding="utf-8"

        ) as raw_file:


            for record in records:

                raw_file.write(

                    json.dumps(

                        record,

                        ensure_ascii=False

                    )

                    + "\n"

                )


        os.replace(

            temp_path,

            raw_path

        )


        consumer.commit()


        print(

            f"Extracted "
            f"{len(records)} records "
            f"into {raw_path}. "
            f"Kafka offsets committed."

        )


    except AirflowSkipException:

        raise


    except Exception:

        raw_path = batch_raw_path()


        temp_path = (
            f"{raw_path}.tmp"
        )


        if os.path.exists(
            temp_path
        ):

            os.remove(
                temp_path
            )


        raise


    finally:

        consumer.close()


default_args = {

    "owner":
        "data_engineering_team",

    "depends_on_past":
        False,

    "start_date":
        datetime(
            2026,
            1,
            1
        ),

    "email_on_failure":
        False,

    "retries":
        1,

    "retry_delay":
        timedelta(
            minutes=1
        ),

}


with DAG(

    dag_id=
        "traffic_hybrid_monitoring_pipeline",

    default_args=
        default_args,

    description=
        "Hybrid traffic pipeline: "
        "Kafka real-time path + "
        "Airflow/PySpark batch path",

    schedule=
        "@hourly",

    catchup=False,

    max_active_runs=1,

    tags=[
        "traffic",
        "kafka",
        "pyspark",
        "batch"
    ],

) as dag:


    check_kafka = PythonOperator(

        task_id=
            "check_kafka_health",

        python_callable=
            check_kafka_connection,

    )


    extract_batch = PythonOperator(

        task_id=
            "extract_kafka_to_json",

        python_callable=
            extract_kafka_to_json,

    )


    transform = BashOperator(

        task_id=
            "run_pyspark_transformation",

        bash_command=
            "python3 "
            "/opt/airflow/scripts/"
            "spark_process_traffic.py",

    )


    quality = BashOperator(

        task_id=
            "run_data_quality_checks",

        bash_command=
            "python3 "
            "/opt/airflow/scripts/"
            "dq_checks.py",

    )


    (
        check_kafka
        >>
        extract_batch
        >>
        transform
        >>
        quality
    )