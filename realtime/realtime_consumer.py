import json
import logging
import os
import signal
import time

from collections import defaultdict, deque

from datetime import datetime, timezone

import psycopg2

from kafka import KafkaConsumer


KAFKA_BROKER = os.getenv(
    "KAFKA_BROKER",
    "kafka:29092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "traffic-telemetry"
)

KAFKA_GROUP = os.getenv(
    "KAFKA_GROUP",
    "traffic-realtime-consumer"
)


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


WINDOW_SECONDS = int(
    os.getenv(
        "WINDOW_SECONDS",
        "60"
    )
)


DB_BATCH_SIZE = int(
    os.getenv(
        "DB_BATCH_SIZE",
        "50"
    )
)


logging.basicConfig(

    level=logging.INFO,

    format=
        "%(asctime)s | "
        "%(levelname)s | "
        "%(message)s"

)

logger = logging.getLogger(
    "realtime-consumer"
)


running = True


windows = defaultdict(
    deque
)


def stop_handler(
    signum,
    frame
):

    global running

    running = False

    logger.info(
        "Shutdown signal received."
    )


def parse_timestamp(
    value
):

    if not value:

        return datetime.now(
            timezone.utc
        )

    try:

        dt = datetime.fromisoformat(
            value.replace(
                "Z",
                "+00:00"
            )
        )

        if dt.tzinfo is None:

            dt = dt.replace(
                tzinfo=timezone.utc
            )

        return dt.astimezone(
            timezone.utc
        )

    except ValueError:

        return datetime.now(
            timezone.utc
        )


def validate_event(
    event
):

    required_fields = [

        "event_id",
        "timestamp",
        "camera_id",
        "location",
        "speed_kmh",
        "is_speeding",

    ]

    if any(
        field not in event
        for field in required_fields
    ):

        return False


    try:

        speed = float(
            event["speed_kmh"]
        )

    except (
        TypeError,
        ValueError
    ):

        return False


    return (
        0 <= speed <= 250
    )


def connect_db():

    while running:

        try:

            conn = psycopg2.connect(

                host=DB_HOST,

                database=DB_NAME,

                user=DB_USER,

                password=DB_PASSWORD,

                port=DB_PORT,

                connect_timeout=5,

            )

            conn.autocommit = False

            logger.info(
                "Connected to PostgreSQL."
            )

            return conn

        except Exception as exc:

            logger.warning(

                "PostgreSQL unavailable: "
                "%s. Retrying in 3s...",

                exc

            )

            time.sleep(3)

    return None


def load_recent_windows(
    conn
):

    with conn.cursor() as cur:

        cur.execute(

            """

            SELECT
                event_timestamp,
                camera_id,
                location,
                speed_kmh,
                is_speeding

            FROM realtime_traffic_events

            WHERE event_timestamp >=
                CURRENT_TIMESTAMP -
                (%s * INTERVAL '1 second')

            ORDER BY event_timestamp ASC;

            """,

            (
                WINDOW_SECONDS,
            )

        )

        rows = cur.fetchall()


    for (

        event_time,
        camera_id,
        location,
        speed,
        speeding

    ) in rows:

        windows[
            (
                camera_id,
                location
            )
        ].append(

            (
                event_time,
                float(speed),
                bool(speeding)
            )

        )


    logger.info(

        "Restored %s recent events "
        "into in-memory windows.",

        len(rows)

    )


def persist_events(
    conn,
    events
):

    if not events:

        return []


    inserted_events = []


    with conn.cursor() as cur:

        for event in events:

            cur.execute(

                """

                INSERT INTO realtime_traffic_events

                (
                    event_id,
                    event_timestamp,
                    camera_id,
                    location,
                    latitude,
                    longitude,
                    vehicle_type,
                    speed_kmh,
                    is_speeding
                )

                VALUES
                (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )

                ON CONFLICT (
                    event_id
                )

                DO NOTHING

                RETURNING event_id;

                """,

                (

                    event["event_id"],

                    parse_timestamp(
                        event.get(
                            "timestamp"
                        )
                    ),

                    event["camera_id"],

                    event["location"],

                    event.get(
                        "latitude"
                    ),

                    event.get(
                        "longitude"
                    ),

                    event.get(
                        "vehicle_type"
                    ),

                    float(
                        event["speed_kmh"]
                    ),

                    bool(
                        event["is_speeding"]
                    ),

                )

            )


            if cur.fetchone():

                inserted_events.append(
                    event
                )


    return inserted_events


def update_metric(
    conn,
    event
):

    camera_id = event[
        "camera_id"
    ]

    location = event[
        "location"
    ]

    event_time = parse_timestamp(
        event.get(
            "timestamp"
        )
    )

    speed = float(
        event["speed_kmh"]
    )

    speeding = bool(
        event["is_speeding"]
    )


    key = (
        camera_id,
        location
    )


    bucket = windows[
        key
    ]


    bucket.append(

        (
            event_time,
            speed,
            speeding
        )

    )


    cutoff = (

        event_time.timestamp()
        - WINDOW_SECONDS

    )


    while (

        bucket
        and
        bucket[0][0].timestamp()
        < cutoff

    ):

        bucket.popleft()


    count = len(
        bucket
    )


    avg_speed = (

        round(

            sum(
                item[1]
                for item in bucket
            )
            / count,

            2

        )

        if count

        else 0.0

    )


    speeding_count = sum(

        1

        for item in bucket

        if item[2]

    )


    if count > 50:

        congestion = "HIGH"

    elif count > 20:

        congestion = "MEDIUM"

    else:

        congestion = "LOW"


    with conn.cursor() as cur:

        cur.execute(

            """

            INSERT INTO realtime_traffic_metrics

            (
                camera_id,
                location,
                window_seconds,
                window_end,
                vehicles_in_window,
                avg_speed,
                speeding_violations,
                congestion_level,
                updated_at
            )

            VALUES
            (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                CURRENT_TIMESTAMP
            )

            ON CONFLICT (
                camera_id,
                location
            )

            DO UPDATE SET

                window_seconds =
                    EXCLUDED.window_seconds,

                window_end =
                    EXCLUDED.window_end,

                vehicles_in_window =
                    EXCLUDED.vehicles_in_window,

                avg_speed =
                    EXCLUDED.avg_speed,

                speeding_violations =
                    EXCLUDED.speeding_violations,

                congestion_level =
                    EXCLUDED.congestion_level,

                updated_at =
                    CURRENT_TIMESTAMP;

            """,

            (

                camera_id,

                location,

                WINDOW_SECONDS,

                event_time,

                count,

                avg_speed,

                speeding_count,

                congestion,

            )

        )


def main():

    signal.signal(
        signal.SIGTERM,
        stop_handler
    )

    signal.signal(
        signal.SIGINT,
        stop_handler
    )


    consumer = KafkaConsumer(

        KAFKA_TOPIC,

        bootstrap_servers=[
            KAFKA_BROKER
        ],

        group_id=KAFKA_GROUP,

        auto_offset_reset="latest",

        enable_auto_commit=False,

        value_deserializer=
            lambda message:  # type: ignore
                json.loads(
                    message.decode(
                        "utf-8"
                    )
                ),

        consumer_timeout_ms=1000,

        max_poll_records=
            DB_BATCH_SIZE,

    )


    logger.info(

        "Real-time consumer started | "
        "topic=%s | group=%s",

        KAFKA_TOPIC,
        KAFKA_GROUP

    )


    conn = connect_db()


    if conn is None:

        consumer.close()

        return


    load_recent_windows(
        conn
    )


    try:

        while running:

            records = consumer.poll(

                timeout_ms=1000,

                max_records=
                    DB_BATCH_SIZE

            )


            if not records:

                continue


            valid_events = []


            for _, messages in records.items():

                for message in messages:

                    event = message.value


                    if validate_event(
                        event
                    ):

                        valid_events.append(
                            event
                        )

                    else:

                        logger.warning(

                            "Dropped invalid event "
                            "at offset=%s",

                            message.offset

                        )


            if not valid_events:

                consumer.commit()

                continue


            try:

                inserted_events = persist_events(

                    conn,

                    valid_events

                )


                for event in inserted_events:

                    update_metric(

                        conn,

                        event

                    )


                conn.commit()


                consumer.commit()


                logger.info(

                    "Processed %s new "
                    "real-time events "
                    "(%s duplicates skipped).",

                    len(inserted_events),

                    (
                        len(valid_events)
                        -
                        len(inserted_events)
                    )

                )


            except Exception:

                conn.rollback()


                try:

                    conn.close()

                except Exception:

                    pass


                conn = connect_db()


                if conn is None:

                    break


                logger.exception(

                    "Failed to persist "
                    "real-time batch. "
                    "Offsets were not committed."

                )


                time.sleep(2)


    finally:

        consumer.close()


        if conn:

            conn.close()


        logger.info(
            "Real-time consumer stopped."
        )


if __name__ == "__main__":

    main()