import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


BROKER = "localhost:9092"

TOPIC = "traffic-telemetry"


producer = KafkaProducer(

    bootstrap_servers=[BROKER],

    value_serializer=lambda value:
        json.dumps(value).encode("utf-8"),

    acks="all",

    retries=5,

    linger_ms=10,
)


CAMERAS = [

    {
        "id": "CAM_01",
        "location": "Cairo_Ring_Road_KM12",
        "latitude": 30.0515,
        "longitude": 31.3375,
        "speed_limit_kmh": 90,
    },

    {
        "id": "CAM_02",
        "location": "Cairo_Alex_Desert_Road_KM45",
        "latitude": 30.1058,
        "longitude": 31.0116,
        "speed_limit_kmh": 100,
    },

    {
        "id": "CAM_03",
        "location": "Mehalla_Tanta_Highway",
        "latitude": 30.7931,
        "longitude": 31.0019,
        "speed_limit_kmh": 80,
    },

    {
        "id": "CAM_04",
        "location": "October_Bridge_Exit_3",
        "latitude": 30.0444,
        "longitude": 31.2357,
        "speed_limit_kmh": 80,
    },
]


VEHICLE_TYPES = [

    "Sedan",
    "SUV",
    "Truck",
    "Bus",
    "Motorcycle",

]


def generate_telemetry():

    camera = random.choice(CAMERAS)

    speed_limit = camera["speed_limit_kmh"]

    speed = round(
        random.uniform(30.0, 140.0),
        2
    )

    event = {

        "event_id":
            f"EVT_{uuid.uuid4().hex}",

        "timestamp":
            datetime.now(timezone.utc).isoformat(),

        "camera_id":
            camera["id"],

        "location":
            camera["location"],

        "latitude":
            camera["latitude"],

        "longitude":
            camera["longitude"],

        "vehicle_type":
            random.choice(VEHICLE_TYPES),

        "speed_kmh":
            speed,

        "speed_limit_kmh":
            speed_limit,

        "is_speeding":
            speed > speed_limit,
    }

    return event


if __name__ == "__main__":

    print(
        f"🚦 Starting Traffic Simulator "
        f"-> Kafka topic '{TOPIC}'"
    )

    print(
        f"Kafka broker: {BROKER}"
    )

    try:

        while True:

            event = generate_telemetry()

            future = producer.send(
                TOPIC,
                value=event
            )

            metadata = future.get(
                timeout=10
            )

            print(

                f"Sent {event['event_id']} | "
                f"{event['camera_id']} | "
                f"{event['speed_kmh']} km/h | "
                f"partition={metadata.partition} "
                f"offset={metadata.offset}"

            )

            time.sleep(
                random.uniform(
                    0.5,
                    1.5
                )
            )

    except KeyboardInterrupt:

        print(
            "\nStopping simulator..."
        )

    finally:

        producer.flush()

        producer.close()