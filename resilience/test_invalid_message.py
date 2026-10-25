import argparse
import json
import time

import pika

from common import config, messages, topology
from common.rmq import ReliablePublisher
from producer.sensor_producer import build_reading, reading_properties
from producer.sensors import SENSORS


def send_invalid_message():
    # reuse an existing temperature sensor
    sensor = next(
        sensor for sensor in SENSORS
        if sensor.sensor_id == "TEMP-H1-01"
    )

    # create a real sensor message
    reading = build_reading(sensor, sensor.baseline)

    # introduce one intentional schema error
    reading["value"] = "INVALID"

    publisher = ReliablePublisher("resilience-invalid-test")

    try:
        publisher.connect()

        publisher.publish(
            topology.EX_SENSOR,
            topology.sensor_routing_key(
                sensor.type,
                sensor.location,
            ),
            messages.encode(reading),
            reading_properties(reading),
        )

        print("Invalid sensor message sent")
        print("Message ID:", reading["message_id"])
        print("Sensor:", reading["sensor_id"])
        print("Invalid value:", reading["value"])

    finally:
        publisher.close()




def check_dlq(expected_message_id):
    connection = pika.BlockingConnection(
        pika.URLParameters(config.RABBITMQ_URL)
    )
    channel = connection.channel()
    deliveries = []

    try:
        queue_info = channel.queue_declare(
            queue=config.DLQ_QUEUE,
            passive=True,
        )

        message_count = queue_info.method.message_count

        for _ in range(message_count):
            method, properties, body = channel.basic_get(
                queue=config.DLQ_QUEUE,
                auto_ack=False,
            )

            if method is None:
                break

            deliveries.append(method.delivery_tag)

            reading = json.loads(body.decode("utf-8"))

            if reading.get("message_id") != expected_message_id:
                continue

            reasons = [
                entry.get("reason")
                for entry in (properties.headers or {}).get(
                    "x-death", []
                )
            ]

            print("Message ID:", reading["message_id"])
            print("Sensor:", reading.get("sensor_id"))
            print("Value:", reading.get("value"))
            print("Dead-letter reasons:", reasons)

            assert reading.get("value") == "INVALID", (
                "Unexpected sensor value"
            )

            assert "rejected" in reasons, (
                "Expected dead-letter reason: rejected"
            )

            print("Test passed: Correct message found in DLQ")
            return

        raise AssertionError(
            f"Message {expected_message_id} not found in DLQ"
        )

    finally:
        for delivery_tag in deliveries:
            channel.basic_nack(
                delivery_tag=delivery_tag,
                requeue=True,
            )

        connection.close()




def send_valid_message():
    # use the same original producer function again
    sensor = next(
        sensor for sensor in SENSORS
        if sensor.sensor_id == "TEMP-H1-01"
    )

    reading = build_reading(sensor, sensor.baseline)

    publisher = ReliablePublisher("resilience-recovery-test")

    try:
        publisher.connect()

        publisher.publish(
            topology.EX_SENSOR,
            topology.sensor_routing_key(
                sensor.type,
                sensor.location,
            ),
            messages.encode(reading),
            reading_properties(reading),
        )

        print("Valid sensor message sent")
        print("Sensor:", reading["sensor_id"])
        print("Value:", reading["value"])
        print("Verify successful processing in consumer logs")

    finally:
        publisher.close()



if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "mode",
        choices=["send", "check", "recover"],
    )

    parser.add_argument(
        "--message-id",
        help="Message ID to verify in the Dead Letter Queue",
    )

    args = parser.parse_args()

    if args.mode == "send":
        send_invalid_message()

    elif args.mode == "check":
        if not args.message_id:
            parser.error("--message-id is required for check mode")

        check_dlq(args.message_id)

    elif args.mode == "recover":
        send_valid_message()
