import argparse
import os

from analysis import consumer
from common import config, messages, topology
from common.rmq import ReliablePublisher
from producer.sensor_producer import build_reading, reading_properties
from producer.sensors import SENSORS


def send_message():
    sensor = next(
        sensor for sensor in SENSORS
        if sensor.sensor_id == "TEMP-H1-01"
    )

    reading = build_reading(sensor, sensor.baseline)

    publisher = ReliablePublisher("resilience-crash-test")

    try:
        publisher.connect()

        publisher.publish(
            topology.EX_SENSOR,
            topology.sensor_routing_key(
                sensor.type, sensor.location
            ),
            messages.encode(reading),
            reading_properties(reading),
        )

        print("Sensor message sent")
        print("Sensor:", reading["sensor_id"])
        print("Message ID:", reading["message_id"])
        print("Value:", reading["value"])

    finally:
        publisher.close()


def crash_consumer():
    # replace only the monitoring call during this test.
    original_publish = consumer.publish_monitoring_event

    def simulate_crash(channel, *, source, event_type, payload):
        if event_type == "processed":
            print("Message received by analysis consumer", flush=True)
            print("Simulating crash before ACK/NACK", flush=True)
            os._exit(1)

        return original_publish(
            channel,
            source=source,
            event_type=event_type,
            payload=payload,
        )

    consumer.publish_monitoring_event = simulate_crash

    consumer.run_consumer("temperature")


def check_redelivery():
    # Iínspect the original temperature queue.
    import pika

    connection = pika.BlockingConnection(
        pika.URLParameters(config.RABBITMQ_URL)
    )
    channel = connection.channel()

    try:
        method, properties, body = channel.basic_get(
            queue=config.TEMPERATURE_QUEUE,
            auto_ack=False,
        )

        if method is None:
            print("No message available for redelivery check")
            return

        try:
            import json

            reading = json.loads(body.decode("utf-8"))

            print("Sensor:", reading["sensor_id"])
            print("Message ID:", reading["message_id"])
            print("Redelivered:", method.redelivered)

            assert method.redelivered, (
                "Expected redelivery after consumer crash"
            )

            print("Test passed: Message was redelivered")

        finally:
            # preserve the message for the normal consumer.
            channel.basic_nack(
                delivery_tag=method.delivery_tag,
                requeue=True,
            )

    finally:
        connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mode",
        choices=["send", "crash", "check"],
    )
    args = parser.parse_args()

    if args.mode == "send":
        send_message()
    elif args.mode == "crash":
        crash_consumer()
    elif args.mode == "check":
        check_redelivery()
