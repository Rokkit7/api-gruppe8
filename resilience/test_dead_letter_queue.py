from __future__ import annotations

import json
import time
from multiprocessing import Process
import logging
from typing import Tuple

import pika
import pytest
from pika import BlockingConnection
from pika.adapters.blocking_connection import BlockingChannel

from analysis.consumer import RABBITMQ_URL, run_consumer
from common.topology import DLQ_QUEUE, TEMPERATURE_QUEUE, setup_rabbitmq
from resilience.test_analysis_consumer import VALID_READING

LOG = logging.getLogger("DLQ Test Logger")

# A faulty sensor reading that is missing the required "sensor_id" field
READING_MISSING_FIELD = {
    "timestamp": "2026-10-17T18:00:00Z",
    "type": "temperature",
    "value": 91.5,
}

# A faulty sensor reading that has an invalid "timestamp" field
READING_INVALID_TIMESTAMP = {
    "sensor_id": "s-01",
    "timestamp": "not-a-timestamp",
    "type": "temperature",
    "value": 91.5,
}

# A faulty sensor reading that has an invalid "type" field
READING_INVALID_SENSOR_TYPE = {
    "sensor_id": "s-01",
    "timestamp": "2026-10-17T18:00:00Z",
    "type": "invalid",
    "value": 91.5,
}

# Malformed JSON payload that misses a closing quotation mark and a closing brace
READING_MALFORMED_JSON = """{
    sensor_id": "s-01",
    "timestamp": "not-a-timestamp",
    "type": "temperature",
    "value": 91.5,
"""

def _wait_for_dead_letter(channel, timeout_seconds: float = 8.0):
    """Poll the DLQ until a message appears or timeout is reached."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        method, properties, body = channel.basic_get(queue=DLQ_QUEUE, auto_ack=True)
        if method is not None:
            return method, properties, body
        time.sleep(0.2)
    return None, None, None

def setup_rabbitmq_test() -> tuple[BlockingConnection, BlockingChannel, Process]:
    """Set up the RabbitMQ topology and start the consumer process for testing."""
    try:
        setup_rabbitmq()
    except pika.exceptions.AMQPConnectionError:
        pytest.skip("RabbitMQ is not reachable on the configured RABBITMQ_URL")

    connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    channel = connection.channel()

    channel.queue_purge(queue=TEMPERATURE_QUEUE)
    channel.queue_purge(queue=DLQ_QUEUE)

    consumer = Process(target=run_consumer, args=("temperature",), daemon=True)
    consumer.start()

    return connection, channel, consumer

def send_sensor_reading(channel, reading: dict) -> None:
    """Send a sensor reading to the RabbitMQ exchange for testing."""
    channel.basic_publish(
        exchange="sensor-exchange",
        routing_key="sensor.temperature",
        body=json.dumps(reading),
        properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
    )

def test_missing_field_message() -> None:
    """
    Test case sending a sensor reading with a missing required field to the RabbitMQ exchange.
    For Task 2.6:
    1. The missing sensor id field in the payload will cause the consumer to reject the message.
    2. The consumer will reject (NACK) the message, and RabbitMQ will route it to the dead-letter queue (DLQ).
    3. A NACK will be sent.
    4. The message will not be sent again, after it has been rejected by the consumer and moved into the DLQ.
    5. The consumers validation method will raise a ValueError because the expected field is missing.
    6. As long as the payload does not contain the required fields, the message will be rejected and routed to the DLQ.
    7. This does not have any effect on the other messages in the queue or on the entire system,
        as the consumer will continue to process other incoming messages.
    """

    connection, channel , consumer = setup_rabbitmq_test()

    try:
        send_sensor_reading(channel, READING_MISSING_FIELD)
        _, properties, body = _wait_for_dead_letter(channel)

        assert body is not None, "Expected message in dead-letter queue"
        dlq_body = json.loads(body.decode("utf-8"))
        assert dlq_body == READING_MISSING_FIELD

        LOG.debug("Message body from DLQ: %s", dlq_body)

        assert properties is not None
        assert properties.headers is not None
        assert "x-death" in properties.headers

    finally:
        if consumer.is_alive():
            consumer.terminate()
            consumer.join(timeout=3)

        if connection.is_open:
            connection.close()


def test_invalid_timestamp() -> None:
    """
    A reading containing an invalid timestamp should be routed to the shared DLQ.
    For Task 2.6:
    1. The invalid timestamp field in the payload will cause the consumer to reject the message.
    2. The consumer will reject (NACK) the message, and RabbitMQ will route it to the dead-letter queue (DLQ).
    3. A NACK will be sent.
    4. The message will not be sent again, after it has been rejected by the consumer and moved into the DLQ.
    5. The consumers validation method will raise a ValueError because parsing the ISO string into a valid datetime object
        was not possible.
    6. As long as the timestamp is not in a valid ISO format, the payload will land in the DLQ.
    7. This does not have any effect on the other messages in the queue or on the entire system,
        as the consumer will continue to process other incoming messages.
    """

    connection, channel, consumer = setup_rabbitmq_test()

    try:
        send_sensor_reading(channel, READING_INVALID_TIMESTAMP)
        _, properties, body = _wait_for_dead_letter(channel)

        assert body is not None, "Expected message in dead-letter queue"
        dlq_body = json.loads(body.decode("utf-8"))
        assert dlq_body == READING_INVALID_TIMESTAMP

        LOG.debug("Message body from DLQ: %s", dlq_body)

        assert properties is not None
        assert properties.headers is not None
        assert "x-death" in properties.headers

    finally:
        if consumer.is_alive():
            consumer.terminate()
            consumer.join(timeout=3)

        if connection.is_open:
            connection.close()

def test_invalid_sensor_type() -> None:
    """
    A reading containing an unknown sensor type should be routed to the shared DLQ.
    For Task 2.6:
    1. The unknown sensor type in the payload will cause the consumer to reject the message.
    2. The consumer will reject (NACK) the message, and RabbitMQ will route it to the dead-letter queue (DLQ).
    3. A NACK will be sent.
    4. The message will not be sent again, after it has been rejected by the consumer and moved into the DLQ.
    5. The consumers validation method will raise a ValueError because the given sensor type could not be found.
    6. As long as the payload does not contain a valid, expected sensor type,
        the message will be rejected and routed to the DLQ.
    7. This does not have any effect on the other messages in the queue or on the entire system,
    as the consumer will continue to process other incoming messages.

    """

    connection, channel, consumer = setup_rabbitmq_test()

    try:
        send_sensor_reading(channel, READING_INVALID_SENSOR_TYPE)
        _, properties, body = _wait_for_dead_letter(channel)

        assert body is not None, "Expected message in dead-letter queue"
        dlq_body = json.loads(body.decode("utf-8"))
        assert dlq_body == READING_INVALID_SENSOR_TYPE

        LOG.debug("Message body from DLQ: %s", dlq_body)

        assert properties is not None
        assert properties.headers is not None
        assert "x-death" in properties.headers

    finally:
        if consumer.is_alive():
            consumer.terminate()
            consumer.join(timeout=3)

        if connection.is_open:
            connection.close()

def test_malformed_json() -> None:
    """
    A reading containing an unknown sensor type should be routed to the shared DLQ.
    For Task 2.6:
    1. The malformed JSON string will make the consumer fail to parse the payload, causing it to reject the message.
    2. The consumer will reject (NACK) the message, and RabbitMQ will route it to the dead-letter queue (DLQ).
    3. A NACK will be sent.
    4. The message will not be sent again, after it has been rejected by the consumer and moved into the DLQ.
    5. The consumers on message method will fail to parse the JSON string into a dictionary, raising a JSONDecodeError and
        causing the message to be rejected.
    6. As long as the payload is malformed and cannot be parsed into a valid JSON object,
        the message will be rejected and routed to the DLQ.
    7. This does not have any effect on the other messages in the queue or on the entire system,
    as the consumer will continue to process other incoming messages.
    """

    connection, channel, consumer = setup_rabbitmq_test()

    try:

        channel.basic_publish(
            exchange="sensor-exchange",
            routing_key="sensor.temperature",
            body=READING_MALFORMED_JSON,
            properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
        )

        _, properties, body = _wait_for_dead_letter(channel)

        assert body is not None, "Expected message in dead-letter queue"
        dlq_body = body.decode("utf-8")
        assert dlq_body == READING_MALFORMED_JSON

        LOG.debug("Message body from DLQ: %s", dlq_body)

        assert properties is not None
        assert properties.headers is not None
        assert "x-death" in properties.headers

    finally:
        if consumer.is_alive():
            consumer.terminate()
            consumer.join(timeout=3)

        if connection.is_open:
            connection.close()

def test_message_ttl() -> None:
    """
    A valid reading that is not consumed within the TTL should be routed to the shared DLQ.
    For Task 2.6:
    1. Stopping the consumer and leaving the message in the queue for longer than 10 seconds will cause the message to
        be marked as expired and thus moved into the DLQ.
    2. RabbitMQ will route the expired message to the dead-letter queue (DLQ).
    3. Neither, as the consumer is not running, so no NACK will be sent.
    4. The message will not be sent again, as the sensor data is already stale and has been moved into the DLQ.
    5. The configured TTL interval of 60 seconds will cause the message to expire and be moved into the configured DLQ.
        (see common/topology.py)
    6. Since the message was left for longer than 60 seconds in the queue, it will be expired.
    7. The message expiring has no effect on other messages, but the consumer shutting down has, since it is an integral
        part of the system. However, this test is only meant to verify that the TTL mechanism works as expected.
    """

    connection, channel, consumer = setup_rabbitmq_test()

    try:
        # Stop the consumer so the message remains in the queue long enough to expire.
        if consumer.is_alive():
            consumer.terminate()
            consumer.join(timeout=3)

        send_sensor_reading(channel, VALID_READING)

        time.sleep(11)  # TTL is currently 10 seconds, so wait slightly longer.

        _, properties, body = _wait_for_dead_letter(channel)

        assert body is not None, "Expected message in dead-letter queue"
        dlq_body = json.loads(body.decode("utf-8"))
        assert dlq_body == VALID_READING

        LOG.debug("Message body from DLQ: %s", dlq_body)

        assert properties is not None
        assert properties.headers is not None
        assert "x-death" in properties.headers
        x_death_entries = properties.headers["x-death"]
        assert x_death_entries
        assert any(
            entry.get("reason") in {"expired", b"expired"}
            for entry in x_death_entries
        ), "Expected x-death reason to include 'expired'"

    finally:
        if consumer.is_alive():
            consumer.terminate()
            consumer.join(timeout=3)

        if connection.is_open:
            connection.close()
