from __future__ import annotations

import json
import os
import time
from multiprocessing import Process

import pika
import pytest

from common import config
from common.topology import TEMPERATURE_QUEUE, setup_rabbitmq
from resilience.test_analysis_consumer import VALID_READING


def _wait_for_message(channel, queue_name: str, timeout_seconds: float = 5.0):
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        method, properties, body = channel.basic_get(queue=queue_name, auto_ack=False)
        if method is not None:
            return method, properties, body
        time.sleep(0.2)
    return None, None, None


def _crash_consumer_once(queue_name: str) -> None:
    connection = pika.BlockingConnection(pika.URLParameters(config.RABBITMQ_URL))
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)

    def on_message(ch, method, properties, body):
        # Simulate hard crash during processing before ACK/NACK.
        os._exit(3)

    channel.basic_consume(queue=queue_name, on_message_callback=on_message, auto_ack=False)
    channel.start_consuming()


def test_consumer_crash_before_ack_redelivers_message() -> None:
    """A message consumed by a crashing worker must be re-delivered because no ACK was sent"""
    try:
        setup_rabbitmq()
    except pika.exceptions.AMQPConnectionError:
        pytest.skip("RabbitMQ is not reachable on the configured RABBITMQ_URL")

    connection = pika.BlockingConnection(pika.URLParameters(config.RABBITMQ_URL))
    channel = connection.channel()
    channel.queue_purge(queue=TEMPERATURE_QUEUE)

    crash_worker = Process(target=_crash_consumer_once, args=(TEMPERATURE_QUEUE,), daemon=True)
    crash_worker.start()

    try:
        channel.basic_publish(
            exchange=config.SENSOR_EXCHANGE,
            routing_key="sensor.temperature.halle1",
            body=json.dumps(VALID_READING),
            properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
        )

        crash_worker.join(timeout=5)
        assert not crash_worker.is_alive(), "Expected crash worker to exit after receiving a message"

        method, _, body = _wait_for_message(channel, TEMPERATURE_QUEUE)
        assert method is not None, "Expected message redelivery after consumer crash"
        redelivered = json.loads(body.decode("utf-8"))
        assert redelivered == VALID_READING
        channel.basic_ack(delivery_tag=method.delivery_tag)
    finally:
        if crash_worker.is_alive():
            crash_worker.terminate()
            crash_worker.join(timeout=3)
        if connection.is_open:
            connection.close()

