"""
This module creates the exchanges, queues and bindings that the analysis,
alerting and monitoring components expect to exist before they start consuming
"""

from __future__ import annotations
import os
import pika
import common.config as conf

# Backward-compatible exports used by other modules/tests
EX_SENSOR = conf.SENSOR_EXCHANGE
EX_MONITORING = conf.MONITORING_EXCHANGE
DLQ_QUEUE = conf.DLQ_QUEUE
TEMPERATURE_QUEUE = conf.TEMPERATURE_QUEUE


def sensor_routing_key(sensor_type: str, location: str) -> str:
    """Build the routing key used for sensor readings."""
    return f"sensor.{sensor_type}.{location}"



def queue_arguments(queue_name: str) -> dict | None:
    """Return queue arguments for the main queues.

    The primary queues temporarily keep messages for up to 60 seconds and then
    route expired messages to the dead-letter exchange
    """
    if queue_name in {conf.TEMPERATURE_QUEUE, conf.ENVIRONMENT_QUEUE}:
        return {
            "x-message-ttl": 60_000,
            "x-dead-letter-exchange": conf.DLX_EXCHANGE,
            "x-dead-letter-routing-key": conf.DLQ_QUEUE,
        }
    return None


def declare_topology(channel) -> None:
    """Create the shared RabbitMQ topology"""

    # Top-level exchanges for sensor data, alarms and monitoring events
    channel.exchange_declare(exchange=conf.SENSOR_EXCHANGE, exchange_type="topic", durable=True)
    channel.exchange_declare(exchange=conf.ALARM_EXCHANGE, exchange_type="direct", durable=True)
    channel.exchange_declare(exchange=conf.MONITORING_EXCHANGE, exchange_type="topic", durable=True)
    channel.exchange_declare(exchange=conf.DLX_EXCHANGE, exchange_type="direct", durable=True)

    # DLX/DLQ setup used when messages fail or expire
    channel.queue_declare(queue=conf.DLQ_QUEUE, durable=True)
    channel.queue_bind(exchange=conf.DLX_EXCHANGE, queue=conf.DLQ_QUEUE, routing_key=conf.DLQ_QUEUE)

    # Declare all main application queues with TTL/DLX arguments for the main data paths
    for queue_name in (
            conf.TEMPERATURE_QUEUE, conf.ENVIRONMENT_QUEUE, conf.ALARM_WARNING_QUEUE,
            conf.ALARM_CRITICAL_QUEUE, conf.MONITORING_QUEUE):

        channel.queue_declare(queue=queue_name, durable=True, arguments=queue_arguments(queue_name))

    # Sensor data is distributed by routing key and separated into analysis queues
    channel.queue_bind(exchange=conf.SENSOR_EXCHANGE, queue=conf.TEMPERATURE_QUEUE, routing_key="sensor.temperature.*")
    channel.queue_bind(exchange=conf.SENSOR_EXCHANGE, queue=conf.ENVIRONMENT_QUEUE, routing_key="sensor.humidity.*")
    channel.queue_bind(exchange=conf.SENSOR_EXCHANGE, queue=conf.ENVIRONMENT_QUEUE, routing_key="sensor.brightness.*")

    # Alarm messages are grouped by severity
    channel.queue_bind(exchange=conf.ALARM_EXCHANGE, queue=conf.ALARM_WARNING_QUEUE, routing_key=conf.ALARM_WARNING_QUEUE)
    channel.queue_bind(exchange=conf.ALARM_EXCHANGE, queue=conf.ALARM_CRITICAL_QUEUE, routing_key=conf.ALARM_CRITICAL_QUEUE)

    # Monitoring data is sent via a dedicated topic exchange
    channel.queue_bind(exchange=conf.MONITORING_EXCHANGE, queue=conf.MONITORING_QUEUE, routing_key=conf.MONITORING_ROUTING_KEY)


def setup_rabbitmq(url: str | None = None) -> None:
    """Create the project topology against the configured RabbitMQ broker"""
    connection = pika.BlockingConnection(pika.URLParameters(url or conf.RABBITMQ_URL))
    channel = connection.channel()
    declare_topology(channel)
    connection.close()
    print("RabbitMQ topology is ready.")


def declare_all(channel) -> None:
    """Backward-compatible alias used by existing publisher code."""
    declare_topology(channel)


if __name__ == "__main__":
    setup_rabbitmq()
