"""
This module creates the exchanges, queues and bindings that the analysis,
alerting and monitoring components expect to exist before they start consuming
"""

from __future__ import annotations
import os
import pika

# Default broker URL
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")

# Exchange names
SENSOR_EXCHANGE = "sensor-exchange"
ALARM_EXCHANGE = "alarm-exchange"
MONITORING_EXCHANGE = "monitoring-exchange"
DLX_EXCHANGE = "dlx"
DLQ_QUEUE = "analysis.dlq"

# Queue names
TEMPERATURE_QUEUE = "analysis.temperature"
ENVIRONMENT_QUEUE = "analysis.environment"
ALARM_WARNING_QUEUE = "alarm.warning"
ALARM_CRITICAL_QUEUE = "alarm.critical"
MONITORING_QUEUE = "monitoring.analysis"


def queue_arguments(queue_name: str) -> dict | None:
    """Return queue arguments for the main queues.

    The primary queues temporarily keep messages for up to 60 seconds and then
    route expired messages to the dead-letter exchange
    """
    if queue_name in {TEMPERATURE_QUEUE, ENVIRONMENT_QUEUE}:
        return {
            "x-message-ttl": 10_000,
            "x-dead-letter-exchange": DLX_EXCHANGE,
            "x-dead-letter-routing-key": DLQ_QUEUE,
        }
    return None


def declare_topology(channel) -> None:
    """Create the shared RabbitMQ topology"""

    # Top-level exchanges for sensor data, alarms and monitoring events
    channel.exchange_declare(exchange=SENSOR_EXCHANGE, exchange_type="topic", durable=True)
    channel.exchange_declare(exchange=ALARM_EXCHANGE, exchange_type="direct", durable=True)
    channel.exchange_declare(exchange=MONITORING_EXCHANGE, exchange_type="topic", durable=True)
    channel.exchange_declare(exchange=DLX_EXCHANGE, exchange_type="direct", durable=True)

    # DLX/DLQ setup used when messages fail or expire
    channel.queue_declare(queue=DLQ_QUEUE, durable=True)
    channel.queue_bind(exchange=DLX_EXCHANGE, queue=DLQ_QUEUE, routing_key=DLQ_QUEUE)

    # Declare all main application queues with TTL/DLX arguments for the main data paths
    for queue_name in (TEMPERATURE_QUEUE, ENVIRONMENT_QUEUE, ALARM_WARNING_QUEUE, ALARM_CRITICAL_QUEUE, MONITORING_QUEUE):
        channel.queue_declare(queue=queue_name, durable=True, arguments=queue_arguments(queue_name))

    # Sensor data is distributed by routing key and separated into analysis queues
    channel.queue_bind(exchange=SENSOR_EXCHANGE, queue=TEMPERATURE_QUEUE, routing_key="sensor.temperature")
    channel.queue_bind(exchange=SENSOR_EXCHANGE, queue=ENVIRONMENT_QUEUE, routing_key="sensor.humidity")
    channel.queue_bind(exchange=SENSOR_EXCHANGE, queue=ENVIRONMENT_QUEUE, routing_key="sensor.pressure")

    # Alarm messages are grouped by severity
    channel.queue_bind(exchange=ALARM_EXCHANGE, queue=ALARM_WARNING_QUEUE, routing_key="alarm.warning")
    channel.queue_bind(exchange=ALARM_EXCHANGE, queue=ALARM_CRITICAL_QUEUE, routing_key="alarm.critical")

    # Monitoring data is sent via a dedicated topic exchange
    channel.queue_bind(exchange=MONITORING_EXCHANGE, queue=MONITORING_QUEUE, routing_key="monitoring.analysis")


def setup_rabbitmq(url: str | None = None) -> None:
    """Create the project topology against the configured RabbitMQ broker"""
    connection = pika.BlockingConnection(pika.URLParameters(url or RABBITMQ_URL))
    channel = connection.channel()
    declare_topology(channel)
    connection.close()
    print("RabbitMQ topology is ready.")


if __name__ == "__main__":
    setup_rabbitmq()
