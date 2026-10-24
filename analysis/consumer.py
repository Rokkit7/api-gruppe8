from __future__ import annotations

import json
import logging
import os
import statistics
import time
from datetime import datetime, timezone
from typing import Any

import pika

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
LOG = logging.getLogger("analysis")

# Broker connection defaults used across the local environment
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
ALARM_EXCHANGE = os.getenv("ALARM_EXCHANGE", "alarm-exchange")
MONITORING_EXCHANGE = os.getenv("MONITORING_EXCHANGE", "monitoring-exchange")
MONITORING_ROUTING_KEY = os.getenv("MONITORING_ROUTING_KEY", "monitoring.analysis")
TEMPERATURE_QUEUE = os.getenv("TEMPERATURE_QUEUE", "analysis.temperature")
ENVIRONMENT_QUEUE = os.getenv("ENVIRONMENT_QUEUE", "analysis.environment")

# Demo thresholds for anomaly detection
THRESHOLDS = {
    "temperature": [(80.0, "WARNING"), (100.0, "CRITICAL")],
    "humidity": [(85.0, "WARNING"), (95.0, "CRITICAL")],
    "pressure": [(1100.0, "WARNING"), (1200.0, "CRITICAL")],
}


def validate_reading(message: Any) -> dict:
    """Validate a sensor payload before processing it.
    Invalid or malformed messages should be rejected early instead of being processed with wrong assumptions
    """
    if not isinstance(message, dict):
        raise ValueError("Message must be a JSON object")

    required = ("sensor_id", "timestamp", "type", "value")
    missing = [key for key in required if key not in message]
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)}")

    if not isinstance(message["sensor_id"], str) or not message["sensor_id"]:
        raise ValueError("sensor_id must be a non-empty string")

    if message["type"] not in THRESHOLDS:
        raise ValueError(f"Unsupported sensor type: {message['type']}")

    if isinstance(message["value"], bool) or not isinstance(message["value"], (int, float)):
        raise ValueError("value must be numeric")

    try:
        datetime.fromisoformat(message["timestamp"])  # Validate ISO format
    except ValueError:
        raise ValueError("timestamp must be a valid ISO string, got: %s", {message['timestamp']})

    return message


def alarm_level(sensor_type: str, value: float) -> str | None:
    """Return the highest alarm severity triggered by a sensor value"""
    matched = [
        severity for threshold, severity in THRESHOLDS[sensor_type]
        if value > threshold
    ]

    if not matched:
        return None

    return "CRITICAL" if "CRITICAL" in matched else "WARNING"


def publish_alarm(channel, reading: dict, severity: str) -> None:
    """Publish a structured alarm message for an anomalous reading

    This method creates a new alarm event and sends it on the direct alarm
    exchange with a routing key based on severity. The alarm is persistent and
    must be confirmed by RabbitMQ before the source message is acknowledged
    """
    alarm = {
        "alarm_id": f'{reading["sensor_id"]}-{time.time_ns()}',
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sensor_id": reading["sensor_id"],
        "sensor_type": reading["type"],
        "value": reading["value"],
        "severity": severity,
        "source_timestamp": reading["timestamp"],
        "message": (
            f'{reading["type"]} value {reading["value"]} '
            f'triggered {severity}'
        ),
    }

    routing_key = f"alarm.{severity.lower()}"
    channel.basic_publish(
        exchange=ALARM_EXCHANGE,
        routing_key=routing_key,
        body=json.dumps(alarm, separators=(",", ":")),
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,
            message_id=alarm["alarm_id"],
            priority=9 if severity == "CRITICAL" else 5,
        ),
        mandatory=True,
    )

    LOG.warning("Published %s alarm for %s=%s", severity, reading["type"], reading["value"])


def publish_monitoring_event(channel, *, source: str, event_type: str, payload: dict) -> None:
    """Emit a monitoring event to the dedicated monitoring exchange"""
    event = {
        "event_id": f"{source}-{event_type}-{time.time_ns()}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "type": event_type,
        "payload": payload,
    }

    event_id = event["event_id"]

    channel.basic_publish(
        exchange=MONITORING_EXCHANGE,
        routing_key=MONITORING_ROUTING_KEY,
        body=json.dumps(event, separators=(",", ":")),
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,
            message_id=event_id,
        ),
        mandatory=True,
    )

    LOG.info("Published monitoring event %s from %s", event_type, source)


def publish_aggregation_monitoring(channel, result: dict, sensor_type: str) -> None:
    """Send aggregation statistics as a structured monitoring event"""
    publish_monitoring_event(
        channel,
        source="analysis.consumer",
        event_type="aggregation.window",
        payload={"sensor_type": sensor_type, "result": result},
    )


class TemperatureAggregator:
    """Aggregate temperature values over a rolling 10-second window.

    All valid temperature readings in a shared 10-second period are combined into one summary
    """

    def __init__(self, channel, window_seconds: float = 10.0):
        self.channel = channel
        self.window_seconds = window_seconds
        self.values: list[float] = []
        self.window_started = time.monotonic()
        self.timer = None

    def add(self, value: float) -> None:
        """Add a new temperature value to the current aggregation window."""
        now = time.monotonic()
        if now - self.window_started >= self.window_seconds:
            self.flush()
        self.values.append(float(value))
        if self.timer is None:
            self.timer = self.channel.connection.call_later(
                self.window_seconds, self._timer_flush
            )

    def _timer_flush(self) -> None:
        """Flush the current aggregation window when the timer expires."""
        self.timer = None
        self.flush()

    def flush(self) -> None:
        """Emit the aggregated statistics for the closing window."""
        if self.values:
            result = {
                "count": len(self.values),
                "average": round(statistics.fmean(self.values), 2),
                "minimum": min(self.values),
                "maximum": max(self.values),
            }
            LOG.info("Temperature aggregation (10s window): %s", result)
            publish_aggregation_monitoring(self.channel, result, "temperature")
            self.values.clear()
        self.window_started = time.monotonic()
        if self.timer is not None:
            try:
                self.channel.connection.remove_timeout(self.timer)
            except Exception:
                pass
            self.timer = None


def run_consumer(mode: str) -> None:
    """Run the analysis consumer in a specific processing mode 'temperature' or 'environment'"""
    if mode not in {"temperature", "environment"}:
        raise ValueError("mode must be temperature or environment")

    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.confirm_delivery()

    queue = TEMPERATURE_QUEUE if mode == "temperature" else ENVIRONMENT_QUEUE
    aggregator = TemperatureAggregator(channel) if mode == "temperature" else None

    channel.basic_qos(prefetch_count=1)

    def on_message(ch, method, properties, body):
        """Handle a single incoming message from the queue"""
        try:
            reading = validate_reading(json.loads(body.decode("utf-8")))

            if mode == "temperature" and reading["type"] != "temperature":
                raise ValueError("Temperature queue received a non-temperature reading")
            if mode == "environment" and reading["type"] not in {"humidity", "pressure"}:
                raise ValueError("Environment queue received an unexpected sensor type")

            severity = alarm_level(reading["type"], float(reading["value"]))
            if severity:
                # Publish first. If publish fails, do not ACK the source message
                publish_alarm(ch, reading, severity)
            publish_monitoring_event(
                ch,
                source="analysis.consumer",
                event_type="reading.processed",
                payload={
                    "sensor_id": reading["sensor_id"],
                    "sensor_type": reading["type"],
                    "value": reading["value"],
                    "severity": severity,
                },
            )

            if aggregator is not None:
                aggregator.add(float(reading["value"]))

            LOG.info(
                "Processed sensor reading sensor_id=%s type=%s value=%s",
                reading["sensor_id"], reading["type"], reading["value"],
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            LOG.exception("Invalid/unprocessable message; sending to DLQ: %s", exc)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception:
            LOG.exception("Processing/publishing failed; sending to DLQ")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_consume(queue=queue, on_message_callback=on_message, auto_ack=False)
    LOG.info("Starting %s analysis consumer on queue %s", mode, queue)
    try:
        channel.start_consuming()
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run an analysis consumer")
    parser.add_argument("mode", choices=("temperature", "environment"))
    args = parser.parse_args()
    run_consumer(args.mode)
