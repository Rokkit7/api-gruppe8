from __future__ import annotations

import json
import logging
import os
import statistics
import time
from datetime import datetime, timezone
from typing import Any

import pika
from common import config as conf, messages

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
LOG = logging.getLogger("analysis")

def validate_reading(message: Any) -> dict:
    """Validate a sensor payload before processing it.
    Invalid or malformed messages should be rejected early instead of being processed with wrong assumptions
    """
    if not isinstance(message, dict):
        raise ValueError("Message must be a JSON object")
    return messages.validate(messages.MT_SENSOR_READING, message)


def alarm_level(sensor_type: str, value: float) -> str | None:
    """Return the highest alarm severity triggered by a sensor value"""
    matched = [
        severity for threshold, severity in conf.ANOMALY_THRESHOLDS[sensor_type]
        if value > threshold
    ]

    if not matched:
        return None

    return "critical" if "critical" in matched else "warning"


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
    messages.validate(messages.MT_ALARM, alarm)

    routing_key = f"alarm.{severity}"
    channel.basic_publish(
        exchange=conf.ALARM_EXCHANGE,
        routing_key=routing_key,
        body=json.dumps(alarm, separators=(",", ":")),
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,
            message_id=alarm["alarm_id"],
            priority=9 if severity == "critical" else 5,
        ),
        mandatory=True,
    )

    LOG.warning("Published %s alarm for %s=%s", severity, reading["type"], reading["value"])


def publish_monitoring_event(channel, *, source: str, event_type: str, payload: dict) -> None:
    """Emit a monitoring event to the dedicated monitoring exchange"""
    sensor_type = payload.get("sensor_type") if isinstance(payload, dict) else None
    processing_time = payload.get("processing_time", 0.0) if isinstance(payload, dict) else 0.0
    if sensor_type not in {"temperature", "humidity", "brightness"}:
        sensor_type = "temperature"

    event = {
        "event_type": event_type,
        "sensor_type": sensor_type,
        "processing_time": float(processing_time),
        "source": source,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }
    messages.validate(messages.MT_MONITORING_EVENT, event)

    event_id = f"{source}-{event_type}-{time.time_ns()}"

    channel.basic_publish(
        exchange=conf.MONITORING_EXCHANGE,
        routing_key=conf.MONITORING_ROUTING_KEY,
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
        event_type="processed",
        payload={"sensor_type": sensor_type, "processing_time": 0.0, "result": result},
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

    params = pika.URLParameters(conf.RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.confirm_delivery()

    queue = conf.TEMPERATURE_QUEUE if mode == "temperature" else conf.ENVIRONMENT_QUEUE
    aggregator = TemperatureAggregator(channel) if mode == "temperature" else None

    channel.basic_qos(prefetch_count=1)

    def on_message(ch, method, properties, body):
        """Handle a single incoming message from the queue"""
        try:
            reading = validate_reading(json.loads(body.decode("utf-8")))

            if mode == "temperature" and reading["type"] != "temperature":
                raise ValueError("Temperature queue received a non-temperature reading")
            if mode == "environment" and reading["type"] not in {"humidity", "brightness"}:
                raise ValueError("Environment queue received an unexpected sensor type")

            started = time.perf_counter()
            severity = alarm_level(reading["type"], float(reading["value"]))
            if severity:
                # Publish first. If publish fails, do not ACK the source message
                publish_alarm(ch, reading, severity)
                publish_monitoring_event(
                    ch,
                    source="analysis.consumer",
                    event_type="anomaly",
                    payload={
                        "sensor_id": reading["sensor_id"],
                        "sensor_type": reading["type"],
                        "value": reading["value"],
                        "severity": severity,
                        "processing_time": time.perf_counter() - started,
                    },
                )
            publish_monitoring_event(
                ch,
                source="analysis.consumer",
                event_type="processed",
                payload={
                    "sensor_id": reading["sensor_id"],
                    "sensor_type": reading["type"],
                    "value": reading["value"],
                    "severity": severity,
                    "processing_time": time.perf_counter() - started,
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
            publish_monitoring_event(
                ch,
                source="analysis.consumer",
                event_type="failure",
                payload={"sensor_type": "temperature", "processing_time": 0.0, "error": str(exc)},
            )
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception:
            LOG.exception("Processing/publishing failed; sending to DLQ")
            publish_monitoring_event(
                ch,
                source="analysis.consumer",
                event_type="failure",
                payload={"sensor_type": "temperature", "processing_time": 0.0, "error": "processing_failure"},
            )
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