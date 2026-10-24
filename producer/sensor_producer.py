"""Sensor-Producer (Aufgabe 2.1).

Ablauf je Messung: Wert simulieren -> Nachricht laut Vertrag bauen und prüfen ->
persistent mit Publisher Confirm an x.sensor senden (Routing Key sensor.<type>.<location>).
Verbindungsfehler: Exponential Backoff in common/rmq.py.
"""
import argparse
import logging
import random
import sys
import time

import pika

from common import messages, topology
from common.rmq import PermanentError, ReliablePublisher, RetriesExhausted
from producer.sensors import SENSOR_TYPES, SENSORS, Sensor

log = logging.getLogger("producer")

STATS_INTERVAL_SECONDS = 10


def build_reading(sensor: Sensor, value: float) -> dict:
    """Nachricht laut contracts/schemas/sensor-reading.v1.json, vor dem Senden geprüft."""
    reading = {
        "message_id": messages.new_message_id(),
        "sensor_id": sensor.sensor_id,
        "timestamp": messages.now_iso(),
        "type": sensor.type,
        "value": value,
        "unit": sensor.unit,
        "location": sensor.location,
    }
    return messages.validate(messages.MT_SENSOR_READING, reading)   # nie etwas Vertragswidriges senden


def reading_properties(reading: dict) -> pika.BasicProperties:
    return pika.BasicProperties(
        content_type="application/json",
        delivery_mode=pika.DeliveryMode.Persistent,   # = 2, übersteht einen Broker-Neustart (persistente Nachrichten)
        message_id=reading["message_id"],
        type=messages.MT_SENSOR_READING,              # Version laut Vertrag
        timestamp=int(time.time()),
        app_id="sensor-producer",
    )


def stats_event(published: dict, connection_retries: int) -> tuple[bytes, pika.BasicProperties]:
    """Auxiliary producer statistics event for dashboarding."""
    event = {
        "event": "producer_stats", "source": "producer", "ts": messages.now_iso(),
        "published": dict(published), "connection_retries": connection_retries,
    }
    props = pika.BasicProperties(content_type="application/json", delivery_mode=pika.DeliveryMode.Transient,
                                 type="producer.stats.v1")
    return messages.encode(event), props


def parse_args():
    parser = argparse.ArgumentParser(description="IoT-Sensor-Producer")
    parser.add_argument("--interval", type=float, default=1.0, help="Sekunden zwischen zwei Messrunden")
    parser.add_argument("--anomaly-rate", type=float, default=0.05, help="Anteil Ausreißer, 0..1")
    parser.add_argument("--only", choices=sorted(SENSOR_TYPES), help="nur diesen Sensortyp senden")
    parser.add_argument("--count", type=int, default=0, help="Anzahl Messrunden (0 = endlos)")
    parser.add_argument("--seed", type=int, help="Zufallsseed für reproduzierbare Werte")
    parser.add_argument("--quiet", action="store_true", help="nicht jede Messung ausgeben")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s [producer] %(message)s",
                        datefmt="%H:%M:%S", stream=sys.stdout)
    logging.getLogger("pika").setLevel(logging.CRITICAL)

    rng = random.Random(args.seed)
    sensors = [s for s in SENSORS if not args.only or s.type == args.only]
    publisher = ReliablePublisher("sensor-producer")
    published = {t: 0 for t in SENSOR_TYPES}
    last_stats = time.monotonic()

    log.info("Starte %d Sensoren: %s", len(sensors), ", ".join(f"{s.sensor_id} ({s.type})" for s in sensors))
    try:
        publisher.connect()
        rounds = 0
        while not args.count or rounds < args.count:
            rounds += 1
            for sensor in sensors:
                reading = build_reading(sensor, sensor.next_value(rng, args.anomaly_rate))
                key = topology.sensor_routing_key(sensor.type, sensor.location)
                publisher.publish(topology.EX_SENSOR, key, messages.encode(reading), reading_properties(reading))
                published[sensor.type] += 1
                if not args.quiet:
                    log.info("-> %-27s %-10s %9.2f %s", key, sensor.sensor_id, reading["value"], reading["unit"])

            if time.monotonic() - last_stats >= STATS_INTERVAL_SECONDS:
                publisher.publish_monitoring(*stats_event(published, publisher.connection_retries))
                log.info("Statistik: gesendet %s, Verbindungs-Retries %d", published, publisher.connection_retries)
                last_stats = time.monotonic()
            publisher.sleep(args.interval)
        publisher.publish_monitoring(*stats_event(published, publisher.connection_retries))

    except RetriesExhausted as exc:
        log.critical("%s. Producer beendet sich.", exc)
        sys.exit(1)
    except PermanentError as exc:
        log.critical("Permanenter Fehler, keine Wiederholung: %s", exc)
        sys.exit(2)
    except KeyboardInterrupt:
        log.info("Beendet durch Benutzer.")
    finally:
        log.info("Gesendet und vom Broker bestätigt: %s", published)
        publisher.close()


if __name__ == "__main__":
    main()