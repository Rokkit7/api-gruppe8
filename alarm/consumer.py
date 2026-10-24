from __future__ import annotations

import json
import time

import pika

from common import config, messages, topology

alarm_counter = {"CRITICAL": 0, "WARNING": 0}


def handle_alarm(alarm: dict, priority: str):
    print(f"Alarm der Prioritaet {priority} wird verarbeitet: {alarm}")
    messages.validate(messages.MT_ALARM, alarm)

    alarm_id = alarm["alarm_id"]
    timestamp = alarm["timestamp"]
    sensor_id = alarm["sensor_id"]
    sensor_type = alarm["sensor_type"]
    value = alarm["value"]
    severity = alarm["severity"]
    source_timestamp = alarm["source_timestamp"]
    message = alarm["message"]

    print(
        f"Der {severity}-Alarm {alarm_id} wurde um {source_timestamp} fuer Sensor-Typ {sensor_type} ausgeloest."
    )
    print(
        f"Alarmzeit {timestamp}: Sensor {sensor_id} meldete {value}. {message}"
    )

    alarm_counter[priority] += 1
    print(f"Anzahl erfolgreicher {priority} verarbeiteter Alarme: {alarm_counter[priority]}")


def init_connection():
    connection = pika.BlockingConnection(pika.URLParameters(config.RABBITMQ_URL))
    channel = connection.channel()
    topology.declare_topology(channel)
    channel.basic_qos(prefetch_count=1)

    print("Alarm-Consumer gestartet")

    try:
        while True:
            method, properties, body = channel.basic_get(
                queue=config.ALARM_CRITICAL_QUEUE,
                auto_ack=False
            )

            priority = "CRITICAL"

            if method is None:
                method, properties, body = channel.basic_get(
                    queue=config.ALARM_WARNING_QUEUE,
                    auto_ack=False
                )
                priority = "WARNING"

            if method is None:
                time.sleep(0.2)
                continue

            try:
                alarm = json.loads(body)
                handle_alarm(alarm, priority)

            except json.JSONDecodeError as exc:
                print(f"Alarmnachricht besitzt kein gueltiges JSON-Format: {exc}")
                channel.basic_nack(
                    delivery_tag=method.delivery_tag,
                    requeue=False
                )
            except Exception as exc:
                print(f"Fehler bei der Alarmverarbeitung: {exc}")
                channel.basic_nack(
                    delivery_tag=method.delivery_tag,
                    requeue=False
                )
            else:
                channel.basic_ack(
                    delivery_tag=method.delivery_tag
                )
    except KeyboardInterrupt:
        print("Alarm-Consumer wird beendet.")
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    init_connection()
