import pika
import json
import os

from common import config, messages, topology

# Globale Variablen
metrics = {
    "total_processed": 0,
    "total_anomalies": 0,
    "total_failures": 0,
    "by_sensor": {
        "temperature": 0,
        "humidity": 0,
        "brightness": 0,
    },
    "sum_processing_time": 0.0,
    "count_events": 0,
}


def print_metrics():
    # AUSGABE

    # Terminal Clear
    os.system("cls" if os.name == "nt" else "clear")

    print("=" * 45)
    print(" LIVE MONITORING DASHBOARD")
    print("=" * 45)

    # Anzahl der verarbeiteten Nachrichten
    print(f"Verarbeitete Nachrichten: {metrics['total_processed']}")

    # Anzahl erkannter Anomalien bzw. erzeugter Alarme
    print(f"Erkannte Anomalien/Alarme: {metrics['total_anomalies']}")

    # Anzahl fehlgeschlagener Verarbeitungen
    print(f"Fehlgeschlagene Verarbeitungen: {metrics['total_failures']}")
    print("-" * 45)

    # Anzahl der Nachrichten je Sensortyp
    print("Nachrichten je Sensortyp (gesamt):")
    for sensor, count in metrics["by_sensor"].items():
        print(f" - {sensor.capitalize():<15}: {count}")
    print("-" * 45)

    # Durchschnittliche Verarbeitungszeit
    if metrics["count_events"] > 0:
        avg_time = metrics["sum_processing_time"] / metrics["count_events"]
        print(f"Durchschnittliche Verarbeitungszeit: {avg_time:.4f}s")
    else:
        print("Durchschnittliche Verarbeitungszeit: -")
    print("=" * 45)


def callback(ch, method, properties, body):
    try:
        data = json.loads(body.decode("utf-8"))

        if "event_type" in data:
            messages.validate(messages.MT_MONITORING_EVENT, data)
            event_type = data.get("event_type")
            sensor_type = data.get("sensor_type")
            processing_time = float(data.get("processing_time", 0.0))
        elif data.get("event") == "producer_stats":
            # Producer stats are informational only and do not affect processing KPIs.
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        else:
            raise KeyError("event_type")

        # Zähler für das jeweilige Event aktualisieren
        if event_type == "processed":
            metrics["total_processed"] += 1
        elif event_type == "anomaly":
            metrics["total_anomalies"] += 1
        elif event_type == "failure":
            metrics["total_failures"] += 1

        # Zähler für den Sensortyp aktualisieren
        if sensor_type in metrics["by_sensor"]:
            metrics["by_sensor"][sensor_type] += 1

        # Verarbeitungszeit für den Durchschnitt aufsummieren
        metrics["sum_processing_time"] += processing_time
        metrics["count_events"] += 1

        # Dashboard in der Konsole aktualisieren
        print_metrics()

        # ACK senden
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except KeyError as e:
        print(f"Fehler: Fehlendes Pflichtfeld im JSON - {e}")
        # NACK senden
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except json.JSONDecodeError:
        print("Fehler: Konnte Nachricht nicht als JSON parsen.")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as exc:
        print(f"Unerwarteter Fehler bei Monitoring-Verarbeitung: {exc}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    connection = pika.BlockingConnection(pika.URLParameters(config.RABBITMQ_URL))
    channel = connection.channel()
    topology.declare_topology(channel)
    channel.basic_qos(prefetch_count=20)

    print("Monitoring-Consumer gestartet. Warte auf Metriken...")
    print_metrics()

    channel.basic_consume(queue=config.MONITORING_QUEUE, on_message_callback=callback, auto_ack=False)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\nMonitoring-Consumer wird beendet...")
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()