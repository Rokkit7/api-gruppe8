"""Gemeinsame Konfiguration aller Komponenten.

Verbindungsdaten kommen aus Umgebungsvariablen (Standard passt zu docker-compose.yml).
Die übrigen Werte sind gemeinsame Vereinbarungen aus contracts/asyncapi.yaml
und werden nur per Pull Request nach main geändert.
"""
import os

# --- Verbindung zu RabbitMQ -------------------------------------------------
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "iot")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "iot")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
HEARTBEAT_SECONDS = 30

# --- Retry bei Verbindungsfehlern (Aufgabe 2.1) ------------------------------
MAX_RETRIES = 5             # maximal 5 Wiederholungsversuche
BACKOFF_BASE_SECONDS = 1.0  # ≈ 1, 2, 4, 8, 10 s
BACKOFF_CAP_SECONDS = 10.0  # nie mehr als 10 s zwischen zwei Versuchen