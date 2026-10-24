"""RabbitMQ-Tools: Verbindung, Fehlerklassifikation, Exponential Backoff mit Jitter,
zuverlässiges Senden mit Publisher Confirms.

* Verbindungs-/Netzwerkfehler werden erkannt und als temporär oder permanent eingestuft.
* Temporäre Fehler: max. 5 Wiederholungen, Wartezeit ≈ 1, 2, 4, 8, 10 s (nie > 10 s).
* Permanente Fehler (z. B. falsches Passwort): sofort abbrechen, Wiederholen hilft nicht.
* Eine Nachricht gilt erst als gesendet, wenn der Broker sie bestätigt hat.
"""
import logging
import random
import time

import pika
from pika import exceptions as px

from common import config, topology

log = logging.getLogger("rmq")


class RetriesExhausted(Exception):
    """Nach MAX_RETRIES Wiederholungen weiterhin ein temporärer Fehler."""


class PermanentError(Exception):
    """Fehler, bei dem eine Wiederholung nichts ändert."""


# --- Verbindung ---------------------------------------------------------------
def connection_parameters(name: str) -> pika.ConnectionParameters:
    return pika.ConnectionParameters(
        host=config.RABBITMQ_HOST,
        port=config.RABBITMQ_PORT,
        virtual_host=config.RABBITMQ_VHOST,
        credentials=pika.PlainCredentials(config.RABBITMQ_USER, config.RABBITMQ_PASSWORD),
        heartbeat=config.HEARTBEAT_SECONDS,     # erkennt tote Verbindungen
        blocked_connection_timeout=30,
        connection_attempts=1,                  # Wiederholungen steuern wir selbst (Backoff)
        socket_timeout=5,
        client_properties={"connection_name": name},   # sichtbar in der Management-UI
    )


def connect(name: str) -> pika.BlockingConnection:
    """Einfache Verbindung ohne Retry, z. B. für Consumer und Hilfsskripte."""
    return pika.BlockingConnection(connection_parameters(name))


# --- Fehlerklassifikation -----------------------------------------------------
def classify(exc: BaseException) -> str:
    """'permanent' oder 'transient'.

    Reihenfolge wichtig: ProbableAuthenticationError und ProbableAccessDeniedError erben
    von AMQPConnectionError und müssen deshalb vorher geprüft werden."""
    if isinstance(exc, (px.ProbableAuthenticationError, px.ProbableAccessDeniedError,
                        px.AuthenticationError, px.IncompatibleProtocolError)):
        return "permanent"          # falsche Zugangsdaten, falscher vhost, falsches Protokoll
    if isinstance(exc, px.ConnectionClosedByBroker) and exc.reply_code in (403, 530):
        return "permanent"          # Zugriff verweigert / vhost nicht erlaubt
    if isinstance(exc, (px.UnroutableError, px.ChannelClosedByBroker)):
        return "permanent"          # kein Binding passt / Topologie-Konflikt (z. B. 406)
    if isinstance(exc, (px.AMQPConnectionError, px.ConnectionWrongStateError,
                        px.ChannelWrongStateError, px.NackError, ConnectionError, OSError)):
        return "transient"          # Broker weg, Netz weg, Broker konnte Nachricht nicht annehmen
    return "permanent"              # Unbekanntes nicht blind wiederholen


# --- Exponential Backoff mit Jitter -------------------------------------------
def backoff_delay(attempt: int) -> float:
    """Wartezeit vor Wiederholung Nr. attempt (1..5): min(10, 1 · 2^(n-1)) · Jitter.

    Der Jitter (0,85 … 1,0) verhindert, dass viele Clients nach einem Broker-Neustart
    exakt gleichzeitig reconnecten. Er verkürzt nur, die 10-s-Grenze bleibt eingehalten."""
    delay = min(config.BACKOFF_CAP_SECONDS, config.BACKOFF_BASE_SECONDS * 2 ** (attempt - 1))
    return delay * random.uniform(0.85, 1.0)


# --- Zuverlässiger Publisher --------------------------------------------------
class ReliablePublisher:
    """Senden mit Publisher Confirms und mandatory=True. Bei temporären Fehlern wird die
    Verbindung mit Exponential Backoff neu aufgebaut (1 Versuch + max. 5 Wiederholungen)."""

    def __init__(self, name: str):
        self.name = name
        self.conn = None
        self.channel = None             # Confirm-Channel für Fachnachrichten
        self.monitoring_channel = None  # ohne Confirms, best effort
        self.connection_retries = 0     # für das Monitoring

    def _connect(self) -> None:
        self.conn = connect(self.name)
        self.channel = self.conn.channel()
        topology.declare_all(self.channel)              # idempotent, Topologie aus dem Vertrag
        self.channel.confirm_delivery()                 # Publisher Confirms aktivieren
        self.monitoring_channel = self.conn.channel()
        log.info("Verbunden mit RabbitMQ %s:%s", config.RABBITMQ_HOST, config.RABBITMQ_PORT)

    def _is_connected(self) -> bool:
        return bool(self.conn and self.conn.is_open and self.channel and self.channel.is_open)

    def _drop(self) -> None:
        try:
            if self.conn and self.conn.is_open:
                self.conn.close()
        except Exception:  # noqa: BLE001 – Verbindung ist ohnehin kaputt
            pass
        self.conn = self.channel = self.monitoring_channel = None

    def _with_retry(self, operation, description: str):
        for attempt in range(config.MAX_RETRIES + 1):
            try:
                if not self._is_connected():
                    self._drop()
                    self._connect()
                return operation()
            except Exception as exc:  # noqa: BLE001 – wird klassifiziert
                if classify(exc) == "permanent":
                    raise PermanentError(f"{description}: {type(exc).__name__} {exc}") from exc
                self._drop()
                if attempt == config.MAX_RETRIES:
                    raise RetriesExhausted(f"{description}: nach {config.MAX_RETRIES} Wiederholungen "
                                           f"aufgegeben ({type(exc).__name__})") from exc
                delay = backoff_delay(attempt + 1)
                self.connection_retries += 1
                log.warning("%s fehlgeschlagen (%s) -> Wiederholung %d/%d in %.1f s",
                            description, type(exc).__name__, attempt + 1, config.MAX_RETRIES, delay)
                time.sleep(delay)

    def connect(self) -> None:
        self._with_retry(lambda: None, "Verbindungsaufbau")

    def publish(self, exchange: str, routing_key: str, body: bytes, properties: pika.BasicProperties) -> None:
        """Kehrt erst zurück, wenn der Broker die Nachricht bestätigt hat.

        Bei einer Wiederholung wird dieselbe Nachricht (gleiche message_id) erneut gesendet.
        Kam nur das Confirm nicht mehr an, entsteht ein Duplikat, das der Empfänger über die
        message_id erkennt (at-least-once)."""
        self._with_retry(
            lambda: self.channel.basic_publish(exchange, routing_key, body, properties, mandatory=True),
            f"Senden '{routing_key}'",
        )

    def publish_monitoring(self, body: bytes, properties: pika.BasicProperties) -> None:
        """Best effort: Monitoring darf das eigentliche Senden nie blockieren oder abbrechen."""
        try:
            if self.monitoring_channel and self.monitoring_channel.is_open:
                self.monitoring_channel.basic_publish(topology.EX_MONITORING, "", body, properties)
        except Exception as exc:  # noqa: BLE001
            log.debug("Monitoring-Event verworfen: %s", exc)

    def sleep(self, seconds: float) -> None:
        """Warten und dabei Heartbeats bedienen. Ein Verbindungsverlust fällt beim nächsten publish() auf."""
        if self._is_connected():
            try:
                self.conn.sleep(seconds)
                return
            except Exception:  # noqa: BLE001
                self._drop()
        time.sleep(seconds)

    def close(self) -> None:
        self._drop()
