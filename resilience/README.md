
## 2.6 Resilienztests

### Übersicht der Tests

| Frage | Test 1: Consumer Crash | Test 2: Invalid Message |
|---|---|---|
| **Fehlerauslösung** | Consumer-Absturz durch `os._exit(1)` | Ungültiger Messwert `value = "INVALID"` |
| **Nachrichtenverhalten** | Unbestätigte Nachricht wird erneut verfügbar | Ungültige Nachricht wird abgelehnt |
| **ACK/NACK** | Kein ACK/NACK vor dem Absturz | NACK ohne Requeue |
| **Redelivery** | Ja, `redelivered=True` | Nein, keine erneute Zustellung in der normalen Queue |
| **Fehlerbehandlung** | RabbitMQ stellt die Nachricht erneut bereit | JSON-Schema-Validierung und Monitoring-Event |
| **Dead Letter Queue** | Keine direkte Weiterleitung durch den Absturz | Nachricht gelangt mit `rejected` in die DLQ |
| **Weiterverarbeitung** | Neuer Consumer kann die Nachricht verarbeiten; automatische Prüfung bestätigt bisher die Redelivery | Consumer verarbeitet danach erfolgreich eine gültige Nachricht |

### Verwendete Dateien und Funktionen

| Datei | Funktionen / Verwendung |
|---|---|
| `resilience/test_consumer_crash.py` | `crash_consumer()`, `send_message()`, `check_redelivery()` – simuliert den Absturz und prüft die erneute Zustellung |
| `resilience/test_invalid_message.py` | `send_invalid_message()`, `check_dlq()`, `send_valid_message()` – testet Validierungsfehler, DLQ und Weiterverarbeitung |
| `resilience/run_tests.py` | `test_crash()`, `test_invalid()` – automatisiert beide Tests und zeigt die Ergebnisse |
| `analysis/consumer.py` | `run_consumer()`, `on_message()` – verarbeitet Sensornachrichten |
| `producer/sensor_producer.py` | `build_reading()`, `reading_properties()` – erstellt Sensornachrichten |
| `common/messages.py` | `validate()` – prüft Nachrichten anhand des JSON-Schemas |
| `common/rmq.py` | `ReliablePublisher` – veröffentlicht Nachrichten über RabbitMQ |
| `common/topology.py` | `setup_rabbitmq()` – richtet Exchanges, Queues und Dead-Letter-Routing ein |

### Automatische Ausführung

**Test 1 – Consumer Crash**

    python -m resilience.run_tests crash

**Test 2 – Invalid Message**

    python -m resilience.run_tests invalid

Beide Tests wurden erfolgreich mit `TEST 1 PASSED` beziehungsweise `TEST 2 PASSED` ausgeführt.
