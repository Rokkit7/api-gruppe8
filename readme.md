# Repo-Struktur (Iteration 1)

```text
.
├── contracts/
│   ├── asyncapi.yaml                   # Channels, Operationen, Nachrichten (Header, Metadata)
│   ├── schemas/                        # Struktur der Nachrichteninhalte (Payload)
│   │   ├── sensor-reading.v1.json
│   │   ├── alarm.v1.json
│   │   └── monitoring-event.v1.json
│   └── examples/*.json                 # Beispielnachrichten zum Testen
├── common/
│   ├── config.py                       # Env-Variablen, Konstanten
│   ├── topology.py                     # (2.4) Exchanges, Queues, Bindings
│   ├── rmq.py                          # RabbitMQ: connect(), Backoff, Jitter, Publish mit Confirms
│   └── messages.py                     # Validierung gegen die Schemas
├── producer/                           # (2.1)
├── analysis/                           # (2.2)
├── alarm/                              # (2.3)
├── monitoring/                         # (2.5)
├── resilience/                         # (2.6) Resilience tests
│   ├── test_consumer_crash.py          # Consumer crash and message redelivery
│   ├── test_invalid_message.py         # Invalid message and DLQ handling
│   ├── run_tests.py                    # Automated execution of both tests
│   ├── dlq_inspector.py                # DLQ inspection, if present
│   └── README.md                       # Test documentation
└── docker-compose.yml                  # als lokale RabbitMQ-Umgebung
```
