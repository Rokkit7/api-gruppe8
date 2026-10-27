## Architekturdiagramm

```mermaid
flowchart LR
    %% Externe Komponenten
    P[Producer\nproducer/sensor_producer.py]
    T[Topology Setup\nscripts/setup_rabbitmq.py]
    R[Resilience Tests\nresilience/*.py]

    %% RabbitMQ
    SX[[sensor-exchange\nTopic Exchange]]
    AX[[alarm-exchange\nDirect Exchange]]
    MX[[monitoring-exchange\nTopic Exchange]]
    DLX[[dlx\nDead Letter Exchange]]

    TQ[(analysis.temperature)]
    EQ[(analysis.environment)]
    AWQ[(alarm.warning)]
    ACQ[(alarm.critical)]
    MQ[(monitoring.analysis)]
    DLQ[(analysis.dlq)]

    %% Consumer / Services
    AC1[Analysis Consumer\nMode: temperature\nanalysis/consumer.py]
    AC2[Analysis Consumer\nMode: environment\nanalysis/consumer.py]
    AL1[Alarm Consumer\nalarm/consumer.py]
    MON[Monitoring Consumer\nmonitoring/monitoring_consumer.py]

    %% Verträge / gemeinsame Logik
    C1[contracts/\nasyncapi + schemas]
    C2[common/\nconfig.py\nmessages.py\nrmq.py\ntopology.py]

    %% Hauptfluss
    P -->|publish sensor readings| SX
    SX -->|sensor.temperature.*| TQ
    SX -->|sensor.humidity.*\nsensor.brightness.*| EQ

    TQ --> AC1
    EQ --> AC2

    %% Analysis Consumer Ausgänge
    AC1 -->|alarm warning/critical| AX
    AC2 -->|alarm warning/critical| AX
    AC1 -->|monitoring events| MX
    AC2 -->|monitoring events| MX

    %% Alarm Routing
    AX -->|alarm.warning| AWQ
    AX -->|alarm.critical| ACQ
    AWQ --> AL1
    ACQ --> AL1

    %% Monitoring Routing
    MX -->|monitoring.analysis| MQ
    MQ --> MON

    %% Dead Letter Flow
    TQ -->|TTL abgelaufen oder rejected| DLX
    EQ -->|TTL abgelaufen oder rejected| DLX
    DLX --> DLQ

    %% Resilience
    R -->|Crash Test / Invalid Message Test| TQ
    R -->|DLQ Check| DLQ

    %% Gemeinsame Basis
    C1 --> P
    C1 --> AC1
    C1 --> AC2
    C1 --> AL1
    C1 --> MON

    C2 --> P
    C2 --> AC1
    C2 --> AC2
    C2 --> AL1
    C2 --> MON
    C2 --> R
    C2 --> T
