## Architekturdiagramm

```mermaid

flowchart TB
    P["Producer (2.1)"] --> SX["Sensor Exchange"]

    subgraph RabbitMQ
        SX --> TQ["Temperature Queue"]
        SX --> EQ["Environment Queue"]
        AX["Alarm Exchange"] --> AQ["Alarm Queues"]
        MX["Monitoring Exchange"] --> MQ["Monitoring Queue"]
        DLX["Dead Letter Exchange"] --> DLQ["Dead Letter Queue"]
    end

    TQ --> A["Analysis Consumer (2.2)"]
    EQ --> A

    A --> AX
    A --> MX
    AQ --> AL["Alarm Consumer (2.3)"]
    MQ --> M["Monitoring Consumer (2.5)"]

    TQ & EQ -->|"Rejected / TTL"| DLX

    R["Resilience Tests (2.6)"] -.-> P
    R -.-> A
    R -.-> DLQ
