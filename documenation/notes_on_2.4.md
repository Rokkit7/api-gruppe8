# 2.4

## Exchanges und Queue-Design
Die zwei gewählten Exhanges sind direct und topic. 

Für die Exchanges zwischen Analysis-Consumer und den Producern sowie zwischen Analysis-Consumer und Monitoring-Consumer wurde topic gewählt, da somit der Nachrichtenaustausch flexibler gestaltet werden kann.
Für mehrere Producer kann somit nur ein Consumer zur Verfügung stehen, oder mehrere, welche jeweils einen Teil der Nachrichten verarbeiten, in dem sie auf das jeweilige Topic abonnieren.
FÜr den Monitor ist es somit ebenfalls möglich, auf die einzelnen Topics zu abonnieren, um somit die Nachrichten der Producer mitzulesen.
Alternativ ist für den Austausch zwischen Analysis-Consumer und Producer auch direct möglich, solange für einen Producer auch ein Consumer zur Verfügung steht, der die Nachrichten verarbeitet.
Für das Versenden von Alerts wird direct verwendet, damit der Alert-Consumer die Nachrichten direkt auf seperate Routing-Keys empfangen kann, bereitgestellt vom Analysis-Consumer.
Somit können die Alerts direkt an den Alarm-Consumer weitergeleitet werden, ohne dass der Alarm-Consumer auf die einzelnen Topics der Producer abonnieren muss.
Alternativ ist es möglich die vom Consumer empfangenen Nachrichten über Fanout an Alarm und Monitoring Consumer weiterzuleiten, beispielsweise um eine Kopie der Warnung an den Monitoring Consumer zu senden.
Für die Dead Letter Queue (DLQ) wurde ebenfalls direct gewählt, da Nachrichten vom Analysis-Consumer verarbeitet werden und somit direkt von diesem an die DLQ weitergeleitet werden können, ohne dass ein weiterer Consumer auf die einzelnen Topics der Producer abonnieren muss.



## TTL
TTL definiert in analysis/topology.py. 

> "x-message-ttl": 60_000,

Eine TTL bei Sensordaten ergibt Sinn, da die Daten jeweils nur einen punktuellen Zustand des Sensors repräsentieren. 
Ältere Daten können somit Verfälschungen in der Analyse hervorrufen. Da Daten über den Zeitraum von 10 Sekunden aggregiert werden,
sind Daten, welche älter als 10 Sekunden sind, nicht mehr relevant, da sie einen alten Zustand darstellen, und somit die Aggregation der Daten verfälschen.
Abgelaufene Nachrichten werden demnach als falsch betrachtet und somit in die Dead Letter Queue (DLQ) verschoben, wo auch andere Nachrichten landen, die nicht verarbeitet werden konnten.

## Dead Letter Queue

DLQ definiert in analysis/topology.py.

- Nimmt Nachrichten auf, welche vom Analysis Consumer mit NACK abgelehnt wurden, oder deren TTL abgelaufen ist

