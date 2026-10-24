"""Simulierte Sensoren (Aufgabe 2.1: mindestens drei Sensortypen).

Ableitung von Einheiten und plausiblen Wertebereichen aus dem Vertrag
(contracts/schemas/sensor-reading.v1.json über common.messages) und werden nicht doppelt gepflegt.
"""
import random
from dataclasses import dataclass, field

from common import messages

SENSOR_TYPES = messages.sensor_types()   # temperature, humidity, brightness


@dataclass
class Sensor:
    sensor_id: str
    type: str
    location: str
    baseline: float                 # Normalwert
    noise: float                    # typische Schwankung/Rauschen
    outliers: list[float]           # Werte bei einer simulierten Anomalie (Ausreißererkennung)
    current: float = field(init=False)

    def __post_init__(self):
        if self.type not in SENSOR_TYPES:
            raise ValueError(f"Sensortyp {self.type!r} ist nicht im Vertrag definiert")
        self.current = self.baseline

    @property
    def unit(self) -> str:
        return SENSOR_TYPES[self.type]["unit"]

    def next_value(self, rng: random.Random, anomaly_rate: float) -> float:
        """Zufallsbewegung um den Normalwert, gelegentlich ein Ausreißer.

        Ob ein Wert eine Anomalie ist, entscheidet 2.2 – der Producer liefert nur
        realistische Daten mit gelegentlichen Ausreißern."""
        self.current += (self.baseline - self.current) * 0.2 + rng.gauss(0, self.noise)
        value = self.current
        if rng.random() < anomaly_rate:
            value = rng.choice(self.outliers) + rng.uniform(-self.noise, self.noise)
        limits = SENSOR_TYPES[self.type]
        return round(min(max(value, limits["min"]), limits["max"]), 2)   # nie außerhalb des Vertrags


SENSORS = [
    #       sensor_id      type           location  Normalwert Rauschen  Ausreißer
    Sensor("TEMP-H1-01", "temperature", "halle1",    60.0,     1.5,  [86.0, 104.0]),   # zu heiß
    Sensor("TEMP-H2-01", "temperature", "halle2",    55.0,     1.5,  [84.0, 103.0]),
    Sensor("HUM-H1-01",  "humidity",    "halle1",    45.0,     2.0,  [88.0, 97.0]),    # zu feucht
    Sensor("LUX-H1-01",  "brightness",  "halle1",   500.0,    15.0,  [150.0, 40.0]),   # Beleuchtung fällt aus
]