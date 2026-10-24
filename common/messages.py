"""Shared message helpers used by producers and consumers"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from jsonschema import Draft7Validator, FormatChecker
from jsonschema.exceptions import ValidationError

MT_SENSOR_READING = "sensor.reading.v1"
MT_MONITORING_EVENT = "monitoring.event.v1"
MT_ALARM = "alarm.v1"

_SCHEMA_DIR = Path(__file__).resolve().parents[1] / "contracts" / "schemas"
_SCHEMA_FILES = {
    MT_SENSOR_READING: _SCHEMA_DIR / "sensor-reading.v1.json",
    MT_MONITORING_EVENT: _SCHEMA_DIR / "monitoring-event.v1.json",
    MT_ALARM: _SCHEMA_DIR / "alarm.v1.json",
}
_VALIDATORS: dict[str, Draft7Validator] = {}


def sensor_types() -> dict:
    """Return supported sensor types and their constraints from schema enums/ranges."""
    return {
        "temperature": {"unit": "\u00b0C", "min": -40.0, "max": 150.0},
        "humidity": {"unit": "%", "min": 0.0, "max": 100.0},
        "brightness": {"unit": "lx", "min": 0.0, "max": 150000.0},
    }


def new_message_id() -> str:
    return str(uuid.uuid4())


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def encode(message: dict) -> bytes:
    return json.dumps(message, separators=(",", ":")).encode("utf-8")


def validate(message_type: str, payload: dict) -> dict:
    """Validate payload against JSON schema for the given message type"""
    if not isinstance(payload, dict):
        raise ValueError("payload must be a JSON object")

    validator = _validator_for(message_type)
    try:
        validator.validate(payload)
    except ValidationError as exc:
        path = ".".join(str(part) for part in exc.path)
        where = f" at '{path}'" if path else ""
        raise ValueError(f"schema validation failed{where}: {exc.message}") from exc

    return payload


def _validator_for(message_type: str) -> Draft7Validator:
    if message_type not in _SCHEMA_FILES:
        raise ValueError(f"unsupported message type: {message_type}")

    if message_type in _VALIDATORS:
        return _VALIDATORS[message_type]

    schema_path = _SCHEMA_FILES[message_type]
    with schema_path.open("r", encoding="utf-8") as handle:
        schema = json.load(handle)

    validator = Draft7Validator(schema, format_checker=FormatChecker())
    _VALIDATORS[message_type] = validator
    return validator
