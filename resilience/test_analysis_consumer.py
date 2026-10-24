"""Unit tests for the analysis consumer logic"""

from __future__ import annotations
import pytest
from analysis.consumer import alarm_level, validate_reading


# A valid sensor reading used across multiple tests
VALID_READING = {
    "sensor_id": "s-01",
    "timestamp": "2026-10-17T18:00:00Z",
    "type": "temperature",
    "value": 91.5,
}


def test_validate_reading_accepts_valid_message() -> None:
    """A properly shaped reading should pass validation without modification"""
    assert validate_reading(VALID_READING) == VALID_READING


def test_validate_reading_rejects_invalid_numeric_value() -> None:
    """Numeric values must remain numbers; strings are rejected before processing"""
    data = {**VALID_READING, "value": "not-a-number"}
    with pytest.raises(ValueError, match="numeric"):
        validate_reading(data)


def test_validate_reading_rejects_missing_fields() -> None:
    """Missing required message fields should fail early and clearly"""
    with pytest.raises(ValueError, match="Missing required fields"):
        validate_reading({"sensor_id": "id", "type": "temperature"})


def test_alarm_level_warns_for_warning_threshold() -> None:
    """A value above the warning threshold should produce WARNING severity"""
    assert alarm_level("temperature", 85.0) == "WARNING"


def test_alarm_level_critical_for_critical_threshold() -> None:
    """A value above the critical threshold should produce CRITICAL severity"""
    assert alarm_level("temperature", 101.0) == "CRITICAL"


def test_alarm_level_returns_none_when_below_threshold() -> None:
    """Values below all defined thresholds should not trigger an alarm"""
    assert alarm_level("humidity", 10.0) is None
