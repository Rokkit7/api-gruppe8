"""Convenience script for creating the project RabbitMQ topology"""

from __future__ import annotations
from common.topology import setup_rabbitmq

if __name__ == "__main__":
    # Run with: python scripts/setup_rabbitmq.py
    setup_rabbitmq()
