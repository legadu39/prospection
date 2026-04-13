# core/database.py - Compatibility alias for NexusDB
# -*- coding: utf-8 -*-
#
# NexusDB is implemented in core/secure_telemetry_store.py.
# This module exists so that all imports across the project use a single,
# stable entrypoint: `from core.database import NexusDB`
#
# DO NOT add logic here. All DB logic lives in secure_telemetry_store.py.

from core.secure_telemetry_store import NexusDB

__all__ = ["NexusDB"]
