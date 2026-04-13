# tests/unit/test_nexusdb_smoke.py
"""
Smoke tests NexusDB — valident que la fixture db fonctionne
et que les méthodes de base sont appelables sans crash.
"""
import sys
import os

os.environ.setdefault("SECURITY_MASTER_KEY", "test-master-key-not-for-production")
os.environ.setdefault("USE_POSTGRES", "False")

import time
from pathlib import Path
import pytest
from core.secure_telemetry_store import NexusDB


@pytest.fixture()
def db():
    instance = NexusDB(db_path=Path(":memory:"), auto_migrate=True)
    yield instance
    instance.close()


# ---------------------------------------------------------------------------
# Hash identity
# ---------------------------------------------------------------------------

def test_hash_identity_is_deterministic(db):
    h1 = db._hash_identity("user123")
    h2 = db._hash_identity("user123")
    assert h1 == h2
    assert len(h1) == 64  # SHA-256 hex


def test_hash_identity_anonymous(db):
    assert db._hash_identity("") == "anonymous_node"
    assert db._hash_identity("unknown") == "anonymous_node"


def test_hash_identity_already_hashed(db):
    raw = "user123"
    h = db._hash_identity(raw)
    # Passer un hash déjà calculé ne doit pas re-hasher
    assert db._hash_identity(h) == h


# ---------------------------------------------------------------------------
# insert_raw_lead / insert_telemetry_signal
# ---------------------------------------------------------------------------

def test_insert_raw_lead_returns_true(db):
    lead = {
        "id": "test_lead_001",
        "source": "tiktok",
        "author": "alice",
        "url": "https://tiktok.com/@alice/video/1",
        "text": "Je cherche une prop firm sérieuse",
    }
    assert db.insert_raw_lead(lead) is True


def test_insert_duplicate_lead_is_idempotent(db):
    lead = {"id": "dup_001", "source": "reddit", "author": "bob", "url": "", "text": "test"}
    assert db.insert_raw_lead(lead) is True
    assert db.insert_raw_lead(lead) is True  # ON CONFLICT DO NOTHING — pas d'exception


def test_insert_telemetry_signal_is_alias(db):
    lead = {"id": "sig_001", "source": "tiktok", "author": "carol", "url": "", "text": "signal"}
    # insert_telemetry_signal est un alias de insert_raw_lead
    assert db.insert_telemetry_signal(lead) is True


# ---------------------------------------------------------------------------
# get_author_reputation / update_author_reputation
# ---------------------------------------------------------------------------

def test_get_author_reputation_default(db):
    rep = db.get_author_reputation("nonexistent_hash", "tiktok")
    assert rep["status"] == "NEUTRAL"
    assert rep["reputation_score"] == 50


def test_update_author_reputation_positive(db):
    db.update_author_reputation("hash_abc", "tiktok", +10, "TRUSTED")
    rep = db.get_author_reputation("hash_abc", "tiktok")
    assert rep["reputation_score"] == 60
    assert rep["status"] == "TRUSTED"


def test_update_author_reputation_negative(db):
    db.update_author_reputation("hash_def", "tiktok", -5)
    rep = db.get_author_reputation("hash_def", "tiktok")
    assert rep["reputation_score"] == 45


def test_update_author_reputation_clamped_at_zero(db):
    db.update_author_reputation("hash_low", "tiktok", -200)
    rep = db.get_author_reputation("hash_low", "tiktok")
    assert rep["reputation_score"] == 0


def test_update_author_reputation_clamped_at_100(db):
    db.update_author_reputation("hash_high", "tiktok", +200)
    rep = db.get_author_reputation("hash_high", "tiktok")
    assert rep["reputation_score"] == 100


# ---------------------------------------------------------------------------
# inject_priority_task
# ---------------------------------------------------------------------------

def test_inject_priority_task_no_exception(db):
    task = {
        "type": "VIP_OUTREACH_SEQUENCE",
        "target_id": "hash_vip_001",
        "priority": 100,
        "protocol": "B2B_PARTNERSHIP_V3",
        "source_module": "topology_mapper",
        "payload": {"origin_url": "https://tiktok.com/@vip", "signal_strength": 85},
    }
    # Ne doit pas lever d'exception
    db.inject_priority_task(task)


# ---------------------------------------------------------------------------
# get_sponsor_stats
# ---------------------------------------------------------------------------

def test_get_sponsor_stats_unknown_id(db):
    stats = db.get_sponsor_stats("unknown_sponsor")
    assert stats["verified_count_month"] == 0
    assert stats["pending_leads_count"] == 0


# ---------------------------------------------------------------------------
# get_backlog_count
# ---------------------------------------------------------------------------

def test_get_backlog_count_empty(db):
    assert db.get_backlog_count() == 0


def test_get_backlog_count_after_insert(db):
    db.insert_raw_lead({"id": "bl_001", "source": "reddit", "author": "dave", "url": "", "text": "backlog"})
    # Un lead NEW est compté dans le backlog
    assert db.get_backlog_count() >= 1
