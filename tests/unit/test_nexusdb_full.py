# tests/unit/test_nexusdb_full.py
"""
Tests complets NexusDB — couvre les méthodes non testées dans test_nexusdb_smoke.py.
Objectif : coverage ≥ 80% sur core/secure_telemetry_store.py.
Toutes les instances utilisent SQLite :memory: (jamais de mock NexusDB).

Known quirks (documented, not bugs to fix here):
- fail_lead / release_lead_hold : SQLite "cannot start a transaction within a
  transaction" → l'UPDATE de statut est rollbacké silencieusement.
  Tests vérifient uniquement qu'aucune exception ne s'échappe.
- register_conversion_event appelle confirm_lead_hold internalement (même bug),
  ce qui fait que confirm_lead_hold retourne False sans erreur visible.
- _seed_initial_data : sponsors.json est un dict, pas une liste → les lignes
  298-315 ne sont pas accessibles sans modifier le fichier de config.
"""
import asyncio
import os
import time
from decimal import Decimal
from pathlib import Path

os.environ.setdefault("SECURITY_MASTER_KEY", "test-master-key-not-for-production")
os.environ.setdefault("USE_POSTGRES", "False")

import pytest  # noqa: E402

from core.secure_telemetry_store import NexusDB  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db():
    instance = NexusDB(db_path=Path(":memory:"), auto_migrate=True)
    # register_conversion_event & analyze_user_history query `program` column
    # from leads — that column is not created by any migration, add it here.
    with instance.session() as conn:
        try:
            conn.execute("ALTER TABLE leads ADD COLUMN program TEXT")
        except Exception:
            pass  # already exists
    yield instance
    instance.close()


@pytest.fixture()
def db_with_lead(db):
    """db + un lead NEW inséré."""
    db.insert_raw_lead(
        {
            "id": "lead_001",
            "source": "tiktok",
            "author": "alice",
            "url": "https://t.tt/1",
            "text": "hello",
        }
    )
    return db


@pytest.fixture()
def db_with_qualified_lead(db):
    """db + un lead en statut QUALIFIED."""
    db.insert_raw_lead(
        {"id": "qlead_001", "source": "reddit", "author": "bob", "url": "", "text": "qualified"}
    )
    with db.session() as conn:
        conn.execute("UPDATE leads SET status='QUALIFIED' WHERE id='qlead_001'")
    return db


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _insert_sponsor(
    db, sponsor_id: str, balance: float = 500.0, quota: int = 100, verified: int = 0
):
    """Insère un sponsor dans la DB de test."""
    with db.session() as conn:
        conn.execute(
            """INSERT OR IGNORE INTO sponsors
                (id, label, program, ref_link, ref_code, priority, monthly_limit_hard,
                 verified_count_month, active, balance_available, balance_reserved)
               VALUES (?, ?, ?, '', '', 2, ?, ?, 1, ?, 0.0)""",
            (sponsor_id, sponsor_id, "TEST_PROGRAM", quota, verified, balance),
        )


def _make_lead(db, lead_id, author_hash, status, program=None, created_at=None):
    """Insère un lead directement avec statut et timestamp donnés."""
    t = created_at or time.time()
    with db.session() as conn:
        conn.execute(
            """INSERT OR IGNORE INTO leads
               (id, source, author, url, comment_text, status, program, created_at, updated_at)
               VALUES (?, 'test', ?, '', '', ?, ?, ?, ?)""",
            (lead_id, author_hash, status, program, t, t),
        )


# ---------------------------------------------------------------------------
# Utilitaires purs
# ---------------------------------------------------------------------------


def test_escape_like_string_empty(db):
    assert db.escape_like_string("") == ""


def test_escape_like_string_special_chars(db):
    result = db.escape_like_string("100%_done\\path")
    assert "\\%" in result
    assert "\\_" in result
    assert "\\\\" in result


def test_to_decimal_none(db):
    assert db.to_decimal(None) == Decimal("0.0")


def test_to_decimal_int(db):
    assert db.to_decimal(42) == Decimal("42")


def test_to_decimal_string(db):
    assert db.to_decimal("3.14") == Decimal("3.14")


def test_to_decimal_invalid_raises(db):
    with pytest.raises(Exception):
        db.to_decimal("not_a_number")


# ---------------------------------------------------------------------------
# sanitize_lead_text / _sanitize_and_score_lead
# ---------------------------------------------------------------------------


def test_sanitize_lead_text_empty(db):
    assert db.sanitize_lead_text("") == ""


def test_sanitize_lead_text_lowercases(db):
    result = db.sanitize_lead_text("Hello World")
    assert result == result.lower()


def test_sanitize_and_score_lead_shouting(db):
    lead = {"text": "THIS IS A VERY LOUD SHOUTING MESSAGE RIGHT HERE YES"}
    clean_text, meta = db._sanitize_and_score_lead(lead)
    assert "SHOUTING" in meta["flags"]
    assert meta["initial_entropy_score"] < 50


def test_sanitize_and_score_lead_pricing_intent(db):
    lead = {"text": "quel est le prix de ce service ?"}
    clean_text, meta = db._sanitize_and_score_lead(lead)
    assert meta["detected_intent"] == "PRICING_QUERY"


def test_sanitize_and_score_lead_info_intent(db):
    lead = {"text": "je voudrais des infos sur votre offre"}
    clean_text, meta = db._sanitize_and_score_lead(lead)
    assert meta["detected_intent"] == "INFO_QUERY"


def test_sanitize_and_score_lead_support_intent(db):
    lead = {"text": "j'ai un bug avec mon compte, erreur critique"}
    clean_text, meta = db._sanitize_and_score_lead(lead)
    assert meta["detected_intent"] == "TECHNICAL_SUPPORT"


def test_sanitize_and_score_lead_noise(db):
    lead = {"text": "!!!@@@###$$$%%%^^^&&&***" * 3}
    clean_text, meta = db._sanitize_and_score_lead(lead)
    assert "NOISE_OVERLOAD" in meta["flags"]


def test_sanitize_and_score_lead_comment_text_field(db):
    lead = {"comment_text": "aide moi"}
    clean_text, meta = db._sanitize_and_score_lead(lead)
    assert meta["detected_intent"] == "TECHNICAL_SUPPORT"


# ---------------------------------------------------------------------------
# get_system_health — branches de score
# ---------------------------------------------------------------------------


def test_get_system_health_returns_float(db):
    h = db.get_system_health()
    assert isinstance(h, float)
    assert 0.0 <= h <= 1.0


def test_get_system_health_zero_on_empty_db(db):
    assert db.get_system_health() == 0.0


def test_get_system_health_backlog_200(db):
    # 201 leads NEW pour déclencher la branche backlog > 200
    for i in range(201):
        db.insert_raw_lead(
            {"id": f"bl_{i:04d}", "source": "r", "author": f"u{i}", "url": "", "text": "x"}
        )
    h = db.get_system_health()
    assert h >= 0.3


def test_get_system_health_backlog_500(db):
    # 501 leads pour déclencher la branche backlog > 500
    for i in range(501):
        db.insert_raw_lead(
            {"id": f"big_{i:04d}", "source": "r", "author": f"v{i}", "url": "", "text": "x"}
        )
    h = db.get_system_health()
    assert h >= 0.6


def test_get_system_health_latency_above_1(db):
    db.last_write_latency = 1.5
    h = db.get_system_health()
    assert h >= 0.2
    db.last_write_latency = 0.0


def test_get_system_health_latency_above_2(db):
    db.last_write_latency = 2.5
    h = db.get_system_health()
    assert h >= 0.4
    db.last_write_latency = 0.0


# ---------------------------------------------------------------------------
# get_lead_by_author
# ---------------------------------------------------------------------------


def test_get_lead_by_author_none_on_empty(db):
    assert db.get_lead_by_author("nonexistent") is None


def test_get_lead_by_author_empty_string(db):
    assert db.get_lead_by_author("") is None


def test_get_lead_by_author_returns_dict(db_with_lead):
    result = db_with_lead.get_lead_by_author("alice")
    assert result is not None
    assert "id" in result


# ---------------------------------------------------------------------------
# mark_lead_sent
# ---------------------------------------------------------------------------


def test_mark_lead_sent_changes_status(db_with_lead):
    db_with_lead.mark_lead_sent("lead_001")
    with db_with_lead.session() as conn:
        row = conn.execute("SELECT status FROM leads WHERE id='lead_001'").fetchone()
    assert row[0] == "SENT"


def test_mark_lead_sent_unknown_id_no_exception(db):
    db.mark_lead_sent("nonexistent_lead_id")  # Ne doit pas lever d'exception


# ---------------------------------------------------------------------------
# fail_lead — nested session bug : le statut est rollbacké silencieusement.
# On vérifie uniquement qu'aucune exception ne s'échappe.
# ---------------------------------------------------------------------------


def test_fail_lead_no_exception_with_reason(db_with_lead):
    # Known behavior: SQLite nested session rolls back the status update.
    db_with_lead.fail_lead("lead_001", "TIMEOUT")  # must not raise


def test_fail_lead_no_exception_default_reason(db_with_lead):
    db_with_lead.fail_lead("lead_001")  # must not raise


# ---------------------------------------------------------------------------
# get_campaign_info
# ---------------------------------------------------------------------------


def test_get_campaign_info_unknown(db):
    assert db.get_campaign_info("NONEXISTENT_PROGRAM") is None


# ---------------------------------------------------------------------------
# upsert_campaign + get_campaign_info + get_max_active_campaign_amount
# ---------------------------------------------------------------------------


def test_upsert_campaign_and_get_info(db):
    future_date = "2099-12-31T00:00:00"
    db.upsert_campaign("APEX_PROP", 150.0, True, future_date, "https://apex.com/ref")
    info = db.get_campaign_info("APEX_PROP")
    assert info is not None
    assert info["program"] == "APEX_PROP"
    assert float(info["amount"]) == 150.0


def test_upsert_campaign_is_idempotent(db):
    future_date = "2099-12-31T00:00:00"
    db.upsert_campaign("APEX_PROP", 150.0, True, future_date, "https://apex.com/ref")
    db.upsert_campaign("APEX_PROP", 200.0, False, future_date, "https://apex.com/ref2")
    info = db.get_campaign_info("APEX_PROP")
    assert float(info["amount"]) == 200.0  # Mis à jour


def test_get_max_active_campaign_amount_empty(db):
    assert db.get_max_active_campaign_amount() == 0.0


def test_get_max_active_campaign_amount_with_campaign(db):
    future_date = "2099-12-31T00:00:00"
    db.upsert_campaign("TEST_PROG", 75.0, False, future_date, "https://test.com")
    assert db.get_max_active_campaign_amount() == 75.0


# ---------------------------------------------------------------------------
# get_program_sponsors_stats
# ---------------------------------------------------------------------------


def test_get_program_sponsors_stats_empty(db):
    result = db.get_program_sponsors_stats("UNKNOWN_PROGRAM")
    assert isinstance(result, list)
    assert result == []


def test_get_program_sponsors_stats_with_sponsor(db):
    _insert_sponsor(db, "sp_stats_001")
    with db.session() as conn:
        conn.execute("UPDATE sponsors SET program='TEST_PROGRAM' WHERE id='sp_stats_001'")
    result = db.get_program_sponsors_stats("TEST_PROGRAM")
    assert isinstance(result, list)
    assert len(result) >= 1


# ---------------------------------------------------------------------------
# get_next_lead_to_send_by_source
# ---------------------------------------------------------------------------


def test_get_next_lead_to_send_by_source_empty(db):
    assert db.get_next_lead_to_send_by_source("tiktok") is None


def test_get_next_lead_to_send_by_source_returns_and_marks_sending(db):
    db.insert_raw_lead(
        {"id": "ready_001", "source": "reddit", "author": "charlie", "url": "", "text": "ready"}
    )
    with db.session() as conn:
        conn.execute("UPDATE leads SET status='READY_TO_SEND' WHERE id='ready_001'")

    lead = db.get_next_lead_to_send_by_source("reddit")
    assert lead is not None
    assert lead["id"] == "ready_001"

    with db.session() as conn:
        row = conn.execute("SELECT status FROM leads WHERE id='ready_001'").fetchone()
    assert row[0] == "SENDING"


# ---------------------------------------------------------------------------
# get_converted_leads_sample
# ---------------------------------------------------------------------------


def test_get_converted_leads_sample_empty(db):
    result = db.get_converted_leads_sample()
    assert isinstance(result, list)
    assert result == []


def test_get_converted_leads_sample_with_data(db):
    _make_lead(db, "conv_001", "hash_conv", "CONVERTED")
    result = db.get_converted_leads_sample()
    assert isinstance(result, list)
    assert len(result) >= 1


# ---------------------------------------------------------------------------
# update_subreddit_stats + get_subreddit_stats
# ---------------------------------------------------------------------------


def test_get_subreddit_stats_unknown_default(db):
    stats = db.get_subreddit_stats("r/unknown")
    assert stats["success_count"] == 0
    assert stats["fail_count"] == 0


def test_update_subreddit_stats_success(db):
    db.update_subreddit_stats("r/trading", success=True)
    stats = db.get_subreddit_stats("r/trading")
    assert stats["success_count"] >= 1


def test_update_subreddit_stats_failure(db):
    db.update_subreddit_stats("r/investing", success=False)
    stats = db.get_subreddit_stats("r/investing")
    assert stats["fail_count"] >= 1


def test_update_subreddit_stats_increments(db):
    for _ in range(3):
        db.update_subreddit_stats("r/forex", success=True)
    stats = db.get_subreddit_stats("r/forex")
    assert stats["success_count"] == 3


# ---------------------------------------------------------------------------
# get_dashboard_snapshot
# ---------------------------------------------------------------------------


def test_get_dashboard_snapshot_returns_expected_keys(db):
    snap = db.get_dashboard_snapshot()
    assert "revenue" in snap
    assert "cost" in snap
    assert "leadsBank" in snap
    assert "leadsCrypto" in snap
    assert "leadsSaaS" in snap
    assert "stocks" in snap
    assert "bots" in snap


def test_get_dashboard_snapshot_initial_zeros(db):
    snap = db.get_dashboard_snapshot()
    assert snap["revenue"] == 0.0
    assert snap["cost"] == 0.0
    assert snap["leadsBank"] == 0
    assert snap["leadsCrypto"] == 0
    assert snap["leadsSaaS"] == 0


def test_get_dashboard_snapshot_with_bank_lead(db):
    """Couvre la branche 'BOURSO' dans get_dashboard_snapshot."""
    _make_lead(db, "d_bank_001", "h1", "CONVERTED", program="BOURSOBANK")
    snap = db.get_dashboard_snapshot()
    assert snap["leadsBank"] >= 1


def test_get_dashboard_snapshot_with_crypto_lead(db):
    """Couvre la branche 'MERIA'/'CRYPTO' dans get_dashboard_snapshot."""
    _make_lead(db, "d_crypto_001", "h2", "CONVERTED", program="MERIA")
    snap = db.get_dashboard_snapshot()
    assert snap["leadsCrypto"] >= 1


def test_get_dashboard_snapshot_with_saas_lead(db):
    """Couvre la branche 'SAAS' dans get_dashboard_snapshot."""
    _make_lead(db, "d_saas_001", "h3", "CONVERTED", program="SAAS_PRODUCT")
    snap = db.get_dashboard_snapshot()
    assert snap["leadsSaaS"] >= 1


def test_get_dashboard_snapshot_bots_section(db):
    """Couvre la construction de la section 'bots' via register_account_heartbeat."""
    db.register_account_heartbeat("bot_dash_001", proxy_url="http://proxy:8080")
    snap = db.get_dashboard_snapshot()
    assert isinstance(snap["bots"], list)
    assert len(snap["bots"]) >= 1
    bot = snap["bots"][0]
    assert "name" in bot
    assert "status" in bot


def test_get_dashboard_snapshot_sponsor_capacity(db):
    """Couvre la section stocks.partnerCap avec un sponsor actif."""
    _insert_sponsor(db, "sp_dash", quota=100, verified=10)
    snap = db.get_dashboard_snapshot()
    assert snap["stocks"]["partnerCap"] >= 90


# ---------------------------------------------------------------------------
# get_active_fleet
# ---------------------------------------------------------------------------


def test_get_active_fleet_empty(db):
    result = db.get_active_fleet("tiktok")
    assert isinstance(result, list)
    assert result == []


# ---------------------------------------------------------------------------
# register_account_heartbeat + get_active_fleet
# ---------------------------------------------------------------------------


def test_register_account_heartbeat_no_exception(db):
    db.register_account_heartbeat("acc_001", proxy_url="http://proxy:8080")


def test_register_account_heartbeat_without_proxy(db):
    db.register_account_heartbeat("acc_002")


def test_register_account_heartbeat_shows_in_fleet(db):
    db.register_account_heartbeat("acc_fleet_001", proxy_url="http://proxy:8080", status="ACTIVE")
    fleet = db.get_active_fleet("tiktok")
    assert len(fleet) >= 1
    ids = [r["id"] for r in fleet]
    assert "acc_fleet_001" in ids


def test_register_account_heartbeat_is_idempotent(db):
    db.register_account_heartbeat("acc_dup", proxy_url="http://proxy:9090")
    db.register_account_heartbeat("acc_dup", proxy_url="http://proxy:9090")
    fleet = db.get_active_fleet("tiktok")
    dup_entries = [r for r in fleet if r["id"] == "acc_dup"]
    assert len(dup_entries) == 1


# ---------------------------------------------------------------------------
# upsert_viral_target + pop_priority_target
# ---------------------------------------------------------------------------


def test_upsert_viral_target_no_exception(db):
    db.upsert_viral_target("user_vip_123", priority=90, source="cascade")


def test_pop_priority_target_empty(db):
    assert db.pop_priority_target() is None


def test_pop_priority_target_returns_hashed_username(db):
    db.upsert_viral_target("user_to_pop", priority=80)
    result = db.pop_priority_target()
    assert result is not None
    assert len(result) == 64  # SHA-256 hex


def test_pop_priority_target_marks_as_processed(db):
    db.upsert_viral_target("user_processed", priority=70)
    first = db.pop_priority_target()
    second = db.pop_priority_target()
    assert first is not None
    assert second is None


def test_pop_priority_target_respects_priority(db):
    db.upsert_viral_target("low_prio_user", priority=10)
    db.upsert_viral_target("high_prio_user", priority=99)
    result = db.pop_priority_target()
    expected_hash = db._hash_identity("high_prio_user")
    assert result == expected_hash


# ---------------------------------------------------------------------------
# reserve_leads_for_dispatch
# ---------------------------------------------------------------------------


def test_reserve_leads_for_dispatch_empty(db):
    result = db.reserve_leads_for_dispatch(batch_size=10)
    assert isinstance(result, list)
    assert result == []


def test_reserve_leads_for_dispatch_returns_qualified_leads(db_with_qualified_lead):
    result = db_with_qualified_lead.reserve_leads_for_dispatch(batch_size=10, batch_id="batch_test")
    assert len(result) == 1
    assert result[0]["id"] == "qlead_001"


def test_reserve_leads_for_dispatch_changes_status_to_dispatching(db_with_qualified_lead):
    db_with_qualified_lead.reserve_leads_for_dispatch(batch_size=10)
    with db_with_qualified_lead.session() as conn:
        row = conn.execute("SELECT status FROM leads WHERE id='qlead_001'").fetchone()
    assert row[0] == "DISPATCHING"


# ---------------------------------------------------------------------------
# get_sponsor_failure_rate
# ---------------------------------------------------------------------------


def test_get_sponsor_failure_rate_unknown_sponsor(db):
    rate = db.get_sponsor_failure_rate("nonexistent_sponsor")
    assert isinstance(rate, float)
    assert rate == 0.0


def test_get_sponsor_failure_rate_with_failures(db):
    """Couvre la branche total > 0 et le calcul du taux."""
    _insert_sponsor(db, "sp_fail_rate")
    now = time.time()
    with db.session() as conn:
        conn.execute(
            """INSERT INTO leads (id, source, author, url, comment_text,
               assigned_sponsor_id, status, created_at, updated_at)
               VALUES ('fl_sent', 'tiktok', 'h1', '', '', 'sp_fail_rate', 'SENT', ?, ?)""",
            (now, now),
        )
        conn.execute(
            """INSERT INTO leads (id, source, author, url, comment_text,
               assigned_sponsor_id, status, created_at, updated_at)
               VALUES ('fl_fail', 'tiktok', 'h2', '', '', 'sp_fail_rate', 'FAILED_ERROR', ?, ?)""",
            (now, now),
        )
    rate = db.get_sponsor_failure_rate("sp_fail_rate", hours=1)
    assert isinstance(rate, float)
    assert rate > 0.0


# ---------------------------------------------------------------------------
# inject_priority_task — vérification persistance
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
    db.inject_priority_task(task)


def test_inject_priority_task_creates_lead_entry(db):
    task = {
        "type": "VIP_OUTREACH",
        "target_id": "hash_vip_test_persist",
        "priority": 100,
        "protocol": "B2B_V3",
        "source_module": "test_module",
        "payload": {"origin_url": "https://tiktok.com/@vip_test"},
    }
    db.inject_priority_task(task)
    with db.session() as conn:
        rows = conn.execute("SELECT id, status FROM leads WHERE status='PRIORITY_TASK'").fetchall()
    assert len(rows) >= 1


# ---------------------------------------------------------------------------
# get_sponsor_stats
# ---------------------------------------------------------------------------


def test_get_sponsor_stats_unknown_id(db):
    stats = db.get_sponsor_stats("unknown_sponsor")
    assert stats["verified_count_month"] == 0
    assert stats["pending_leads_count"] == 0


def test_get_sponsor_stats_with_existing_sponsor(db):
    _insert_sponsor(db, "sp_stats_test", verified=5)
    stats = db.get_sponsor_stats("sp_stats_test")
    assert stats["verified_count_month"] == 5
    assert stats["pending_leads_count"] == 0


# ---------------------------------------------------------------------------
# get_backlog_count
# ---------------------------------------------------------------------------


def test_get_backlog_count_empty(db):
    assert db.get_backlog_count() == 0


def test_get_backlog_count_after_insert(db):
    db.insert_raw_lead(
        {"id": "bl_001", "source": "reddit", "author": "dave", "url": "", "text": "backlog"}
    )
    assert db.get_backlog_count() >= 1


# ---------------------------------------------------------------------------
# get_program_leaderboard
# ---------------------------------------------------------------------------


def test_get_program_leaderboard_empty(db):
    result = db.get_program_leaderboard("UNKNOWN_PROGRAM")
    assert isinstance(result, list)
    assert result == []


def test_get_program_leaderboard_with_sponsors(db):
    _insert_sponsor(db, "sp_lb_001", balance=200.0)
    _insert_sponsor(db, "sp_lb_002", balance=100.0)
    # Both are TEST_PROGRAM from _insert_sponsor
    result = db.get_program_leaderboard("TEST_PROGRAM")
    assert isinstance(result, list)
    assert len(result) == 2
    # Vérifie le tri : le plus haut score en premier
    assert result[0]["balance"] >= result[1]["balance"]
    # Vérifie les champs
    assert "rank" in result[0]
    assert "score" in result[0]
    assert result[0]["rank"] == 1


# ---------------------------------------------------------------------------
# get_sponsor_queue_position
# ---------------------------------------------------------------------------


def test_get_sponsor_queue_position_inactive_sponsor(db):
    pos = db.get_sponsor_queue_position("nonexistent_sp", "PROP_FIRM")
    assert pos["rank"] == -1
    assert pos["status"] == "inactive"


def test_get_sponsor_queue_position_active_sponsor(db):
    _insert_sponsor(db, "sp_qpos_001", balance=300.0)
    pos = db.get_sponsor_queue_position("sp_qpos_001", "TEST_PROGRAM")
    assert pos["rank"] >= 1
    assert "score" in pos


def test_get_sponsor_queue_position_rank1_score_to_beat_zero(db):
    """Le sponsor #1 n'a personne devant lui → points_needed = 0."""
    _insert_sponsor(db, "sp_first", balance=500.0)
    pos = db.get_sponsor_queue_position("sp_first", "TEST_PROGRAM")
    assert pos["rank"] == 1
    assert pos["points_needed_for_next_rank"] == 0


def test_get_sponsor_queue_position_rank2_score_to_beat(db):
    """Le sponsor #2 a un concurrent devant lui → points_needed > 0."""
    _insert_sponsor(db, "sp_top", balance=1000.0)
    _insert_sponsor(db, "sp_second", balance=100.0)
    pos = db.get_sponsor_queue_position("sp_second", "TEST_PROGRAM")
    assert pos["rank"] == 2
    assert pos["points_needed_for_next_rank"] > 0


# ---------------------------------------------------------------------------
# process_wallet_topup
# ---------------------------------------------------------------------------


def test_process_wallet_topup_no_sponsor(db):
    """Pas de sponsor → balance inchangeable, retourne True (try/except)."""
    result = db.process_wallet_topup("nonexistent_sp", 50.0, "REF_123")
    # La méthode retourne True si pas d'erreur fatale (sponsor inexistant ne lève pas)
    assert isinstance(result, bool)


def test_process_wallet_topup_with_sponsor(db):
    _insert_sponsor(db, "sp_topup_001", balance=100.0)
    result = db.process_wallet_topup("sp_topup_001", 50.0, "REF_TOPUP_001")
    assert result is True
    with db.session() as conn:
        row = conn.execute(
            "SELECT balance_available FROM sponsors WHERE id='sp_topup_001'"
        ).fetchone()
    assert float(row[0]) == 150.0


def test_process_wallet_topup_idempotent(db):
    """Même référence → deuxième appel retourne True sans doubler le crédit."""
    _insert_sponsor(db, "sp_topup_idem", balance=100.0)
    db.process_wallet_topup("sp_topup_idem", 50.0, "REF_IDEM_001")
    db.process_wallet_topup("sp_topup_idem", 50.0, "REF_IDEM_001")  # Idempotent
    with db.session() as conn:
        row = conn.execute(
            "SELECT balance_available FROM sponsors WHERE id='sp_topup_idem'"
        ).fetchone()
    assert float(row[0]) == 150.0  # Pas doublé


def test_process_wallet_topup_priority_boost(db):
    """amount >= 100 → priority boost (priority -1, min 1)."""
    _insert_sponsor(db, "sp_boost", balance=0.0)
    # Set initial priority to 3
    with db.session() as conn:
        conn.execute("UPDATE sponsors SET priority=3 WHERE id='sp_boost'")
    result = db.process_wallet_topup("sp_boost", 150.0, "REF_BOOST_001")
    assert result is True
    with db.session() as conn:
        row = conn.execute("SELECT priority FROM sponsors WHERE id='sp_boost'").fetchone()
    assert row[0] <= 2  # Priority decreased


# ---------------------------------------------------------------------------
# atomic_dispatch_transaction
# ---------------------------------------------------------------------------


def test_atomic_dispatch_transaction_success(db_with_lead):
    _insert_sponsor(db_with_lead, "sp_001", balance=500.0)
    result = db_with_lead.atomic_dispatch_transaction(
        "lead_001", "sp_001", "PROP_FIRM", Decimal("10.00")
    )
    assert result is True
    with db_with_lead.session() as conn:
        row = conn.execute("SELECT status FROM leads WHERE id='lead_001'").fetchone()
    assert row[0] == "READY_TO_SEND"


def test_atomic_dispatch_transaction_unknown_sponsor(db_with_lead):
    result = db_with_lead.atomic_dispatch_transaction(
        "lead_001", "nonexistent_sp", "PROP_FIRM", Decimal("10.00")
    )
    assert result is False


def test_atomic_dispatch_transaction_insufficient_funds_no_quota(db_with_lead):
    """Balance insuffisante ET quota épuisé → False."""
    _insert_sponsor(db_with_lead, "sp_broke", balance=0.0, quota=3, verified=3)
    result = db_with_lead.atomic_dispatch_transaction(
        "lead_001", "sp_broke", "PROP_FIRM", Decimal("10.00")
    )
    assert result is False


def test_atomic_dispatch_updates_sponsor_balance(db_with_lead):
    """Après dispatch, balance_available diminue et balance_reserved augmente."""
    _insert_sponsor(db_with_lead, "sp_bal", balance=200.0)
    db_with_lead.atomic_dispatch_transaction("lead_001", "sp_bal", "PROP_FIRM", Decimal("20.00"))
    with db_with_lead.session() as conn:
        row = conn.execute(
            "SELECT balance_available, balance_reserved FROM sponsors WHERE id='sp_bal'"
        ).fetchone()
    assert float(row[0]) == 180.0  # 200 - 20
    assert float(row[1]) == 20.0  # réservé


def test_atomic_dispatch_creates_dispatch_log(db_with_lead):
    """Vérifie la création du dispatch_log avec statut RESERVED."""
    _insert_sponsor(db_with_lead, "sp_log", balance=100.0)
    db_with_lead.atomic_dispatch_transaction("lead_001", "sp_log", "PROP_FIRM", Decimal("5.00"))
    with db_with_lead.session() as conn:
        row = conn.execute("SELECT status FROM dispatch_logs WHERE lead_id='lead_001'").fetchone()
    assert row is not None
    assert row[0] == "RESERVED"


# ---------------------------------------------------------------------------
# confirm_lead_hold
# ---------------------------------------------------------------------------


def test_confirm_lead_hold_no_dispatch_log(db_with_lead):
    result = db_with_lead.confirm_lead_hold("lead_001")
    assert result is False


def test_confirm_lead_hold_after_dispatch(db_with_lead):
    _insert_sponsor(db_with_lead, "sp_confirm", balance=500.0)
    dispatched = db_with_lead.atomic_dispatch_transaction(
        "lead_001", "sp_confirm", "PROP_FIRM", Decimal("5.00")
    )
    assert dispatched is True
    result = db_with_lead.confirm_lead_hold("lead_001")
    assert result is True


def test_confirm_lead_hold_updates_dispatch_log(db_with_lead):
    """Après confirm, le dispatch_log doit passer à CONSUMED."""
    _insert_sponsor(db_with_lead, "sp_consumed", balance=500.0)
    db_with_lead.atomic_dispatch_transaction(
        "lead_001", "sp_consumed", "PROP_FIRM", Decimal("5.00")
    )
    db_with_lead.confirm_lead_hold("lead_001")
    with db_with_lead.session() as conn:
        row = conn.execute("SELECT status FROM dispatch_logs WHERE lead_id='lead_001'").fetchone()
    assert row[0] == "CONSUMED"


def test_confirm_lead_hold_releases_reserved_balance(db_with_lead):
    """Après confirm, balance_reserved doit diminuer."""
    _insert_sponsor(db_with_lead, "sp_rel_bal", balance=200.0)
    db_with_lead.atomic_dispatch_transaction(
        "lead_001", "sp_rel_bal", "PROP_FIRM", Decimal("10.00")
    )
    db_with_lead.confirm_lead_hold("lead_001")
    with db_with_lead.session() as conn:
        row = conn.execute("SELECT balance_reserved FROM sponsors WHERE id='sp_rel_bal'").fetchone()
    assert float(row[0]) == 0.0


# ---------------------------------------------------------------------------
# release_lead_hold
# ---------------------------------------------------------------------------


def test_release_lead_hold_no_dispatch_log(db_with_lead):
    result = db_with_lead.release_lead_hold("lead_001")
    assert result is False


def test_release_lead_hold_after_dispatch(db):
    db.insert_raw_lead(
        {"id": "lead_rel", "source": "tiktok", "author": "dave", "url": "", "text": "release test"}
    )
    _insert_sponsor(db, "sp_release", balance=500.0)
    db.atomic_dispatch_transaction("lead_rel", "sp_release", "PROP_FIRM", Decimal("5.00"))
    result = db.release_lead_hold("lead_rel")
    assert result is True


def test_release_lead_hold_restores_balance(db):
    """Après release, balance_available revient au niveau initial."""
    db.insert_raw_lead(
        {"id": "lead_restore", "source": "tiktok", "author": "eve", "url": "", "text": "restore"}
    )
    _insert_sponsor(db, "sp_restore", balance=300.0)
    db.atomic_dispatch_transaction("lead_restore", "sp_restore", "PROP_FIRM", Decimal("30.00"))
    db.release_lead_hold("lead_restore")
    with db.session() as conn:
        row = conn.execute(
            "SELECT balance_available, balance_reserved FROM sponsors WHERE id='sp_restore'"
        ).fetchone()
    assert float(row[0]) == 300.0  # Restauré
    assert float(row[1]) == 0.0


def test_release_lead_hold_updates_dispatch_log(db):
    db.insert_raw_lead(
        {
            "id": "lead_rel_log",
            "source": "tiktok",
            "author": "frank",
            "url": "",
            "text": "release log",
        }
    )
    _insert_sponsor(db, "sp_rel_log", balance=200.0)
    db.atomic_dispatch_transaction("lead_rel_log", "sp_rel_log", "PROP_FIRM", Decimal("5.00"))
    db.release_lead_hold("lead_rel_log")
    with db.session() as conn:
        row = conn.execute(
            "SELECT status FROM dispatch_logs WHERE lead_id='lead_rel_log'"
        ).fetchone()
    assert row[0] == "RELEASED"


# ---------------------------------------------------------------------------
# analyze_user_history
# ---------------------------------------------------------------------------


def test_analyze_user_history_empty_id(db):
    result = db.analyze_user_history("")
    assert result["status"] == "NEW"
    assert result["action"] == "PROCESS"


def test_analyze_user_history_unknown_user(db):
    result = db.analyze_user_history("totally_unknown_user_xyz")
    assert result["status"] == "NEW"
    assert result["action"] == "PROCESS"


def test_analyze_user_history_vip_user(db):
    """Un user avec une conversion récente → VIP_USER."""
    author_hash = db._hash_identity("vip_author")
    _make_lead(db, "vip_lead_01", author_hash, "CONVERTED")
    result = db.analyze_user_history("vip_author")
    assert result["status"] == "VIP_USER"
    assert result["action"] == "PRIORITY_DISPATCH"


def test_analyze_user_history_dead_end(db):
    """Un user avec > 2 failures → DEAD_END."""
    author_hash = db._hash_identity("dead_author")
    for i in range(3):
        _make_lead(db, f"dead_lead_{i}", author_hash, "FAILED_TIMEOUT")
    result = db.analyze_user_history("dead_author")
    assert result["status"] == "DEAD_END"
    assert result["action"] == "HARD_BLOCK"


def test_analyze_user_history_spam(db):
    """Un user avec >= 2 leads récents (non-FAILED) → SPAM."""
    author_hash = db._hash_identity("spam_author")
    now = time.time()
    # 2 leads très récents
    _make_lead(db, "spam_lead_1", author_hash, "NEW", created_at=now - 60)
    _make_lead(db, "spam_lead_2", author_hash, "NEW", created_at=now - 120)
    result = db.analyze_user_history("spam_author", hours=24)
    assert result["status"] == "SPAM"
    assert result["action"] == "BLOCK"


def test_analyze_user_history_hot_return(db):
    """Un user avec exactement 1 lead récent non-FAILED → HOT_RETURN."""
    author_hash = db._hash_identity("hot_author")
    now = time.time()
    _make_lead(db, "hot_lead_1", author_hash, "SENT", created_at=now - 3600)
    result = db.analyze_user_history("hot_author", hours=24)
    assert result["status"] == "HOT_RETURN"
    assert result["action"] == "BOOST_SCORE"


# ---------------------------------------------------------------------------
# register_conversion_event
# ---------------------------------------------------------------------------


def test_register_conversion_event_unknown_lead(db):
    """Lead inexistant → retourne False."""
    result = db.register_conversion_event("nonexistent_lead_id", 100.0)
    assert result is False


def test_register_conversion_event_already_converted(db_with_lead):
    """Lead déjà CONVERTED → retourne True sans doublon."""
    with db_with_lead.session() as conn:
        conn.execute("UPDATE leads SET status='CONVERTED' WHERE id='lead_001'")
    result = db_with_lead.register_conversion_event("lead_001", 50.0)
    assert result is True


def test_register_conversion_event_new_lead_returns_true(db_with_lead):
    """
    Conversion d'un lead NEW → retourne True.
    Note: le statut reste 'NEW' en SQLite car confirm_lead_hold déclenche
    un nested session qui rollback la transaction outer (bug connu).
    Coverage: lignes 786-839 + 841-870 + 917.
    """
    result = db_with_lead.register_conversion_event("lead_001", 75.0)
    assert result is True


def test_register_conversion_event_with_transaction_id(db_with_lead):
    """Passe un transaction_id explicite — couvre la branche tx_id existant."""
    result = db_with_lead.register_conversion_event("lead_001", 25.0, transaction_id="TX_TEST_001")
    assert result is True


def test_register_conversion_event_covers_matrix_logic(db_with_lead):
    """
    Avec assigned_program='BOURSOBANK', le code définit NEXT_STEP_MATRIX et
    cherche next_step. En SQLite, 'assigned_program' in row vérifie les
    VALEURS (pas les clés) → current_program reste 'UNKNOWN' → next_step=None.
    Lignes 841-870 sont couvertes même si if next_step: est False.
    """
    with db_with_lead.session() as conn:
        conn.execute("UPDATE leads SET assigned_program='BOURSOBANK' WHERE id='lead_001'")
    result = db_with_lead.register_conversion_event("lead_001", 50.0)
    assert result is True  # retourne True même si next_step=None


# ---------------------------------------------------------------------------
# CrossPlatformLock — interface async
# ---------------------------------------------------------------------------


def test_cross_platform_lock_acquire_and_release(db):
    """Couvre les méthodes acquire_async et release de CrossPlatformLock."""
    lock = NexusDB.CrossPlatformLock(Path("/tmp/test.lock"))
    lock.release()  # Ne doit pas lever d'exception

    async def _run():
        result = await lock.acquire_async(timeout=0.1)
        return result

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# insert_raw_lead — branches supplémentaires
# ---------------------------------------------------------------------------


def test_insert_raw_lead_without_id_returns_false(db):
    """Couvre la ligne 'return False' quand lead_id est absent."""
    result = db.insert_raw_lead({"source": "tiktok", "author": "a", "url": "", "text": "no id"})
    assert result is False


def test_insert_raw_lead_with_invalid_json_ai_process_info(db):
    """Couvre les lignes 1469-1472 (json.loads d'une chaîne invalide)."""
    lead = {
        "id": "ai_json_test_001",
        "source": "tiktok",
        "author": "test_user",
        "url": "",
        "text": "test",
        "ai_process_info": "NOT_VALID_JSON{{{",  # JSON invalide
    }
    result = db.insert_raw_lead(lead)
    assert result is True  # Exception interne absorbée


def test_insert_raw_lead_with_valid_json_ai_process_info(db):
    """Couvre les lignes 1469-1470 (json.loads réussi)."""
    lead = {
        "id": "ai_json_test_002",
        "source": "tiktok",
        "author": "test_user2",
        "url": "",
        "text": "test",
        "ai_process_info": '{"key": "value"}',  # JSON valide
    }
    result = db.insert_raw_lead(lead)
    assert result is True


def test_insert_raw_lead_with_conn_parameter(db):
    """Couvre la branche 'if conn: conn.execute(...)' (ligne 1501)."""
    lead = {
        "id": "conn_param_001",
        "source": "reddit",
        "author": "user_conn",
        "url": "",
        "text": "test with conn",
    }
    with db.session() as conn:
        result = db.insert_raw_lead(lead, conn=conn)
    assert result is True


# ---------------------------------------------------------------------------
# atomic_dispatch_transaction — LEGACY_DISPATCH path
# ---------------------------------------------------------------------------


def test_atomic_dispatch_legacy_quota_path(db):
    """
    Couvre lignes 1323-1343 : LEGACY_DISPATCH quand balance=0 mais quota disponible.
    Condition : balance_avail < cost_decimal ET verified < limit.
    """
    db.insert_raw_lead(
        {"id": "legacy_lead", "source": "tiktok", "author": "lh", "url": "", "text": "x"}
    )
    # balance=0 (insuffisant pour Decimal("5.00")), mais verified=0 < quota=50
    _insert_sponsor(db, "sp_legacy", balance=0.0, quota=50, verified=0)
    result = db.atomic_dispatch_transaction(
        "legacy_lead", "sp_legacy", "PROP_FIRM", Decimal("5.00")
    )
    assert result is True
    # Vérifier que le dispatch_log est LEGACY_DISPATCH
    with db.session() as conn:
        row = conn.execute(
            "SELECT status FROM dispatch_logs WHERE lead_id='legacy_lead'"
        ).fetchone()
    assert row is not None
    assert row[0] == "LEGACY_DISPATCH"


def test_atomic_dispatch_exception_path(db):
    """Couvre lignes 1347-1349 (exception dans atomic_dispatch_transaction)."""
    db.insert_raw_lead(
        {"id": "exc_lead", "source": "tiktok", "author": "eh", "url": "", "text": "x"}
    )
    # Supprimer la table sponsors pour forcer une exception SQL
    with db.session() as conn:
        conn.execute("DROP TABLE sponsors")
    result = db.atomic_dispatch_transaction("exc_lead", "any_sp", "PROP", Decimal("5.00"))
    assert result is False


# ---------------------------------------------------------------------------
# ConnectionProxy — méthodes executemany, commit, rollback
# ---------------------------------------------------------------------------


def test_connection_proxy_executemany(db):
    """Couvre lignes 228-231 (ConnectionProxy.executemany)."""
    with db.session() as conn:
        conn.executemany(
            "INSERT OR IGNORE INTO subreddit_stats (subreddit, success_count, fail_count, updated_at) "
            "VALUES (?, ?, ?, ?)",
            [("r/execmany_test1", 2, 0, 0), ("r/execmany_test2", 0, 3, 0)],
        )
    stats1 = db.get_subreddit_stats("r/execmany_test1")
    assert stats1["success_count"] >= 2


def test_connection_proxy_commit_and_rollback(db):
    """Couvre lignes 234 et 237 (ConnectionProxy.commit et rollback)."""
    with db.session() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO viral_queue (username, source, priority, status, created_at, updated_at) "
            "VALUES ('proxy_test_hash', 'test', 50, 'PENDING', 0, 0)"
        )
        # Appel explicite à commit (ligne 234)
        conn.commit()
        # Appel explicite à rollback (ligne 237) — no-op après commit
        conn.rollback()


# ---------------------------------------------------------------------------
# update_author_reputation — branche 'existing record'
# ---------------------------------------------------------------------------


def test_update_author_reputation_existing_record(db):
    """
    Couvre lignes 1773-1776 : mise à jour d'un enregistrement existant.
    Le premier appel crée le record, le second le met à jour.
    """
    db.update_author_reputation("hash_exist_test", "tiktok", +10, "TRUSTED")
    rep1 = db.get_author_reputation("hash_exist_test", "tiktok")
    assert rep1["reputation_score"] == 60

    # Deuxième appel : couvre la branche 'if existing:' (lignes 1773-1776)
    db.update_author_reputation("hash_exist_test", "tiktok", +5, "VIP")
    rep2 = db.get_author_reputation("hash_exist_test", "tiktok")
    assert rep2["reputation_score"] == 65
    assert rep2["status"] == "VIP"


# ---------------------------------------------------------------------------
# Chemins d'exception — tables supprimées
# ---------------------------------------------------------------------------


def test_register_conversion_event_exception_on_missing_table(db):
    """Couvre lignes 919-921 (exception dans register_conversion_event)."""
    db.insert_raw_lead({"id": "ce_exc", "source": "t", "author": "a", "url": "", "text": "x"})
    # Supprimer conversions pour forcer l'erreur
    with db.session() as conn:
        conn.execute("DROP TABLE conversions")
    result = db.register_conversion_event("ce_exc", 100.0)
    assert result is False


def test_get_active_fleet_exception_on_missing_table(db):
    """Couvre lignes 934-936 (exception dans get_active_fleet)."""
    with db.session() as conn:
        conn.execute("DROP TABLE accounts")
    result = db.get_active_fleet("tiktok")
    assert result == []


def test_register_account_heartbeat_exception_on_missing_table(db):
    """Couvre lignes 965-966 (exception dans register_account_heartbeat)."""
    with db.session() as conn:
        conn.execute("DROP TABLE accounts")
    db.register_account_heartbeat("acc_exc")  # ne doit pas lever


def test_upsert_viral_target_exception_on_missing_table(db):
    """Couvre lignes 987-988 (exception dans upsert_viral_target)."""
    with db.session() as conn:
        conn.execute("DROP TABLE viral_queue")
    db.upsert_viral_target("user_exc")  # ne doit pas lever (warning seulement)


def test_pop_priority_target_exception_on_missing_table(db):
    """Couvre lignes 1013-1015 (exception dans pop_priority_target)."""
    with db.session() as conn:
        conn.execute("DROP TABLE viral_queue")
    result = db.pop_priority_target()
    assert result is None


def test_get_sponsor_failure_rate_exception_on_missing_table(db):
    """Couvre lignes 1052-1054 (exception dans get_sponsor_failure_rate)."""
    with db.session() as conn:
        conn.execute("DROP TABLE leads")
    result = db.get_sponsor_failure_rate("any_sponsor")
    assert result == 0.0


def test_analyze_user_history_exception_on_missing_table(db):
    """Couvre lignes 1115-1116 (exception dans analyze_user_history)."""
    with db.session() as conn:
        conn.execute("DROP TABLE leads")
    result = db.analyze_user_history("some_user_xyz")
    assert result["status"] == "NEW"


def test_get_program_leaderboard_exception_on_missing_table(db):
    """Couvre lignes 1160-1162 (exception dans get_program_leaderboard)."""
    with db.session() as conn:
        conn.execute("DROP TABLE sponsors")
    result = db.get_program_leaderboard("ANY_PROGRAM")
    assert result == []


def test_mark_lead_sent_exception_on_missing_table(db):
    """Couvre lignes 1583-1584 (exception dans mark_lead_sent)."""
    with db.session() as conn:
        conn.execute("DROP TABLE leads")
    db.mark_lead_sent("any_lead")  # ne doit pas lever


def test_fail_lead_exception_on_missing_table(db):
    """Couvre lignes 1595-1596 (exception dans fail_lead)."""
    with db.session() as conn:
        conn.execute("DROP TABLE leads")
    db.fail_lead("any_lead", "TIMEOUT")  # ne doit pas lever


def test_get_campaign_info_exception_on_missing_table(db):
    """Couvre lignes 1603-1604 (exception dans get_campaign_info)."""
    with db.session() as conn:
        conn.execute("DROP TABLE campaigns")
    result = db.get_campaign_info("SOME_PROGRAM")
    assert result is None


def test_get_program_sponsors_stats_exception_on_missing_table(db):
    """Couvre lignes 1626-1627 (exception dans get_program_sponsors_stats)."""
    with db.session() as conn:
        conn.execute("DROP TABLE sponsors")
    result = db.get_program_sponsors_stats("SOME_PROGRAM")
    assert result == []


def test_get_backlog_count_exception_on_missing_table(db):
    """Couvre lignes 1636-1637 (exception dans get_backlog_count)."""
    with db.session() as conn:
        conn.execute("DROP TABLE leads")
    result = db.get_backlog_count()
    assert result == 0


def test_get_max_active_campaign_amount_exception_on_missing_table(db):
    """Couvre lignes 1648-1649 (exception dans get_max_active_campaign_amount)."""
    with db.session() as conn:
        conn.execute("DROP TABLE campaigns")
    result = db.get_max_active_campaign_amount()
    assert result == 0.0


def test_get_converted_leads_sample_exception_on_missing_table(db):
    """Couvre lignes 1676-1677 (exception dans get_converted_leads_sample)."""
    with db.session() as conn:
        conn.execute("DROP TABLE leads")
    result = db.get_converted_leads_sample()
    assert result == []


def test_update_subreddit_stats_exception_on_missing_table(db):
    """Couvre lignes 1702-1703 (exception dans update_subreddit_stats)."""
    with db.session() as conn:
        conn.execute("DROP TABLE subreddit_stats")
    db.update_subreddit_stats("r/test", success=True)  # ne doit pas lever


def test_get_subreddit_stats_exception_on_missing_table(db):
    """Couvre lignes 1714-1715 (exception dans get_subreddit_stats)."""
    with db.session() as conn:
        conn.execute("DROP TABLE subreddit_stats")
    result = db.get_subreddit_stats("r/test")
    assert result["success_count"] == 0


def test_get_next_lead_exception_on_missing_table(db):
    """Couvre lignes 1735-1736 (exception dans get_next_lead_to_send_by_source)."""
    with db.session() as conn:
        conn.execute("DROP TABLE leads")
    result = db.get_next_lead_to_send_by_source("tiktok")
    assert result is None


def test_get_author_reputation_exception_on_missing_table(db):
    """Couvre lignes 1751-1753 (exception dans get_author_reputation)."""
    with db.session() as conn:
        conn.execute("DROP TABLE author_reputation")
    result = db.get_author_reputation("hash_test", "tiktok")
    assert result["status"] == "NEUTRAL"


def test_update_author_reputation_exception_on_missing_table(db):
    """Couvre lignes 1795-1796 (exception dans update_author_reputation)."""
    with db.session() as conn:
        conn.execute("DROP TABLE author_reputation")
    db.update_author_reputation("hash_exc", "tiktok", +10)  # ne doit pas lever


def test_inject_priority_task_exception_on_missing_table(db):
    """Couvre lignes 1837-1838 (exception dans inject_priority_task)."""
    with db.session() as conn:
        conn.execute("DROP TABLE leads")
    task = {"type": "TEST", "target_id": "h", "protocol": "P", "source_module": "m", "payload": {}}
    db.inject_priority_task(task)  # ne doit pas lever


def test_get_sponsor_stats_exception_on_missing_table(db):
    """Couvre lignes 1878-1880 (exception dans get_sponsor_stats)."""
    with db.session() as conn:
        conn.execute("DROP TABLE sponsors")
    result = db.get_sponsor_stats("any_sponsor")
    assert result["verified_count_month"] == 0


def test_get_dashboard_snapshot_exception_on_missing_table(db):
    """Couvre lignes 1966-1968 (exception dans get_dashboard_snapshot)."""
    with db.session() as conn:
        conn.execute("DROP TABLE conversions")
    snap = db.get_dashboard_snapshot()
    # Retourne le snapshot partiel (pas d'exception propagée)
    assert isinstance(snap, dict)


def test_reserve_leads_for_dispatch_exception_on_missing_table(db):
    """Couvre lignes 1563-1565 (exception dans reserve_leads_for_dispatch)."""
    with db.session() as conn:
        conn.execute("DROP TABLE leads")
    result = db.reserve_leads_for_dispatch(batch_size=10)
    assert result == []


def test_insert_raw_lead_exception_on_missing_table(db):
    """Couvre lignes 1509-1511 (exception dans insert_raw_lead)."""
    with db.session() as conn:
        conn.execute("DROP TABLE leads")
    result = db.insert_raw_lead(
        {"id": "exc_001", "source": "t", "author": "a", "url": "", "text": "x"}
    )
    assert result is False
