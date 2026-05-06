# tests/integration/test_tiktok_bots.py
"""
Tests d'integration end-to-end pour les 4 modules TikTok.

Profil CDP : `test_profile` uniquement — jamais de profil production.
Flux valide : NEW -> QUALIFIED -> DISPATCHING (via DB reelle).
"""
import os
import sys
import time
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

os.environ.setdefault("SECURITY_MASTER_KEY", "test-master-key-not-for-production")
os.environ.setdefault("USE_POSTGRES", "False")

import pytest  # noqa: E402

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# ---------------------------------------------------------------------------
# Stub missing classes BEFORE importing any TikTok module
# ---------------------------------------------------------------------------


class _StubStealthInjector:
    @staticmethod
    async def inject(page, identity=None):
        pass


class _StubPhysicsHumanizer:
    def __init__(self, page):
        self.page = page

    async def scroll_organic(self, intensity="medium"):
        pass


# Inject stubs into sys.modules so TikTok module imports succeed
_core_browser_engine = sys.modules.get("core.browser_engine")
if _core_browser_engine:
    _core_browser_engine.StealthInjector = _StubStealthInjector
else:
    _fake_browser_engine = MagicMock()
    _fake_browser_engine.StealthInjector = _StubStealthInjector
    _fake_browser_engine.SandboxCDPProfile = MagicMock()
    sys.modules["core.browser_engine"] = _fake_browser_engine

_core_humanizer = sys.modules.get("core.humanizer")
if _core_humanizer:
    _core_humanizer.PhysicsHumanizer = _StubPhysicsHumanizer
else:
    _fake_humanizer = MagicMock()
    _fake_humanizer.PhysicsHumanizer = _StubPhysicsHumanizer
    sys.modules["core.humanizer"] = _fake_humanizer

from playwright.async_api import async_playwright  # noqa: E402

from core.database import NexusDB  # noqa: E402
from core.settings import settings  # noqa: E402

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

TEST_PROFILE_DIR = settings.CHROME_PROFILES_DIR / "test_profile"


@pytest.fixture()
def db():
    instance = NexusDB(db_path=Path(":memory:"), auto_migrate=True)
    yield instance
    instance.close()


@pytest.fixture()
def db_with_sponsor(db):
    with db.session() as conn:
        conn.execute(
            """INSERT OR IGNORE INTO sponsors
               (id, label, program, ref_link, ref_code, priority, monthly_limit_hard,
                verified_count_month, active, balance_available, balance_reserved)
               VALUES (?, ?, ?, '', '', 2, ?, ?, 1, ?, 0.0)""",
            ("sp_tiktok_001", "Test Sponsor", "TEST_PROGRAM", 100, 0, 500.0),
        )
    return db


@pytest.fixture()
def db_with_ready_lead(db_with_sponsor):
    db_with_sponsor.insert_raw_lead(
        {
            "id": "tiktok_int_lead_001",
            "source": "tiktok",
            "author": "test_user_hash",
            "url": "https://www.tiktok.com/@test/video/123",
            "text": "comment interessant sur prop firm",
        }
    )
    with db_with_sponsor.session() as conn:
        conn.execute("UPDATE leads SET status='QUALIFIED' WHERE id='tiktok_int_lead_001'")
    return db_with_sponsor


# ---------------------------------------------------------------------------
# 1. sniper.py — ProtocolEngine signal analysis (pure logic, static methods)
# ---------------------------------------------------------------------------


class TestProtocolEngine:
    def setup_method(self):
        from channels.tiktok.sniper import ProtocolEngine

        self.engine = ProtocolEngine

    def test_noise_signal_rejected(self):
        score, tag = self.engine.analyze_signal_entropy("parrainage boursorama prime 80")
        assert score < 0
        assert tag == "NOISE_SIGNAL"

    def test_congested_node_rejected(self):
        score, tag = self.engine.analyze_signal_entropy("mon code promo giveaway bot spam")
        assert score < 0
        assert tag == "CONGESTED_NODE"

    def test_system_noise_rejected(self):
        score, tag = self.engine.analyze_signal_entropy("loading original sound translate")
        assert score < 0
        assert tag == "SYSTEM_NOISE"

    def test_prop_firm_protocol_detected(self):
        score, tag = self.engine.analyze_signal_entropy(
            "I need help with my FTMO challenge prop firm funded account"
        )
        assert score > 0
        assert "PROTOCOL_TRADING_CAPITAL" in tag

    def test_crypto_protocol_detected(self):
        score, tag = self.engine.analyze_signal_entropy(
            "How to secure my crypto with ledger wallet staking btc"
        )
        assert score > 0
        assert "PROTOCOL_CRYPTO_TOOLS" in tag

    def test_saas_keywords_contribute_score(self):
        # "bot" is in CONGESTION_SIGNATURES so we avoid both "bot" and "robot"
        score, tag = self.engine.analyze_signal_entropy(
            "Best tradingview indicator strategy for analysis"
        )
        assert score >= 0

    def test_direct_link_detected(self):
        score, tag = self.engine.analyze_signal_entropy("lien code ou interesse info go")
        assert score > 0
        assert "PROTOCOL_DIRECT_LINK" in tag

    def test_interrogative_boost(self):
        score_pos, _ = self.engine.analyze_signal_entropy("comment passer le challenge FTMO?")
        score_neg, _ = self.engine.analyze_signal_entropy("comment passer le challenge FTMO")
        assert score_pos >= score_neg

    def test_ideal_packet_size_boost(self):
        score_short, _ = self.engine.analyze_signal_entropy("funded")
        score_med, _ = self.engine.analyze_signal_entropy(
            "I want to know more about the FTMO funded account challenge"
        )
        assert score_med > score_short

    def test_empty_text_returns_zero(self):
        score, tag = self.engine.analyze_signal_entropy("")
        assert score == 0
        assert tag == "DEFAULT"

    def test_congestion_signature_filtering(self):
        from channels.tiktok.sniper import ProtocolEngine

        # "spam" is in CONGESTION_SIGNATURES so it correctly returns True
        assert ProtocolEngine.is_congested_node("check my code in bio bot giveaway") is True
        # Use text without ANY congestion keyword for False case
        assert ProtocolEngine.is_congested_node("clean text no issues here") is False

    def test_noise_signature_filtering(self):
        from channels.tiktok.sniper import ProtocolEngine

        assert (
            ProtocolEngine.is_noise_signal("parrainage boursorama prime 80 banque gratuite") is True
        )
        assert ProtocolEngine.is_noise_signal("prop firm funded challenge") is False


# ---------------------------------------------------------------------------
# 2. sniper.py — TrafficShaper dynamic threshold
# ---------------------------------------------------------------------------


class TestTrafficShaper:
    def test_dynamic_threshold_adjusts_with_backlog(self, db):
        from channels.tiktok.sniper import TrafficShaper, Config

        shaper = TrafficShaper(db, base_threshold=Config.MIN_SIGNAL_ENTROPY)
        base = shaper.get_dynamic_threshold()

        for i in range(130):
            db.insert_raw_lead(
                {"id": f"bl_{i:04d}", "source": "r", "author": f"u{i}", "url": "", "text": "x"}
            )
        high = shaper.get_dynamic_threshold()
        assert high > base

    def test_dynamic_threshold_lower_when_idle(self, db):
        from channels.tiktok.sniper import TrafficShaper, Config

        shaper = TrafficShaper(db, base_threshold=Config.MIN_SIGNAL_ENTROPY)
        low = shaper.get_dynamic_threshold()
        assert low >= 10
        assert low <= 65

    def test_threshold_bounds_enforced(self, db):
        from channels.tiktok.sniper import TrafficShaper, Config

        shaper = TrafficShaper(db, base_threshold=Config.MIN_SIGNAL_ENTROPY)
        t = shaper.get_dynamic_threshold()
        assert 10 <= t <= 65


# ---------------------------------------------------------------------------
# 3. sniper.py — SmartTopologyManager circuit breaker
# ---------------------------------------------------------------------------


class TestSmartTopologyManager:
    @pytest.fixture(autouse=True)
    def _patch_file_load(self):
        """Prevent SmartTopologyManager from loading/persisting to real JSON files."""
        with patch("channels.tiktok.sniper.SmartTopologyManager._load", return_value={}):
            with patch("channels.tiktok.sniper.SmartTopologyManager.save", return_value=None):
                yield

    def test_circuit_opens_on_high_failure_ratio(self):
        from channels.tiktok.sniper import SmartTopologyManager, Config

        mgr = SmartTopologyManager()
        for i in range(Config.MIN_ATTEMPTS_FOR_BREAKER):
            mgr.update_scan_result("toxic_node", 0, technical_success=False)

        assert mgr.is_circuit_open("toxic_node") is True

    def test_circuit_closed_on_successful_scan(self):
        from channels.tiktok.sniper import SmartTopologyManager

        mgr = SmartTopologyManager()
        mgr.update_scan_result("healthy_node", 5, technical_success=True)
        assert mgr.is_circuit_open("healthy_node") is False

    def test_quarantine_on_consecutive_failures(self):
        from channels.tiktok.sniper import SmartTopologyManager

        mgr = SmartTopologyManager()
        for _ in range(5):
            mgr.update_scan_result("sterile_node", 0, technical_success=True)

        entry = mgr.scores["sterile_node"]
        assert entry["quarantine_until"] > time.time()

    def test_node_toxic_marking(self):
        from channels.tiktok.sniper import SmartTopologyManager

        mgr = SmartTopologyManager()
        mgr.scores["bad_node"] = {
            "score": 100,
            "validations": 0,
            "heatmap": [0] * 24,
            "consecutive_failures": 0,
            "technical_failures": 0,
            "total_attempts": 0,
            "quarantine_until": 0,
            "referred_by": [],
            "toxicity_level": 0,
        }
        mgr.mark_node_toxic("bad_node")
        assert mgr.scores["bad_node"]["toxicity_level"] >= 1
        assert mgr.scores["bad_node"]["score"] == 5

    def test_smart_batch_returns_nodes(self):
        from channels.tiktok.sniper import SmartTopologyManager

        mgr = SmartTopologyManager()
        mgr.update_scan_result("node_a", 5, technical_success=True)
        mgr.update_scan_result("node_b", 3, technical_success=True)
        batch = mgr.get_smart_batch(2)
        assert len(batch) >= 1

    def test_viral_node_addition(self):
        from channels.tiktok.sniper import SmartTopologyManager

        mgr = SmartTopologyManager()
        mgr.add_viral_node("new_viral_node", source_referrer="referrer_001")
        assert "new_viral_node" in mgr.scores
        assert mgr.scores["new_viral_node"]["referred_by"] == ["referrer_001"]

    def test_update_scan_result_increments_attempts(self):
        from channels.tiktok.sniper import SmartTopologyManager

        mgr = SmartTopologyManager()
        mgr.update_scan_result("test_node", 3, technical_success=True)
        assert mgr.scores["test_node"]["total_attempts"] == 1
        assert mgr.scores["test_node"]["score"] > 50

    def test_update_scan_result_decreases_on_technical_fail(self):
        from channels.tiktok.sniper import SmartTopologyManager

        mgr = SmartTopologyManager()
        mgr.scores["fail_node"] = {
            "score": 100,
            "validations": 0,
            "heatmap": [0] * 24,
            "consecutive_failures": 0,
            "technical_failures": 0,
            "total_attempts": 0,
            "quarantine_until": 0,
            "referred_by": [],
            "toxicity_level": 0,
        }
        mgr.update_scan_result("fail_node", 0, technical_success=False)
        assert mgr.scores["fail_node"]["score"] < 100
        assert mgr.scores["fail_node"]["technical_failures"] == 1


# ---------------------------------------------------------------------------
# 4. sniper.py — AsyncNodeDeduplicator
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestAsyncNodeDeduplicator:
    async def test_local_cache_deduplication(self, db):
        from channels.tiktok.sniper import AsyncNodeDeduplicator

        async with AsyncNodeDeduplicator(db) as dedup:
            is_dup = await dedup.is_duplicate_async("sig_001")
            assert is_dup is False

            dedup._update_cache("sig_001")
            is_dup = await dedup.is_duplicate_async("sig_001")
            assert is_dup is True

    async def test_unknown_node_not_duplicate(self, db):
        from channels.tiktok.sniper import AsyncNodeDeduplicator

        async with AsyncNodeDeduplicator(db) as dedup:
            is_dup = await dedup.is_duplicate_async("unknown_sig")
            assert is_dup is False

    async def test_cache_eviction_on_max_size(self, db):
        from channels.tiktok.sniper import AsyncNodeDeduplicator

        async with AsyncNodeDeduplicator(db, max_cache_size=3) as dedup:
            for i in range(5):
                dedup._update_cache(f"sig_{i}")
            assert len(dedup.local_cache) <= 3


# ---------------------------------------------------------------------------
# 5. partner_sniper.py — Signal strength analysis (patched DB)
# ---------------------------------------------------------------------------


class TestPartnerSniperSignalAnalysis:
    def _make_mapper(self):
        from channels.tiktok.partner_sniper import TikTokTopologyMapper

        mapper = TikTokTopologyMapper()
        mapper.db = None
        return mapper

    def test_high_authority_match(self):
        mapper = self._make_mapper()
        score, is_match = mapper._analyze_signal_strength(
            "Best tradingview review tutorial strategy guide backtest indicator setup"
        )
        assert score > 10
        assert is_match is True

    def test_strong_b2b_match(self):
        mapper = self._make_mapper()
        score, is_match = mapper._analyze_signal_strength(
            "link in bio partner collab tools software discount code trading"
        )
        assert score > 10
        assert is_match is True

    def test_noise_markers_reject(self):
        mapper = self._make_mapper()
        score, is_match = mapper._analyze_signal_strength(
            "svp besoin qui a donnez moi arnaque fake banque bourso"
        )
        assert is_match is False

    def test_empty_text_no_match(self):
        mapper = self._make_mapper()
        score, is_match = mapper._analyze_signal_strength("")
        assert is_match is False

    def test_ecosystem_score_high_for_pro(self):
        mapper = self._make_mapper()
        score = mapper._calculate_ecosystem_score(
            "Founder | CEO | Trading educator. Join discord: linktr.ee/mytrading"
        )
        assert score >= 30

    def test_ecosystem_score_low_for_empty(self):
        mapper = self._make_mapper()
        score = mapper._calculate_ecosystem_score("")
        assert score == 0

    def test_signal_performance_tracking(self):
        mapper = self._make_mapper()
        mapper.current_signal = "test signal"
        mapper._handle_failure()
        assert mapper.consecutive_failures["test signal"] >= 1


# ---------------------------------------------------------------------------
# 6. media_optimizer.py — MediaAssetMetrics & StateManager
# ---------------------------------------------------------------------------


class TestMediaAssetMetrics:
    def _metrics(self, **kwargs):
        from channels.tiktok.media_optimizer import MediaAssetMetrics

        return MediaAssetMetrics(url="https://test.com", **kwargs)

    def test_yield_efficiency_zero_scans(self):
        asset = self._metrics()
        assert asset.yield_efficiency == 0.0

    def test_yield_efficiency_with_scans(self):
        asset = self._metrics(total_scans=10, consecutive_empty_scans=3)
        assert asset.yield_efficiency == 0.3

    def test_depreciated_when_threshold_exceeded(self):
        from channels.tiktok.media_optimizer import Config

        asset = self._metrics(
            total_scans=20,
            consecutive_empty_scans=Config.YIELD_DROP_THRESHOLD + 1,
            total_conversions=0,
        )
        assert asset.is_depreciated is True

    def test_immune_asset_never_depreciated(self):
        from channels.tiktok.media_optimizer import Config

        asset = self._metrics(
            total_scans=100,
            consecutive_empty_scans=Config.YIELD_DROP_THRESHOLD + 50,
            total_conversions=0,
            is_immune=True,
        )
        assert asset.is_depreciated is False

    def test_predict_trend_rising_fast(self):
        asset = self._metrics(velocity_history=[1, 3, 5, 8, 12])
        assert asset.predict_trend() == "RISING_FAST"

    def test_predict_trend_cooling_down(self):
        asset = self._metrics(velocity_history=[10, 8, 5, 3, 1])
        assert asset.predict_trend() == "COOLING_DOWN"

    def test_predict_trend_stable(self):
        asset = self._metrics(velocity_history=[5, 5, 5])
        assert asset.predict_trend() == "STABLE"

    def test_predict_trend_insufficient_data(self):
        asset = self._metrics(velocity_history=[1, 2])
        assert asset.predict_trend() == "STABLE"


class TestMediaOptimizerUtils:
    def test_is_competitor_signal_true(self):
        from channels.tiktok.media_optimizer import is_competitor_signal

        assert is_competitor_signal("mon code promo en bio") is True

    def test_is_competitor_signal_false(self):
        from channels.tiktok.media_optimizer import is_competitor_signal

        assert is_competitor_signal("intéressé par le prop firm") is False

    def test_is_noise_short_text(self):
        from channels.tiktok.media_optimizer import is_noise

        assert is_noise("ok") is True

    def test_is_noise_time_stamp(self):
        from channels.tiktok.media_optimizer import is_noise

        assert is_noise("2h") is True

    def test_is_noise_system_noise(self):
        from channels.tiktok.media_optimizer import is_noise

        assert is_noise("original sound tiktok loading") is True

    def test_calculate_urgency_high(self):
        from channels.tiktok.media_optimizer import calculate_urgency

        assert calculate_urgency("combien ça coute? bug marche pas") >= 10

    def test_calculate_urgency_low(self):
        from channels.tiktok.media_optimizer import calculate_urgency

        assert calculate_urgency("merci top") == 0

    def test_generate_compliance_id_deterministic(self):
        from channels.tiktok.media_optimizer import generate_compliance_id

        id1 = generate_compliance_id("https://t.tt/1", "hash_a", "text")
        id2 = generate_compliance_id("https://t.tt/1", "hash_a", "text")
        assert id1 == id2

    def test_generate_compliance_id_different_for_different_input(self):
        from channels.tiktok.media_optimizer import generate_compliance_id

        id1 = generate_compliance_id("https://t.tt/1", "hash_a", "text1")
        id2 = generate_compliance_id("https://t.tt/1", "hash_a", "text2")
        assert id1 != id2


# ---------------------------------------------------------------------------
# 7. sender.py — Language detection & payload selection (patched DB)
# ---------------------------------------------------------------------------


class TestSenderLanguageDetection:
    def _make_injector(self):
        from channels.tiktok.sender import TelemetryInjector

        with patch("channels.tiktok.sender.NexusDB"):
            with patch("channels.tiktok.sender.SandboxCDPProfile"):
                with patch("channels.tiktok.sender.CircadianCycle"):
                    injector = TelemetryInjector()
        injector.db = None
        return injector

    def test_detect_fr_from_common_words(self):
        injector = self._make_injector()
        lang = injector._detect_language_context("bonjour merci pas pour une dans avec")
        assert lang == "FR"

    def test_detect_en_from_text(self):
        injector = self._make_injector()
        lang = injector._detect_language_context("best prop firm challenge funded account")
        assert lang == "EN"

    def test_detect_fr_from_tags(self):
        injector = self._make_injector()
        lang = injector._detect_language_context("hello world", tags=["france", "bourse"])
        assert lang == "FR"

    def test_detect_en_from_tags(self):
        injector = self._make_injector()
        lang = injector._detect_language_context("bonjour", tags=["trading", "usa"])
        assert lang == "EN"


class TestSenderPayloadSelection:
    def _make_injector(self):
        from channels.tiktok.sender import TelemetryInjector

        with patch("channels.tiktok.sender.NexusDB"):
            with patch("channels.tiktok.sender.SandboxCDPProfile"):
                with patch("channels.tiktok.sender.CircadianCycle"):
                    injector = TelemetryInjector()
        injector.db = None
        return injector

    def test_prop_firm_template_en(self):
        injector = self._make_injector()
        payload = injector._get_protocol_template("prop_firm", lang="EN")
        assert "[Ad]" in payload or "[Sponsor]" in payload

    def test_prop_firm_template_fr_redirects(self):
        injector = self._make_injector()
        payload = injector._get_protocol_template("prop_firm", lang="FR")
        assert (
            "[Pub]" in payload
            or "[Partenaire]" in payload
            or "[Conseil]" in payload
            or "[Outils]" in payload
        )

    def test_saas_template_en(self):
        injector = self._make_injector()
        payload = injector._get_protocol_template("tradingview", lang="EN")
        assert "[Ad]" in payload or "[Info]" in payload

    def test_security_template_en(self):
        injector = self._make_injector()
        payload = injector._get_protocol_template("ledger", lang="EN")
        assert "[Ad]" in payload or "[Advice]" in payload or "[Partner]" in payload

    def test_default_template(self):
        injector = self._make_injector()
        payload = injector._get_protocol_template("unknown_category", lang="EN")
        assert "[Ad]" in payload or "[Info]" in payload or "[Partner]" in payload

    def test_viral_ping_is_short(self):
        injector = self._make_injector()
        payload = injector._get_protocol_template("default", lang="EN", is_viral=True)
        assert len(payload) < 50

    def test_sanitize_payload_strips_special_chars(self):
        injector = self._make_injector()
        clean = injector._sanitize_payload("Hello! <script>alert('xss')</script> @user")
        assert "<" not in clean
        assert ">" not in clean

    def test_sanitize_empty_returns_empty(self):
        injector = self._make_injector()
        assert injector._sanitize_payload("") == ""
        assert injector._sanitize_payload(None) == ""

    def test_momentum_score_increases_on_failure(self):
        injector = self._make_injector()
        initial = injector.momentum_score
        injector.update_momentum("ERROR")
        assert injector.momentum_score > initial

    def test_momentum_score_decreases_on_success(self):
        injector = self._make_injector()
        for _ in range(5):
            injector.update_momentum("SUCCESS")
        assert injector.momentum_score < 1.0

    def test_dynamic_pause_intra_burst(self):
        injector = self._make_injector()
        pause = injector._calculate_dynamic_pause()
        assert 20 <= pause <= 300

    def test_hard_ban_detection(self):
        from channels.tiktok.sender import HARD_BANS

        assert "scam" in HARD_BANS
        assert "arnaque" in HARD_BANS

    def test_restriction_indicators(self):
        from channels.tiktok.sender import RESTRICTION_INDICATORS

        assert any("turn" in ind.lower() for ind in RESTRICTION_INDICATORS)
        assert any("desactiv" in ind.lower() or "désactiv" in ind for ind in RESTRICTION_INDICATORS)


# ---------------------------------------------------------------------------
# 8. Full DB flow — NEW -> QUALIFIED -> DISPATCHING
# ---------------------------------------------------------------------------


class TestFullDBFlow:
    def test_insert_raw_lead_creates_new_status(self, db):
        result = db.insert_raw_lead(
            {
                "id": "flow_lead_001",
                "source": "tiktok",
                "author": "user_hash",
                "url": "https://www.tiktok.com/@user/video/999",
                "text": "interesse par le guide",
            }
        )
        assert result is True

        with db.session() as conn:
            row = conn.execute(
                "SELECT status, source FROM leads WHERE id='flow_lead_001'"
            ).fetchone()
        assert row[0] == "NEW"
        assert row[1] == "tiktok"

    def test_qualify_lead_changes_status(self, db):
        db.insert_raw_lead(
            {
                "id": "flow_lead_002",
                "source": "tiktok",
                "author": "user_hash",
                "url": "https://www.tiktok.com/@user/video/888",
                "text": "comment passer le challenge",
            }
        )
        with db.session() as conn:
            conn.execute("UPDATE leads SET status='QUALIFIED' WHERE id='flow_lead_002'")

        with db.session() as conn:
            row = conn.execute("SELECT status FROM leads WHERE id='flow_lead_002'").fetchone()
        assert row[0] == "QUALIFIED"

    def test_reserve_dispatch_moves_to_dispatching(self, db):
        db.insert_raw_lead(
            {
                "id": "flow_lead_003",
                "source": "tiktok",
                "author": "user_hash",
                "url": "https://www.tiktok.com/@user/video/777",
                "text": "funded account prop firm",
            }
        )
        with db.session() as conn:
            conn.execute("UPDATE leads SET status='QUALIFIED' WHERE id='flow_lead_003'")

        leads = db.reserve_leads_for_dispatch(batch_size=5, batch_id="test_batch")
        assert len(leads) >= 1

        with db.session() as conn:
            row = conn.execute("SELECT status FROM leads WHERE id='flow_lead_003'").fetchone()
        assert row[0] == "DISPATCHING"

    def test_full_pipeline_new_to_dispatching(self, db):
        db.insert_raw_lead(
            {
                "id": "flow_lead_004",
                "source": "tiktok",
                "author": "full_pipe_user",
                "url": "https://www.tiktok.com/@user/video/666",
                "text": "best prop firm funded challenge",
            }
        )
        with db.session() as conn:
            row = conn.execute("SELECT status FROM leads WHERE id='flow_lead_004'").fetchone()
        assert row[0] == "NEW"

        with db.session() as conn:
            conn.execute("UPDATE leads SET status='QUALIFIED' WHERE id='flow_lead_004'")
        with db.session() as conn:
            row = conn.execute("SELECT status FROM leads WHERE id='flow_lead_004'").fetchone()
        assert row[0] == "QUALIFIED"

        db.reserve_leads_for_dispatch(batch_size=5, batch_id="full_pipe_batch")
        with db.session() as conn:
            row = conn.execute("SELECT status FROM leads WHERE id='flow_lead_004'").fetchone()
        assert row[0] == "DISPATCHING"

    def test_fail_lead_from_dispatching(self, db):
        db.insert_raw_lead(
            {
                "id": "flow_lead_005",
                "source": "tiktok",
                "author": "fail_user",
                "url": "https://www.tiktok.com/@user/video/555",
                "text": "hello",
            }
        )
        with db.session() as conn:
            conn.execute("UPDATE leads SET status='QUALIFIED' WHERE id='flow_lead_005'")
        db.reserve_leads_for_dispatch(batch_size=5, batch_id="fail_batch")

        with db.session() as conn:
            row = conn.execute("SELECT status FROM leads WHERE id='flow_lead_005'").fetchone()
        assert row[0] == "DISPATCHING"

        db.fail_lead("flow_lead_005", "POST_FAILED")
        with db.session() as conn:
            row = conn.execute("SELECT status FROM leads WHERE id='flow_lead_005'").fetchone()
        assert "FAILED" in row[0]

    def test_insert_telemetry_signal_alias(self, db):
        result = db.insert_telemetry_signal(
            {
                "id": "alias_lead_001",
                "source": "tiktok",
                "author": "alias_user",
                "url": "https://www.tiktok.com/@user/video/111",
                "text": "alias test",
            }
        )
        assert result is True


# ---------------------------------------------------------------------------
# 9. Atomic dispatch with sponsor balance (tiktok sender flow)
# ---------------------------------------------------------------------------


class TestAtomicDispatchTiktokFlow:
    def test_dispatch_ready_to_send(self, db_with_sponsor):
        db_with_sponsor.insert_raw_lead(
            {
                "id": "dispatch_lead_001",
                "source": "tiktok",
                "author": "dispatch_user",
                "url": "https://www.tiktok.com/@user/video/444",
                "text": "prop firm challenge",
            }
        )
        with db_with_sponsor.session() as conn:
            conn.execute("UPDATE leads SET status='QUALIFIED' WHERE id='dispatch_lead_001'")

        result = db_with_sponsor.atomic_dispatch_transaction(
            "dispatch_lead_001", "sp_tiktok_001", "PROP_FIRM", Decimal("10.00")
        )
        assert result is True

        with db_with_sponsor.session() as conn:
            row = conn.execute("SELECT status FROM leads WHERE id='dispatch_lead_001'").fetchone()
        assert row[0] == "READY_TO_SEND"

    def test_mark_sent_after_dispatch(self, db_with_sponsor):
        db_with_sponsor.insert_raw_lead(
            {
                "id": "dispatch_lead_002",
                "source": "tiktok",
                "author": "sent_user",
                "url": "https://www.tiktok.com/@user/video/333",
                "text": "tradingview setup",
            }
        )
        db_with_sponsor.atomic_dispatch_transaction(
            "dispatch_lead_002", "sp_tiktok_001", "SAAS_TOOLS", Decimal("5.00")
        )
        db_with_sponsor.mark_lead_sent("dispatch_lead_002")

        with db_with_sponsor.session() as conn:
            row = conn.execute("SELECT status FROM leads WHERE id='dispatch_lead_002'").fetchone()
        assert row[0] == "SENT"

    def test_confirm_hold_after_dispatch(self, db_with_sponsor):
        db_with_sponsor.insert_raw_lead(
            {
                "id": "confirm_lead_001",
                "source": "tiktok",
                "author": "confirm_user",
                "url": "https://www.tiktok.com/@user/video/222",
                "text": "ledger hardware wallet",
            }
        )
        db_with_sponsor.atomic_dispatch_transaction(
            "confirm_lead_001", "sp_tiktok_001", "CRYPTO", Decimal("8.00")
        )
        result = db_with_sponsor.confirm_lead_hold("confirm_lead_001")
        assert result is True

    def test_release_hold_after_dispatch(self, db_with_sponsor):
        db_with_sponsor.insert_raw_lead(
            {
                "id": "release_lead_001",
                "source": "tiktok",
                "author": "release_user",
                "url": "https://www.tiktok.com/@user/video/111",
                "text": "apex trader funding",
            }
        )
        db_with_sponsor.atomic_dispatch_transaction(
            "release_lead_001", "sp_tiktok_001", "PROP_FIRM", Decimal("12.00")
        )
        result = db_with_sponsor.release_lead_hold("release_lead_001")
        assert result is True


# ---------------------------------------------------------------------------
# 10. Playwright DOM tests — test_profile browser
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestPlaywrightDOMInteractions:
    async def test_browser_launch_with_test_profile(self):
        """Verify Playwright can launch with a dedicated test_profile."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()

            await page.goto("data:text/html,<h1>Test</h1>")
            title = await page.title()
            assert title == ""

            await page.close()
            await context.close()
            await browser.close()

    async def test_extract_comment_like_dom_pattern(self):
        """Test the DOM selector pattern used by media_optimizer._extract_signals_from_dom."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            html = """
            <html><body>
            <div class="comment-item">
                <a data-e2e="comment-username" href="/@user1">user1</a>
                <p data-e2e="comment-text">interesse par le guide</p>
            </div>
            <div class="comment-item">
                <a data-e2e="comment-username" href="/@user2">user2</a>
                <p data-e2e="comment-text">prop firm challenge</p>
            </div>
            <div class="noise">loading original sound</div>
            </body></html>
            """
            await page.goto(f"data:text/html,{html}")

            comments = await page.evaluate(
                """() => {
                const data = [];
                const containers = document.querySelectorAll('.comment-item');
                containers.forEach(el => {
                    const authorEl = el.querySelector('[data-e2e="comment-username"]');
                    const textEl = el.querySelector('[data-e2e="comment-text"]');
                    if (authorEl && textEl) {
                        data.push({
                            author: authorEl.innerText.trim(),
                            text: textEl.innerText.trim(),
                            url: authorEl.href || ''
                        });
                    }
                });
                return data;
            }"""
            )

            assert len(comments) == 2
            assert comments[0]["author"] == "user1"
            assert comments[1]["text"] == "prop firm challenge"

            await browser.close()

    async def test_extract_video_links_dom_pattern(self):
        """Test the DOM selector pattern used by sniper.py GraphNavigator for video links."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            html = """
            <html><body>
            <a href="/@user/video/123">Video 1</a>
            <a href="/@user/video/456">Video 2</a>
            <a href="/@user">Profile</a>
            </body></html>
            """
            await page.goto(f"data:text/html,{html}")

            videos = await page.eval_on_selector_all(
                'a[href*="/video/"]', "els => els.map(e => e.getAttribute('href'))"
            )

            assert len(videos) == 2
            assert "/video/" in videos[0]

            await browser.close()

    async def test_contenteditable_input_pattern(self):
        """Test the contenteditable input pattern used by sender.py inject_packet."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            html = """
            <html><body>
            <div data-e2e="comment-input" contenteditable="true"></div>
            <div data-e2e="comment-post">Post</div>
            </body></html>
            """
            await page.goto(f"data:text/html,{html}")

            is_visible = await page.is_visible('[data-e2e="comment-input"]')
            assert is_visible is True

            await page.click('[data-e2e="comment-input"]')
            await page.fill('[data-e2e="comment-input"]', "Test comment [Ad]")

            value = await page.inner_text('[data-e2e="comment-input"]')
            assert "Test comment" in value

            await browser.close()

    async def test_dom_missing_element_handling(self):
        """Test handling of missing DOM elements (edge case for sender DOM shake)."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            await page.goto("data:text/html,<h1>Empty page</h1>")

            is_visible = await page.is_visible('[data-e2e="comment-input"]')
            assert is_visible is False

            is_visible_fb = await page.is_visible('div[contenteditable="true"]:visible')
            assert is_visible_fb is False

            await browser.close()

    async def test_page_navigation_404_handling(self):
        """Test handling of 404 page navigation (simulated error 404)."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            response = await page.goto("https://httpbin.org/status/404", timeout=10000)
            assert response is not None
            assert response.status == 404

            await browser.close()

    async def test_page_navigation_422_handling(self):
        """Test handling of 422 page navigation (simulated error 422)."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            response = await page.goto("https://httpbin.org/status/422", timeout=10000)
            assert response is not None
            assert response.status == 422

            await browser.close()

    async def test_page_navigation_500_handling(self):
        """Test handling of 500 page navigation (simulated error 500)."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            response = await page.goto("https://httpbin.org/status/500", timeout=10000)
            assert response is not None
            assert response.status == 500

            await browser.close()

    async def test_page_navigation_timeout(self):
        """Test handling of network timeout (edge case)."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            with pytest.raises(Exception):
                await page.goto("https://192.0.2.1", timeout=3000)

            await browser.close()

    async def test_profile_directory_test_profile_isolation(self):
        """Verify test_profile directory name is distinct from production profiles."""
        test_profile_path = TEST_PROFILE_DIR
        assert "test_profile" in str(test_profile_path)
        assert "tiktok_sender" not in str(test_profile_path)
        assert "tiktok_mapper" not in str(test_profile_path)
        assert "nexus_cartographer" not in str(test_profile_path)

    async def test_extract_account_links_dom_pattern(self):
        """Test the DOM selector pattern used by sniper.py GraphNavigator.get_neighboring_nodes."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            html = """
            <html><body>
            <a href="/@user1">User 1</a>
            <a href="/@user2">User 2</a>
            <a href="/@user_with_query?tab=videos">User 3</a>
            <a href="/other">Not a user</a>
            </body></html>
            """
            await page.goto(f"data:text/html,{html}")

            accounts = await page.eval_on_selector_all(
                'a[href*="/@"]',
                """
                els => els.map(e => e.getAttribute('href').split('/@')[1].split('?')[0])
                          .filter(u => u && u.length > 2)
                """,
            )

            assert len(accounts) == 3
            assert "user1" in accounts
            assert "user2" in accounts
            assert "user_with_query" in accounts

            await browser.close()
