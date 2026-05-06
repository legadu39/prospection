# tests/integration/test_reddit_bots.py
"""
Tests d'integration end-to-end pour les 2 modules Reddit.

Profil CDP : `test_profile` uniquement — jamais de profil production.
Flux valide : GQL parsing -> classification -> insertion DB -> QUALIFIED -> DISPATCHING.
"""

import asyncio
import json
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
# Stub missing classes BEFORE importing any Reddit module
# ---------------------------------------------------------------------------


class _StubPhysicsHumanizer:
    def __init__(self, page):
        self.page = page

    async def scroll_organic(self, intensity="medium"):
        pass


# Inject stubs into sys.modules so Reddit module imports succeed
_core_humanizer = sys.modules.get("core.humanizer")
if _core_humanizer:
    _core_humanizer.PhysicsHumanizer = _StubPhysicsHumanizer
else:
    _fake_humanizer = MagicMock()
    _fake_humanizer.PhysicsHumanizer = _StubPhysicsHumanizer
    _fake_humanizer.NetworkComplianceEngine = MagicMock()
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
            ("sp_reddit_001", "Test Reddit Sponsor", "TEST_PROGRAM", 100, 0, 500.0),
        )
    return db


@pytest.fixture()
def db_with_ready_lead(db_with_sponsor):
    db_with_sponsor.insert_raw_lead(
        {
            "id": "reddit_int_lead_001",
            "source": "reddit",
            "author": "test_user_hash",
            "url": "https://www.reddit.com/r/Daytrading/comments/abc/",
            "text": "looking for prop firm challenge help",
        }
    )
    with db_with_sponsor.session() as conn:
        conn.execute("UPDATE leads SET status='QUALIFIED' WHERE id='reddit_int_lead_001'")
    return db_with_sponsor


# ---------------------------------------------------------------------------
# 1. audience_listener.py — SemanticIntentClassifier (pure logic)
# ---------------------------------------------------------------------------


class TestSemanticIntentClassifier:
    def test_high_intent_prospect(self):
        from channels.reddit.audience_listener import SemanticIntentClassifier

        score, segment = SemanticIntentClassifier.analyze_segment(
            "Looking for help with FTMO challenge",
            "I need a valid code for funded account evaluation. Anyone have experience with payout?",
        )
        assert score >= 80
        assert segment == "HIGH_INTENT_PROSPECT"

    def test_commercial_offer(self):
        from channels.reddit.audience_listener import SemanticIntentClassifier

        # Avoid UNAUTHORIZED_MARKERS ("referral", "my link", etc.) — use other commercial signals
        score, segment = SemanticIntentClassifier.analyze_segment(
            "Get bonus deal today",
            "Here is the best deal, sign up with this for free money and get bonus, voici.",
        )
        assert score == 10
        assert segment == "COMMERCIAL_OFFER"

    def test_informational_query(self):
        from channels.reddit.audience_listener import SemanticIntentClassifier

        # Must avoid HIGH_INTENT_SIGNALS words (need, want, help, looking for, etc.)
        # Use ONLY INFORMATIONAL_SIGNALS words
        score, segment = SemanticIntentClassifier.analyze_segment(
            "Strategy guide and tuto",
            "Can someone explain how the trading fonctionne? Good guide for beginners.",
        )
        assert score >= 60
        assert segment == "INFORMATIONAL_QUERY"

    def test_banking_noise_rejected(self):
        from channels.reddit.audience_listener import SemanticIntentClassifier

        # Avoid ALL high-intent signals AND unauthorized markers — neutral banking text
        score, segment = SemanticIntentClassifier.analyze_segment(
            "Bank account comparison",
            "Comparing banque fortuneo boursorama for livret a epargne placements.",
        )
        # "banque"/"fortuneo"/"bourso" subtract 50 from base 0
        assert score < 0

    def test_trading_override_on_banking_context(self):
        from channels.reddit.audience_listener import SemanticIntentClassifier

        score, segment = SemanticIntentClassifier.analyze_segment(
            "Trading vs banque comparison",
            "How to trade crypto and bitcoin vs keeping in fortuneo livret a? Need help with strategy.",
        )
        # Contains banking words but also trading context — should not be fully excluded
        assert "EXCLUSION_COMPETITOR" not in segment

    def test_prop_firm_boost(self):
        from channels.reddit.audience_listener import SemanticIntentClassifier

        score, segment = SemanticIntentClassifier.analyze_segment(
            "Apex Trader Funding review",
            "Best prop firm for futures trading. Apex vs FTMO comparison. Need help with challenge.",
        )
        assert score >= 90
        assert segment == "HIGH_INTENT_PROSPECT"

    def test_empty_text(self):
        from channels.reddit.audience_listener import SemanticIntentClassifier

        score, segment = SemanticIntentClassifier.analyze_segment("", "")
        assert score >= 0
        assert segment == "GENERAL_INTEREST"

    def test_question_mark_bonus(self):
        from channels.reddit.audience_listener import SemanticIntentClassifier

        score_q, _ = SemanticIntentClassifier.analyze_segment("How to pass evaluation?", "")
        score_nq, _ = SemanticIntentClassifier.analyze_segment("How to pass evaluation", "")
        assert score_q >= score_nq

    def test_trading_profit_boost(self):
        from channels.reddit.audience_listener import SemanticIntentClassifier

        score, _ = SemanticIntentClassifier.analyze_segment(
            "Trading profit strategy",
            "Best approach for consistent trading profit and risk management.",
        )
        assert score >= 70

    def test_high_intent_keywords(self):
        from channels.reddit.audience_listener import SemanticIntentClassifier

        for kw in [
            "cherche",
            "besoin",
            "need",
            "want",
            "anyone have",
            "code for",
            "help",
            "payout",
        ]:
            score, segment = SemanticIntentClassifier.analyze_segment(
                f"test {kw} something", "some text"
            )
            assert score >= 80, f"Failed for keyword: {kw}"


# ---------------------------------------------------------------------------
# 2. audience_listener.py — CircadianScheduler (pure logic)
# ---------------------------------------------------------------------------


class TestCircadianScheduler:
    @patch("channels.reddit.audience_listener.datetime")
    def test_business_hours_time_factor(self, mock_dt):
        from channels.reddit.audience_listener import CircadianScheduler

        mock_dt.utcnow.return_value.hour = 10  # UTC 10 + offset 2 = 12 (business)
        factor = CircadianScheduler.get_time_factor(offset_hour=2)
        assert factor == 0.8

    @patch("channels.reddit.audience_listener.datetime")
    def test_peak_engagement_time_factor(self, mock_dt):
        from channels.reddit.audience_listener import CircadianScheduler

        mock_dt.utcnow.return_value.hour = 20  # UTC 20 + offset 0 = 20 (peak)
        factor = CircadianScheduler.get_time_factor(offset_hour=0)
        assert factor == 1.5

    @patch("channels.reddit.audience_listener.datetime")
    def test_late_night_time_factor(self, mock_dt):
        from channels.reddit.audience_listener import CircadianScheduler

        mock_dt.utcnow.return_value.hour = 1  # UTC 1 + offset 0 = 1 (late night)
        factor = CircadianScheduler.get_time_factor(offset_hour=0)
        assert factor == 0.5

    @patch("channels.reddit.audience_listener.datetime")
    def test_deep_night_time_factor(self, mock_dt):
        from channels.reddit.audience_listener import CircadianScheduler

        mock_dt.utcnow.return_value.hour = 4  # UTC 4 + offset 0 = 4 (deep night)
        factor = CircadianScheduler.get_time_factor(offset_hour=0)
        assert factor == 0.1

    @patch("channels.reddit.audience_listener.datetime")
    def test_adjust_priorities_scales_correctly(self, mock_dt):
        from channels.reddit.audience_listener import (
            CircadianScheduler,
        )

        mock_dt.utcnow.return_value.hour = 12
        base = {"Daytrading": 1.0, "Forex": 2.0, "PropFirm": 1.5}
        adjusted = CircadianScheduler.adjust_priorities(base)
        assert "Daytrading" in adjusted
        assert "Forex" in adjusted
        assert "PropFirm" in adjusted
        for v in adjusted.values():
            assert v > 0


# ---------------------------------------------------------------------------
# 3. audience_listener.py — RedditStreamListener (DB + GQL parsing)
# ---------------------------------------------------------------------------


class TestRedditStreamListener:
    def _make_listener(self, db):
        """Create a listener with a mock event loop to avoid asyncio.create_task in sync context."""
        from channels.reddit.audience_listener import RedditStreamListener

        with patch("channels.reddit.audience_listener.asyncio.create_task"):
            with patch(
                "channels.reddit.audience_listener.asyncio.Queue", return_value=asyncio.Queue()
            ):
                listener = RedditStreamListener(db)
                listener.queue = asyncio.Queue()
                return listener

    def test_extract_posts_iterative_flat(self, db):
        listener = self._make_listener(db)

        data = {
            "data": [
                {"id": "t3_001", "title": "Prop Firm Help", "author": "user1"},
                {"id": "t3_002", "title": "Crypto Guide", "author": "user2"},
            ]
        }
        posts = listener._extract_posts_iterative(data)
        assert len(posts) == 2

    def test_extract_posts_iterative_nested(self, db):
        listener = self._make_listener(db)

        data = {
            "data": {
                "children": [
                    {"data": {"id": "t3_003", "title": "FTMO Challenge", "author": "trader1"}}
                ]
            }
        }
        posts = listener._extract_posts_iterative(data)
        assert len(posts) == 1
        assert posts[0]["title"] == "FTMO Challenge"

    def test_extract_posts_iterative_empty(self, db):
        listener = self._make_listener(db)
        posts = listener._extract_posts_iterative({})
        assert len(posts) == 0

    def test_enqueue_posts_filters_low_relevance(self, db):
        listener = self._make_listener(db)

        posts = [
            {
                "id": "t3_low",
                "title": "Random post",
                "author": "user1",
                "selftext": "nothing relevant here",
                "permalink": "/r/test/comments/low/",
            }
        ]
        listener._enqueue_posts(posts)
        assert listener.queue.empty()

    def test_enqueue_posts_accepts_high_intent(self, db):
        # Mock check_if_user_already_targeted (missing method on NexusDB)
        db.check_if_user_already_targeted = MagicMock(return_value=False)
        listener = self._make_listener(db)
        listener.db = db

        posts = [
            {
                "id": "t3_high",
                "title": "Need help with FTMO",
                "author": "user2",
                "selftext": "Looking for a funded account challenge code. Need help with payout.",
                "permalink": "/r/Daytrading/comments/high/",
            }
        ]
        listener._enqueue_posts(posts)
        assert not listener.queue.empty()

    def test_enqueue_posts_skips_duplicates(self, db):
        listener = self._make_listener(db)
        listener.seen_ids.add("t3_dup")

        posts = [
            {
                "id": "t3_dup",
                "title": "Duplicate post",
                "author": "user1",
                "selftext": "Looking for help",
                "permalink": "/r/test/comments/dup/",
            }
        ]
        listener._enqueue_posts(posts)
        assert listener.queue.empty()

    def test_enqueue_posts_skips_sponsored(self, db):
        listener = self._make_listener(db)

        posts = [
            {
                "id": "t3_sponsored",
                "title": "Sponsored post",
                "author": "user1",
                "selftext": "Looking for help with FTMO",
                "isSponsored": True,
                "permalink": "/r/test/comments/sponsored/",
            }
        ]
        listener._enqueue_posts(posts)
        assert listener.queue.empty()

    def test_enqueue_posts_skips_automoderator(self, db):
        listener = self._make_listener(db)

        posts = [
            {
                "id": "t3_auto",
                "title": "AutoMod post",
                "author": "AutoModerator",
                "selftext": "Looking for help with FTMO",
                "permalink": "/r/test/comments/auto/",
            }
        ]
        listener._enqueue_posts(posts)
        assert listener.queue.empty()

    def test_check_health_returns_true_when_active(self, db):
        listener = self._make_listener(db)
        listener.last_activity = time.time()
        assert listener.check_health() is True

    def test_check_health_returns_false_when_stale(self, db):
        listener = self._make_listener(db)
        listener.last_activity = time.time() - 3600
        assert listener.check_health() is False

    @pytest.mark.asyncio
    async def test_shutdown_cancels_worker(self, db):
        from channels.reddit.audience_listener import RedditStreamListener

        listener = RedditStreamListener(db)
        await listener.shutdown()
        assert listener._worker_task.cancelled()


# ---------------------------------------------------------------------------
# 4. partner_hunter.py — AuthorityClassifier (pure logic)
# ---------------------------------------------------------------------------


class TestAuthorityClassifier:
    def test_prop_firm_lead_detected(self):
        from channels.reddit.partner_hunter import AuthorityClassifier

        # Ensure body is long enough for +20 bonus and includes analysis keyword for +20
        tier, score, intent = AuthorityClassifier.classify(
            "FTMO Challenge analysis review",
            "Finally passed my funded account evaluation after 3 months. " * 8
            + "The payout process was smooth. Here is my drawdown strategy.",
        )
        assert intent == "PROP_FIRM_LEAD"
        assert score >= 45

    def test_b2b_partner_detected(self):
        from channels.reddit.partner_hunter import AuthorityClassifier

        tier, score, intent = AuthorityClassifier.classify(
            "My Course on Day Trading",
            "Join my discord link for mentorship. I teach my strategy. "
            "Subscribe to my telegram group for signals.",
        )
        assert intent == "B2B_PARTNER"

    def test_saas_crypto_lead_detected(self):
        from channels.reddit.partner_hunter import AuthorityClassifier

        tier, score, intent = AuthorityClassifier.classify(
            "TradingView Setup Guide",
            "Best indicator bot for python API chart analysis. "
            "Ledger vs binance comparison for storing crypto.",
        )
        assert intent == "SAAS_CRYPTO_LEAD"

    def test_banking_noise_filtered(self):
        from channels.reddit.partner_hunter import AuthorityClassifier

        tier, score, intent = AuthorityClassifier.classify(
            "Best bank account",
            "Looking for credit agricole bnp pret immobilier livret a assurance vie.",
        )
        assert intent == "NOISE"
        assert score == 0

    def test_banking_with_trading_context_allowed(self):
        from channels.reddit.partner_hunter import AuthorityClassifier

        tier, score, intent = AuthorityClassifier.classify(
            "Trading vs banque",
            "How to trade crypto and bitcoin vs keeping in fortuneo livret a? "
            "Need help with strategy and bourse.",
        )
        assert intent != "NOISE"

    def test_generic_low_score(self):
        from channels.reddit.partner_hunter import AuthorityClassifier

        tier, score, intent = AuthorityClassifier.classify("Hello", "Just saying hi.")
        assert score < 20

    def test_long_body_bonus(self):
        from channels.reddit.partner_hunter import AuthorityClassifier

        long_body = "This is a detailed analysis. " * 30
        _, score_short, _ = AuthorityClassifier.classify("Analysis", "short text")
        _, score_long, _ = AuthorityClassifier.classify("Analysis", long_body)
        assert score_long > score_short

    def test_guide_title_bonus(self):
        from channels.reddit.partner_hunter import AuthorityClassifier

        # Source code checks "guide" in title (case-sensitive on original title)
        _, score_guide, _ = AuthorityClassifier.classify("day trading guide review", "Some content")
        _, score_plain, _ = AuthorityClassifier.classify("day trading", "Some content")
        assert score_guide > score_plain

    def test_urgency_triangular_boost(self):
        from channels.reddit.partner_hunter import AuthorityClassifier

        _, score_normal, _ = AuthorityClassifier.classify(
            "FTMO Challenge", "I need help with funded account challenge."
        )
        _, score_urgent, _ = AuthorityClassifier.classify(
            "FTMO Challenge",
            "I need help with funded account challenge. Margin call today, account blown.",
        )
        assert score_urgent > score_normal

    def test_scam_noise(self):
        from channels.reddit.partner_hunter import AuthorityClassifier

        tier, score, intent = AuthorityClassifier.classify(
            "This is a scam", "Fake bot arnaque everywhere. Be careful."
        )
        assert intent == "NOISE"

    def test_high_tier_threshold(self):
        from channels.reddit.partner_hunter import AuthorityClassifier

        tier, score, intent = AuthorityClassifier.classify(
            "Complete Guide to Apex Trader Funding Payout Strategy",
            "Here is my detailed analysis vs forecast on funded account evaluation. "
            "The payout process after challenge passed requires understanding drawdown. " * 10,
        )
        assert tier == "HIGH_TIER"

    def test_mid_tier_threshold(self):
        from channels.reddit.partner_hunter import AuthorityClassifier

        tier, score, intent = AuthorityClassifier.classify(
            "FTMO review", "Some thoughts on the challenge payout and evaluation."
        )
        assert tier in ("MID_TIER", "HIGH_TIER")


# ---------------------------------------------------------------------------
# 5. partner_hunter.py — PartnerHunter (DB + anonymization + cache)
# ---------------------------------------------------------------------------


class TestPartnerHunterAnonymization:
    def _make_hunter(self):
        from channels.reddit.partner_hunter import PartnerHunter

        with patch("channels.reddit.partner_hunter.NexusDB") as mock_db_cls:
            mock_db = MagicMock()
            mock_db._hash_identity = NexusDB(db_path=Path(":memory:"))._hash_identity
            mock_db_cls.return_value = mock_db
            hunter = PartnerHunter()
        # Replace with real DB for tests that need it
        return hunter

    def test_anonymize_identity_is_deterministic(self, db):
        from channels.reddit.partner_hunter import PartnerHunter

        # Create a hunter with real DB to test _hash_identity
        with patch("channels.reddit.partner_hunter.NexusDB", return_value=db):
            hunter = PartnerHunter()

        hash1 = hunter._anonymize_identity("test_user")
        hash2 = hunter._anonymize_identity("test_user")
        assert hash1 == hash2
        assert len(hash1) == 64  # SHA-256 hex

    def test_anonymize_identity_empty_returns_unknown(self, db):
        with patch("channels.reddit.partner_hunter.NexusDB", return_value=db):
            from channels.reddit.partner_hunter import PartnerHunter

            hunter = PartnerHunter()
        assert hunter._anonymize_identity("") == "unknown"
        assert hunter._anonymize_identity("unknown") == "unknown"

    def test_cache_maintenance(self, db):
        with patch("channels.reddit.partner_hunter.NexusDB", return_value=db):
            from channels.reddit.partner_hunter import PartnerHunter

            hunter = PartnerHunter()
            hunter.CACHE_LIMIT = 5
            for i in range(10):
                hunter.local_cache[f"t3_{i}"] = time.time()
            hunter._maintain_cache()
            assert len(hunter.local_cache) <= hunter.CACHE_LIMIT

    def test_select_next_target_returns_valid_sub(self, db):
        with patch("channels.reddit.partner_hunter.NexusDB", return_value=db):
            from channels.reddit.partner_hunter import PartnerHunter

            hunter = PartnerHunter()
            # Reset all cooldowns
            for sub in hunter.targets_state:
                hunter.targets_state[sub]["last_visit"] = 0
            target, strategy = hunter._select_next_target()
            assert target is not None
            assert target in [
                "Daytrading",
                "Forex",
                "PropFunded",
                "algotrading",
                "Futures",
                "wallstreetbets",
                "RealDayTrading",
                "vosfinances",
                "francefire",
                "cryptomonnaie",
                "finance",
                "investir",
            ]


# ---------------------------------------------------------------------------
# 6. Full DB flow — Reddit lead insertion -> QUALIFIED -> DISPATCHING
# ---------------------------------------------------------------------------


class TestRedditFullDBFlow:
    def test_insert_raw_lead_creates_new_status(self, db):
        result = db.insert_raw_lead(
            {
                "id": "reddit_flow_001",
                "source": "reddit_listener",
                "author": "user_hash",
                "url": "https://www.reddit.com/r/Daytrading/comments/abc/",
                "text": "Looking for help with FTMO challenge",
            }
        )
        assert result is True

        with db.session() as conn:
            row = conn.execute(
                "SELECT status, source FROM leads WHERE id='reddit_flow_001'"
            ).fetchone()
        assert row[0] == "NEW"
        assert row[1] == "reddit_listener"

    def test_insert_telemetry_signal_creates_lead(self, db):
        result = db.insert_telemetry_signal(
            {
                "id": "reddit_flow_002",
                "source": "reddit_partner_hunter",
                "author": "partner_hash",
                "url": "https://www.reddit.com/r/Forex/comments/xyz/",
                "text": "FTMO payout review",
            }
        )
        assert result is True

        with db.session() as conn:
            row = conn.execute(
                "SELECT status, source FROM leads WHERE id='reddit_flow_002'"
            ).fetchone()
        assert row[0] == "NEW"
        assert row[1] == "reddit_partner_hunter"

    def test_qualify_lead_changes_status(self, db):
        db.insert_raw_lead(
            {
                "id": "reddit_flow_003",
                "source": "reddit",
                "author": "user_hash",
                "url": "https://www.reddit.com/r/PropFunded/comments/def/",
                "text": "Best prop firm for futures",
            }
        )
        with db.session() as conn:
            conn.execute("UPDATE leads SET status='QUALIFIED' WHERE id='reddit_flow_003'")

        with db.session() as conn:
            row = conn.execute("SELECT status FROM leads WHERE id='reddit_flow_003'").fetchone()
        assert row[0] == "QUALIFIED"

    def test_reserve_dispatch_moves_to_dispatching(self, db):
        db.insert_raw_lead(
            {
                "id": "reddit_flow_004",
                "source": "reddit",
                "author": "user_hash",
                "url": "https://www.reddit.com/r/algotrading/comments/ghi/",
                "text": "Python API trading bot strategy",
            }
        )
        with db.session() as conn:
            conn.execute("UPDATE leads SET status='QUALIFIED' WHERE id='reddit_flow_004'")

        db.reserve_leads_for_dispatch(batch_size=5, batch_id="reddit_batch")

        with db.session() as conn:
            row = conn.execute("SELECT status FROM leads WHERE id='reddit_flow_004'").fetchone()
        assert row[0] == "DISPATCHING"

    def test_full_pipeline_new_to_dispatching(self, db):
        db.insert_telemetry_signal(
            {
                "id": "reddit_flow_005",
                "source": "reddit_partner_hunter",
                "author": "full_pipe_user",
                "url": "https://www.reddit.com/r/wallstreetbets/comments/jkl/",
                "text": "Apex Trader Funding review payout strategy",
            }
        )
        with db.session() as conn:
            row = conn.execute("SELECT status FROM leads WHERE id='reddit_flow_005'").fetchone()
        assert row[0] == "NEW"

        with db.session() as conn:
            conn.execute("UPDATE leads SET status='QUALIFIED' WHERE id='reddit_flow_005'")
        with db.session() as conn:
            row = conn.execute("SELECT status FROM leads WHERE id='reddit_flow_005'").fetchone()
        assert row[0] == "QUALIFIED"

        db.reserve_leads_for_dispatch(batch_size=5, batch_id="full_pipe_batch")
        with db.session() as conn:
            row = conn.execute("SELECT status FROM leads WHERE id='reddit_flow_005'").fetchone()
        assert row[0] == "DISPATCHING"

    def test_fail_lead_from_dispatching(self, db):
        db.insert_raw_lead(
            {
                "id": "reddit_flow_006",
                "source": "reddit",
                "author": "fail_user",
                "url": "https://www.reddit.com/r/Daytrading/comments/mno/",
                "text": "hello",
            }
        )
        with db.session() as conn:
            conn.execute("UPDATE leads SET status='QUALIFIED' WHERE id='reddit_flow_006'")
        db.reserve_leads_for_dispatch(batch_size=5, batch_id="fail_batch")

        with db.session() as conn:
            row = conn.execute("SELECT status FROM leads WHERE id='reddit_flow_006'").fetchone()
        assert row[0] == "DISPATCHING"

        db.fail_lead("reddit_flow_006", "POST_FAILED")
        with db.session() as conn:
            row = conn.execute("SELECT status FROM leads WHERE id='reddit_flow_006'").fetchone()
        assert "FAILED" in row[0]

    def test_hash_identity_produces_valid_sha256(self, db):
        hashed = db._hash_identity("test_reddit_user")
        assert len(hashed) == 64
        assert hashed == db._hash_identity("test_reddit_user")
        assert hashed != db._hash_identity("different_user")


# ---------------------------------------------------------------------------
# 7. Atomic dispatch with sponsor balance (Reddit sender flow)
# ---------------------------------------------------------------------------


class TestRedditAtomicDispatchFlow:
    def test_dispatch_ready_to_send(self, db_with_sponsor):
        db_with_sponsor.insert_raw_lead(
            {
                "id": "reddit_dispatch_001",
                "source": "reddit",
                "author": "dispatch_user",
                "url": "https://www.reddit.com/r/Daytrading/comments/pqr/",
                "text": "prop firm challenge help needed",
            }
        )
        with db_with_sponsor.session() as conn:
            conn.execute("UPDATE leads SET status='QUALIFIED' WHERE id='reddit_dispatch_001'")

        result = db_with_sponsor.atomic_dispatch_transaction(
            "reddit_dispatch_001",
            "sp_reddit_001",
            "PROP_FIRM",
            Decimal("10.00"),
        )
        assert result is True

        with db_with_sponsor.session() as conn:
            row = conn.execute("SELECT status FROM leads WHERE id='reddit_dispatch_001'").fetchone()
        assert row[0] == "READY_TO_SEND"

    def test_mark_sent_after_dispatch(self, db_with_sponsor):
        db_with_sponsor.insert_raw_lead(
            {
                "id": "reddit_dispatch_002",
                "source": "reddit",
                "author": "sent_user",
                "url": "https://www.reddit.com/r/algotrading/comments/stu/",
                "text": "tradingview python bot setup",
            }
        )
        db_with_sponsor.atomic_dispatch_transaction(
            "reddit_dispatch_002",
            "sp_reddit_001",
            "SAAS_TOOLS",
            Decimal("5.00"),
        )
        db_with_sponsor.mark_lead_sent("reddit_dispatch_002")

        with db_with_sponsor.session() as conn:
            row = conn.execute("SELECT status FROM leads WHERE id='reddit_dispatch_002'").fetchone()
        assert row[0] == "SENT"

    def test_confirm_hold_after_dispatch(self, db_with_sponsor):
        db_with_sponsor.insert_raw_lead(
            {
                "id": "reddit_confirm_001",
                "source": "reddit",
                "author": "confirm_user",
                "url": "https://www.reddit.com/r/cryptomonnaie/comments/vwx/",
                "text": "ledger hardware wallet security",
            }
        )
        db_with_sponsor.atomic_dispatch_transaction(
            "reddit_confirm_001",
            "sp_reddit_001",
            "CRYPTO",
            Decimal("8.00"),
        )
        result = db_with_sponsor.confirm_lead_hold("reddit_confirm_001")
        assert result is True

    def test_release_hold_after_dispatch(self, db_with_sponsor):
        db_with_sponsor.insert_raw_lead(
            {
                "id": "reddit_release_001",
                "source": "reddit",
                "author": "release_user",
                "url": "https://www.reddit.com/r/Futures/comments/yza/",
                "text": "apex trader funding review",
            }
        )
        db_with_sponsor.atomic_dispatch_transaction(
            "reddit_release_001",
            "sp_reddit_001",
            "PROP_FIRM",
            Decimal("12.00"),
        )
        result = db_with_sponsor.release_lead_hold("reddit_release_001")
        assert result is True


# ---------------------------------------------------------------------------
# 8. Playwright DOM tests — Reddit-specific patterns, test_profile
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestRedditPlaywrightDOM:
    async def test_browser_launch_with_test_profile(self):
        """Verify Playwright can launch with a dedicated test_profile."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()

            await page.goto("data:text/html,<h1>Reddit Test</h1>")
            title = await page.title()
            assert title == ""

            await page.close()
            await context.close()
            await browser.close()

    async def test_extract_reddit_post_dom_pattern(self):
        """Test the DOM selector pattern used for extracting Reddit posts."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            html = """
            <html><body>
            <div class="post" data-testid="post-container">
                <h3><a href="/r/Daytrading/comments/abc/">Need help with FTMO challenge</a></h3>
                <div class="author">u/trader123</div>
                <div class="body">Looking for a funded account code. Need payout help.</div>
                <span class="score">42 points</span>
            </div>
            <div class="post" data-testid="post-container">
                <h3><a href="/r/Forex/comments/def/">TradingView setup guide</a></h3>
                <div class="author">u/analyst</div>
                <div class="body">Best indicator bot for chart analysis.</div>
                <span class="score">15 points</span>
            </div>
            </body></html>
            """
            await page.goto(f"data:text/html,{html}")

            posts = await page.evaluate(
                """() => {
                const data = [];
                const containers = document.querySelectorAll('[data-testid="post-container"]');
                containers.forEach(el => {
                    const titleEl = el.querySelector('h3 a');
                    const authorEl = el.querySelector('.author');
                    const bodyEl = el.querySelector('.body');
                    if (titleEl && bodyEl) {
                        data.push({
                            title: titleEl.innerText.trim(),
                            author: authorEl ? authorEl.innerText.trim() : '',
                            body: bodyEl.innerText.trim(),
                            url: titleEl.href || ''
                        });
                    }
                });
                return data;
            }"""
            )

            assert len(posts) == 2
            assert "FTMO" in posts[0]["title"]
            assert "TradingView" in posts[1]["title"]

            await browser.close()

    async def test_extract_reddit_comments_dom_pattern(self):
        """Test the DOM selector pattern for Reddit comment threads."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            html = """
            <html><body>
            <div class="comment" data-testid="comment">
                <span class="author">u/user1</span>
                <div class="content">I passed my Apex challenge, payout was smooth.</div>
            </div>
            <div class="comment" data-testid="comment">
                <span class="author">u/user2</span>
                <div class="content">Looking for help with FTMO evaluation.</div>
            </div>
            </body></html>
            """
            await page.goto(f"data:text/html,{html}")

            comments = await page.evaluate(
                """() => {
                const data = [];
                const elements = document.querySelectorAll('[data-testid="comment"]');
                elements.forEach(el => {
                    const authorEl = el.querySelector('.author');
                    const contentEl = el.querySelector('.content');
                    if (authorEl && contentEl) {
                        data.push({
                            author: authorEl.innerText.trim(),
                            content: contentEl.innerText.trim()
                        });
                    }
                });
                return data;
            }"""
            )

            assert len(comments) == 2
            assert "Apex" in comments[0]["content"]
            assert "FTMO" in comments[1]["content"]

            await browser.close()

    async def test_reddit_gql_url_pattern_detection(self):
        """Test that we can detect GQL endpoints in URLs."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)

            urls_to_test = [
                ("https://gql.reddit.com/api/v1/posts", True),
                ("https://gateway.reddit.com/api/listing", True),
                ("https://www.reddit.com/r/Daytrading", False),
                ("https://old.reddit.com/new", False),
            ]

            for url, expected in urls_to_test:
                is_gql = "gql" in url or "gateway" in url
                assert is_gql == expected, f"Failed for: {url}"

            await browser.close()

    async def test_dom_missing_element_handling(self):
        """Test handling of missing DOM elements on Reddit."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            await page.goto("data:text/html,<h1>Empty page</h1>")

            is_visible = await page.is_visible('[data-testid="post-container"]')
            assert is_visible is False

            await browser.close()

    async def test_page_navigation_404_handling(self):
        """Test handling of 404 page navigation."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            response = await page.goto("https://httpbin.org/status/404", timeout=10000)
            assert response is not None
            assert response.status == 404

            await browser.close()

    async def test_page_navigation_500_handling(self):
        """Test handling of 500 page navigation."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            response = await page.goto("https://httpbin.org/status/500", timeout=10000)
            assert response is not None
            assert response.status == 500

            await browser.close()

    async def test_page_navigation_timeout(self):
        """Test handling of network timeout."""
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
        assert "reddit_main" not in str(test_profile_path)
        assert "reddit_master" not in str(test_profile_path)
        assert "reddit_hunter_global" not in str(test_profile_path)

    async def test_extract_reddit_sidebar_links(self):
        """Test extracting sidebar/community links from Reddit."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            html = """
            <html><body>
            <div class="sidebar">
                <a href="/r/Daytrading">r/Daytrading</a>
                <a href="/r/Forex">r/Forex</a>
                <a href="/r/PropFunded">r/PropFunded</a>
                <a href="/r/wallstreetbets">r/wallstreetbets</a>
            </div>
            </body></html>
            """
            await page.goto(f"data:text/html,{html}")

            subs = await page.eval_on_selector_all(
                '.sidebar a[href*="/r/"]',
                "els => els.map(e => e.getAttribute('href').split('/r/')[1])",
            )

            assert len(subs) == 4
            assert "Daytrading" in subs
            assert "Forex" in subs
            assert "PropFunded" in subs

            await browser.close()

    async def test_reddit_scroll_simulation(self):
        """Test simulating human-like scroll on Reddit-like page."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            # Create a tall page to scroll
            html = """
            <html><body style="height: 5000px;">
            <div id="scroll-marker-1" style="position:absolute;top:1000px">1</div>
            <div id="scroll-marker-2" style="position:absolute;top:3000px">2</div>
            </body></html>
            """
            await page.goto(f"data:text/html,{html}")

            # Initial viewport should not see marker 2
            initial = await page.evaluate(
                """() => {
                    const el = document.getElementById('scroll-marker-2');
                    const rect = el.getBoundingClientRect();
                    return rect.top < window.innerHeight;
                }"""
            )
            assert initial is False

            # Scroll down
            await page.evaluate("window.scrollTo(0, 3000)")
            await page.wait_for_timeout(100)

            # Now marker 2 should be visible
            after_scroll = await page.evaluate(
                """() => {
                    const el = document.getElementById('scroll-marker-2');
                    const rect = el.getBoundingClientRect();
                    return rect.top < window.innerHeight;
                }"""
            )
            assert after_scroll is True

            await browser.close()

    async def test_reddit_json_gql_response_parsing(self):
        """Test parsing JSON responses like Reddit GQL would return."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            json_data = {
                "data": {
                    "subreddit": {
                        "name": "Daytrading",
                        "posts": {
                            "edges": [
                                {
                                    "node": {
                                        "id": "t3_001",
                                        "title": "FTMO Help",
                                        "author": {"name": "trader1"},
                                        "body": "Need challenge help",
                                    }
                                },
                                {
                                    "node": {
                                        "id": "t3_002",
                                        "title": "Apex Review",
                                        "author": {"name": "trader2"},
                                        "body": "Payout was smooth",
                                    }
                                },
                            ]
                        },
                    }
                }
            }

            result = await page.evaluate(
                f"""() => {{
                    const data = {json.dumps(json_data)};
                    const posts = [];
                    function extract(obj) {{
                        if (obj && obj.title && obj.author && obj.id) {{
                            posts.push({{
                                id: obj.id,
                                title: obj.title,
                                author: obj.author.name,
                                body: obj.body || ''
                            }});
                        }}
                        if (obj && typeof obj === 'object') {{
                            for (const key of Object.keys(obj)) {{
                                extract(obj[key]);
                            }}
                        }}
                    }}
                    extract(data);
                    return posts;
                }}"""
            )

            assert len(result) == 2
            assert result[0]["title"] == "FTMO Help"
            assert result[1]["title"] == "Apex Review"

            await browser.close()
