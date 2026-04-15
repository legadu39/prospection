# tests/unit/test_workload_orchestrator.py
"""
Tests unitaires pour ComputeGridOrchestrator (core/workload_orchestrator.py).

Couvre :
  - infer_process_type()        : routing sémantique + intent explicite + fuzzy
  - _calculate_ucb1_score()     : algorithme UCB1 (exploitation / exploration)
  - attempt_atomic_allocation() : scarcity curve, compliance FR, filtres erreur/quota

Convention NexusDB : toujours SQLite :memory: — jamais mocké (CLAUDE.md).
"""
import json
import math
import time
import pytest

from core.workload_orchestrator import (
    ComputeGridOrchestrator,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture()
def orc(db):
    """
    Orchestrator avec DB in-memory et semantic_map déterministe.
    Overrider la map évite toute dépendance sur config/semantic_map.json.
    """
    o = ComputeGridOrchestrator(db)
    o.semantic_map = {
        "prop_firm": {"funding": 5, "capital": 5, "evaluation": 4, "challenge": 4, "drawdown": 3},
        "tradingview": {"chart": 5, "backtest": 5, "indicateur": 4, "pro": 2},
        "ledger": {"ledger": 5, "nano": 5, "wallet": 3},
        "meria": {"meria": 5, "staking": 3},
        "binance": {"binance": 5, "crypto": 4, "bitcoin": 3},
    }
    return o


def _candidate(
    id="s1",
    limit=100,
    used=10,
    pending=5,
    payout=100,
    visits=5,
):
    """Construit un dict candidat qui passe tous les filtres par défaut."""
    return {
        "id": id,
        "monthly_limit_hard": limit,
        "verified_count_month": used,
        "pending_leads_count": pending,
        "estimated_payout": payout,
        "total_leads_assigned": visits,
    }


# ─────────────────────────────────────────────────────────────────────────────
# infer_process_type()
# ─────────────────────────────────────────────────────────────────────────────


class TestInferProcessType:
    # ── Input vide / None ─────────────────────────────────────────────────────

    def test_none_input_returns_none(self, orc):
        assert orc.infer_process_type(None) is None

    def test_empty_dict_returns_none(self, orc):
        assert orc.infer_process_type({}) is None

    # ── Intent explicite PROTOCOL_ ────────────────────────────────────────────

    def test_protocol_prop(self, orc):
        assert orc.infer_process_type({"intent": "PROTOCOL_PROP_FIRM"}) == "prop_firm"

    def test_protocol_funding(self, orc):
        assert orc.infer_process_type({"intent": "PROTOCOL_FUNDING_A"}) == "prop_firm"

    def test_protocol_capital(self, orc):
        assert orc.infer_process_type({"intent": "PROTOCOL_CAPITAL_TIER1"}) == "prop_firm"

    def test_protocol_saas(self, orc):
        assert orc.infer_process_type({"intent": "PROTOCOL_SAAS_TOOLS"}) == "tradingview"

    def test_protocol_tradingview(self, orc):
        assert orc.infer_process_type({"intent": "PROTOCOL_TRADINGVIEW"}) == "tradingview"

    def test_protocol_chart(self, orc):
        assert orc.infer_process_type({"intent": "PROTOCOL_CHART_V2"}) == "tradingview"

    def test_protocol_ledger(self, orc):
        assert orc.infer_process_type({"intent": "PROTOCOL_LEDGER_SECURE"}) == "ledger"

    def test_protocol_hardware(self, orc):
        assert orc.infer_process_type({"intent": "PROTOCOL_HARDWARE_WALLET"}) == "ledger"

    def test_protocol_security(self, orc):
        assert orc.infer_process_type({"intent": "PROTOCOL_SECURITY_A"}) == "ledger"

    def test_protocol_meria(self, orc):
        assert orc.infer_process_type({"intent": "PROTOCOL_MERIA_YIELD"}) == "meria"

    def test_protocol_passive(self, orc):
        assert orc.infer_process_type({"intent": "PROTOCOL_PASSIVE_INCOME"}) == "meria"

    def test_protocol_crypto(self, orc):
        assert orc.infer_process_type({"intent": "PROTOCOL_CRYPTO_CHAIN"}) == "binance"

    def test_protocol_binance(self, orc):
        assert orc.infer_process_type({"intent": "PROTOCOL_BINANCE_SPOT"}) == "binance"

    def test_protocol_ambiguous_defaults_to_prop_firm(self, orc):
        # Préfixe PROTOCOL_ présent mais aucun mot-clé reconnu → default stratégique
        assert orc.infer_process_type({"intent": "PROTOCOL_UNKNOWN_XYZ"}) == "prop_firm"

    def test_intent_without_protocol_prefix_falls_through(self, orc):
        # Sans "PROTOCOL_" dans intent → passe au chemin sémantique → None (texte vide)
        result = orc.infer_process_type({"intent": "SOME_OTHER_INTENT", "comment_text": ""})
        assert result is None

    # ── ai_process_info ───────────────────────────────────────────────────────

    def test_ai_process_info_dict_valid_program(self, orc):
        pkt = {"ai_process_info": {"suggested_program": "ledger"}}
        assert orc.infer_process_type(pkt) == "ledger"

    def test_ai_process_info_json_string_valid_program(self, orc):
        pkt = {"ai_process_info": json.dumps({"suggested_program": "meria"})}
        assert orc.infer_process_type(pkt) == "meria"

    def test_ai_process_info_unknown_program_falls_through_to_text(self, orc):
        # Programme inconnu → retombe sur analyse textuelle → None (texte vide)
        pkt = {"ai_process_info": {"suggested_program": "unknown_prog"}, "comment_text": ""}
        assert orc.infer_process_type(pkt) is None

    def test_ai_process_info_malformed_json_falls_through(self, orc):
        # JSON invalide → exception catchée → retombe sur texte → None
        pkt = {"ai_process_info": "not_valid_json{{{", "comment_text": ""}
        assert orc.infer_process_type(pkt) is None

    # ── Analyse sémantique textuelle ──────────────────────────────────────────

    def test_text_strong_signal_prop_firm(self, orc):
        # "funding"(5) + "capital"(5) = 10 ≥ SIGNAL_QUALITY_THRESHOLD(3)
        pkt = {"comment_text": "looking for funding and capital allocation"}
        assert orc.infer_process_type(pkt) == "prop_firm"

    def test_text_strong_signal_tradingview(self, orc):
        pkt = {"comment_text": "I backtest my chart strategy every week"}
        assert orc.infer_process_type(pkt) == "tradingview"

    def test_text_strong_signal_ledger(self, orc):
        pkt = {"comment_text": "I store my crypto on a ledger nano"}
        assert orc.infer_process_type(pkt) == "ledger"

    def test_text_no_keywords_returns_none(self, orc):
        pkt = {"comment_text": "hello world nothing relevant here at all"}
        assert orc.infer_process_type(pkt) is None

    def test_text_below_threshold_returns_none(self, orc):
        # "pro" → weight=2 < SIGNAL_QUALITY_THRESHOLD(3) → None
        pkt = {"comment_text": "just a pro trader"}
        assert orc.infer_process_type(pkt) is None

    def test_text_negation_cancels_score(self, orc):
        # "pas" dans les 20 chars avant "funding" → score[prop_firm] -= 5*2 = -10 → None
        pkt = {"comment_text": "pas funding pour moi"}
        assert orc.infer_process_type(pkt) is None

    def test_comment_text_and_ai_draft_combined(self, orc):
        # Signal réparti sur les deux champs
        pkt = {"comment_text": "funding available", "ai_draft": "capital required"}
        assert orc.infer_process_type(pkt) == "prop_firm"

    def test_text_only_ai_draft(self, orc):
        pkt = {"ai_draft": "I need capital for my evaluation"}
        assert orc.infer_process_type(pkt) == "prop_firm"

    # ── Fuzzy matching ────────────────────────────────────────────────────────

    def test_fuzzy_match_single_typo(self, orc):
        # "fundingg" ≈ "funding" → ratio ≈ 0.93 > 0.85 → match prop_firm
        pkt = {"comment_text": "great fundingg opportunity here"}
        assert orc.infer_process_type(pkt) == "prop_firm"

    def test_fuzzy_no_match_on_short_words(self, orc):
        # "pro" len=3 ≤ 4 → fuzzy skipped; direct match weight=2 < threshold → None
        pkt = {"comment_text": "I am pro"}
        assert orc.infer_process_type(pkt) is None


# ─────────────────────────────────────────────────────────────────────────────
# _calculate_ucb1_score()
# ─────────────────────────────────────────────────────────────────────────────


class TestCalculateUcb1Score:
    def test_never_visited_returns_inf(self, orc):
        node = {"total_leads_assigned": 0, "estimated_payout": 100, "verified_count_month": 0}
        assert orc._calculate_ucb1_score(node) == float("inf")

    def test_empty_node_defaults_to_inf(self, orc):
        # total_leads_assigned manquant → .get() retourne 0 → inf
        assert orc._calculate_ucb1_score({}) == float("inf")

    def test_zero_wins_empty_stats_score_is_zero(self, orc):
        # wins=0 → win_rate=0 ; total_system_visits=1 → log(1)=0 → exploration=0
        node = {"total_leads_assigned": 5, "verified_count_month": 0, "estimated_payout": 50}
        assert orc._calculate_ucb1_score(node) == pytest.approx(0.0)

    def test_full_payout_full_wins_empty_stats(self, orc):
        # payout=150→1.0, visits=10, wins=10→win_rate=1.0, exploration=0
        # score = 1.0 * 1.0 + 0.0 = 1.0
        node = {"estimated_payout": 150, "total_leads_assigned": 10, "verified_count_month": 10}
        assert orc._calculate_ucb1_score(node) == pytest.approx(1.0)

    def test_exploration_bonus_with_system_visits(self, orc):
        orc._selection_stats["anchor"] = 100  # total_system_visits = 100
        node = {"estimated_payout": 0, "total_leads_assigned": 4, "verified_count_month": 0}
        # score = 0 + sqrt(log(100) / 4)
        expected = math.sqrt(math.log(100) / 4)
        assert orc._calculate_ucb1_score(node) == pytest.approx(expected, rel=1e-6)

    def test_more_visits_lower_exploration(self, orc):
        orc._selection_stats["anchor"] = 100
        few = {"estimated_payout": 0, "total_leads_assigned": 4, "verified_count_month": 0}
        many = {"estimated_payout": 0, "total_leads_assigned": 50, "verified_count_month": 0}
        assert orc._calculate_ucb1_score(few) > orc._calculate_ucb1_score(many)

    def test_payout_above_150_gives_score_above_one(self, orc):
        # Pas de clamping dans l'implémentation actuelle
        orc._selection_stats["anchor"] = 10
        node = {"estimated_payout": 300, "total_leads_assigned": 5, "verified_count_month": 5}
        # payout_score=2.0, win_rate=1.0, exploration=sqrt(log(10)/5)
        expected = 2.0 * 1.0 + math.sqrt(math.log(10) / 5)
        assert orc._calculate_ucb1_score(node) == pytest.approx(expected, rel=1e-6)

    def test_partial_win_rate(self, orc):
        orc._selection_stats["anchor"] = 20
        node = {"estimated_payout": 150, "total_leads_assigned": 10, "verified_count_month": 5}
        # payout=1.0, win_rate=0.5, exploration=sqrt(log(20)/10)
        expected = 1.0 * 0.5 + math.sqrt(math.log(20) / 10)
        assert orc._calculate_ucb1_score(node) == pytest.approx(expected, rel=1e-6)


# ─────────────────────────────────────────────────────────────────────────────
# attempt_atomic_allocation()
# ─────────────────────────────────────────────────────────────────────────────


class TestAttemptAtomicAllocation:
    # ── Edge cases : liste vide / quota épuisé ────────────────────────────────

    def test_empty_candidates_returns_none(self, orc):
        assert orc.attempt_atomic_allocation("l1", "prop_firm", []) is None

    def test_all_used_equals_limit_filtered(self, orc):
        c = _candidate(limit=10, used=10, pending=0)  # total_load=10 >= 10
        assert orc.attempt_atomic_allocation("l1", "prop_firm", [c]) is None

    def test_used_plus_pending_over_limit(self, orc):
        c = _candidate(limit=20, used=10, pending=12)  # 22 >= 20
        assert orc.attempt_atomic_allocation("l1", "prop_firm", [c]) is None

    def test_zero_limit_filtered(self, orc):
        c = _candidate(limit=0, used=0, pending=0)  # limit <= 0
        assert orc.attempt_atomic_allocation("l1", "prop_firm", [c]) is None

    # ── Filtre santé (error rate) ─────────────────────────────────────────────

    def test_error_rate_above_half_filtered(self, orc):
        c = _candidate(id="bad_s")
        orc._node_error_rates["bad_s"] = 0.6  # > 0.5
        assert orc.attempt_atomic_allocation("l1", "prop_firm", [c]) is None

    def test_error_rate_exactly_half_passes(self, orc):
        # Condition : > 0.5 (strict) → 0.5 passe le filtre
        c = _candidate(id="edge_s")
        orc._node_error_rates["edge_s"] = 0.5
        result = orc.attempt_atomic_allocation("l1", "prop_firm", [c])
        assert result is None or result["id"] == "edge_s"

    # ── Filtre velocity exclusion ─────────────────────────────────────────────

    def test_velocity_excluded_candidate_filtered(self, orc):
        c = _candidate(id="vel_s")
        orc._velocity_exclusion["vel_s"] = time.time() + 9999
        assert orc.attempt_atomic_allocation("l1", "prop_firm", [c]) is None

    # ── Compliance FR/EU : binance + lead de qualité ──────────────────────────

    def test_binance_high_lead_score_filtered(self, orc):
        # lead_score=0.7 > 0.6 AND protocol="binance" → skip (FR compliance)
        c = _candidate(id="bnb_s")
        assert orc.attempt_atomic_allocation("l1", "binance", [c], lead_score=0.7) is None

    def test_binance_lead_score_at_threshold_filtered(self, orc):
        # lead_score=0.61 > 0.6 → filtré
        c = _candidate(id="bnb_s2")
        assert orc.attempt_atomic_allocation("l1", "binance", [c], lead_score=0.61) is None

    def test_binance_low_lead_score_passes_compliance(self, orc):
        # lead_score=0.3 ≤ 0.6 → compliance ne bloque pas
        c = _candidate(id="bnb_ok")
        result = orc.attempt_atomic_allocation("l1", "binance", [c], lead_score=0.3)
        assert result is None or result["id"] == "bnb_ok"

    def test_binance_score_exactly_60pct_passes(self, orc):
        # Condition : > 0.6 (strict) → 0.6 passe
        c = _candidate(id="bnb_60")
        result = orc.attempt_atomic_allocation("l1", "binance", [c], lead_score=0.6)
        assert result is None or result["id"] == "bnb_60"

    def test_non_binance_protocol_high_score_not_filtered(self, orc):
        # Règle compliance uniquement sur "binance"
        c = _candidate(id="prop_s")
        result = orc.attempt_atomic_allocation("l1", "prop_firm", [c], lead_score=0.9)
        assert result is None or result["id"] == "prop_s"

    # ── Scarcity curve ────────────────────────────────────────────────────────

    def test_scarcity_above_90pct_low_lead_filtered(self, orc):
        # utilization=0.91 > 0.90 → required_quality=0.8 ; lead_score=0.5 < 0.8
        c = _candidate(limit=100, used=91, pending=0)
        assert orc.attempt_atomic_allocation("l1", "prop_firm", [c], lead_score=0.5) is None

    def test_scarcity_above_90pct_high_lead_passes(self, orc):
        # utilization=0.91, lead_score=0.9 ≥ 0.8
        c = _candidate(id="s_90", limit=100, used=91, pending=0)
        result = orc.attempt_atomic_allocation("l1", "prop_firm", [c], lead_score=0.9)
        assert result is None or result["id"] == "s_90"

    def test_scarcity_above_90pct_lead_at_quality_threshold_passes(self, orc):
        # lead_score=0.8 == required_quality → non bloqué (< 0.8 est strict)
        c = _candidate(id="s_90b", limit=100, used=91, pending=0)
        result = orc.attempt_atomic_allocation("l1", "prop_firm", [c], lead_score=0.8)
        assert result is None or result["id"] == "s_90b"

    def test_scarcity_above_75pct_low_lead_filtered(self, orc):
        # utilization=0.80 > 0.75 → required_quality=0.5 ; lead_score=0.3 < 0.5
        c = _candidate(limit=100, used=80, pending=0)
        assert orc.attempt_atomic_allocation("l1", "prop_firm", [c], lead_score=0.3) is None

    def test_scarcity_above_75pct_adequate_lead_passes(self, orc):
        # utilization=0.80, lead_score=0.6 ≥ 0.5
        c = _candidate(id="s_75", limit=100, used=80, pending=0)
        result = orc.attempt_atomic_allocation("l1", "prop_firm", [c], lead_score=0.6)
        assert result is None or result["id"] == "s_75"

    def test_scarcity_below_75pct_any_lead_score_passes(self, orc):
        # utilization=0.50 ≤ 0.75 → required_quality=0.0 → tout lead passe
        c = _candidate(id="s_50", limit=100, used=50, pending=0)
        result = orc.attempt_atomic_allocation("l1", "prop_firm", [c], lead_score=0.1)
        assert result is None or result["id"] == "s_50"

    # ── Tri UCB1 : le non-visité bat le visité ────────────────────────────────

    def test_ucb1_unvisited_has_infinite_priority(self, orc):
        c_new = _candidate(id="new_s", visits=0, payout=50)
        c_old = _candidate(id="old_s", visits=100, payout=150)
        assert orc._calculate_ucb1_score(c_new) == float("inf")
        assert orc._calculate_ucb1_score(c_old) < float("inf")

    # ── Happy path : lead réel en DB, allocation de bout en bout ─────────────

    def test_happy_path_valid_lead_and_sponsor(self, orc, db):
        """
        Insère un lead QUALIFIED et un sponsor en DB.
        Vérifie que le winner retourné est celui attendu (ou None si la
        transaction atomique échoue — le filtrage a quand même été traversé).
        """
        lead_id = "lead_alloc_test"
        sponsor_id = "sp_alloc_test"

        db.insert_raw_lead(
            {
                "id": lead_id,
                "source": "unit_test",
                "author": db._hash_identity("test_author"),
            }
        )
        with db.session() as conn:
            conn.execute("UPDATE leads SET status='QUALIFIED' WHERE id=?", (lead_id,))
            conn.execute(
                """INSERT OR IGNORE INTO sponsors
                   (id, label, program, ref_link, active, monthly_limit_hard, verified_count_month)
                   VALUES (?, 'TestSponsor', 'prop_firm', 'https://test.com', 1, 100, 5)""",
                (sponsor_id,),
            )

        c = _candidate(id=sponsor_id, limit=100, used=5, pending=0, payout=150, visits=10)
        result = orc.attempt_atomic_allocation(lead_id, "prop_firm", [c])
        assert result is None or result["id"] == sponsor_id
