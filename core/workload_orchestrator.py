### prospection/core/workload_orchestrator.py
# core/workload_orchestrator.py - NEXUS YIELD OPTIMIZER V3.1 (STRATEGY 3.0 ALIGNED)
# -*- coding: utf-8 -*-

"""
NEXUS YIELD OPTIMIZER V3.0 - INTELLIGENT TRAFFIC DISPATCHER
-----------------------------------------------------------
TECHNICAL OBJECTIVE (AGENCY): 
This module acts as a High-Frequency Traffic Dispatcher. It allocates User Traffic (Leads)
to the highest-yielding Partner Programs based on Real-Time Market Bonuses AND Probabilistic Conversion Rates.

BUSINESS MECHANISM (YIELD MANAGEMENT):
1. Detects Active Bonuses (via Offer Hunter).
2. Prioritizes Partners based on UCB1 (Upper Confidence Bound) logic.
3. Uses Laplace Smoothing to handle "Cold Start" for new partners.
4. SCARCITY LOGIC: Increases qualification threshold as partner quotas fill up.
5. PID CONTROLLER: Auto-regulates batch size based on error rates.
6. INTELLIGENCE V2: Global Feedback Loop to steer Hunting Drones.

STRATEGY V3.1 UPDATE:
- Strict Geo-Routing (US -> Prop Firm / FR -> SaaS & PSAN)
- Banking Sector Deprecated (Kill Switch Active) - NO BANKING ROUTING.
- Enforced Compliance: Unregulated exchanges penalization in FR zone.

LEGAL NOTICE:
Operates strictly as a B2B Media Buying router. 
No charge to the end-user. Billing is handled via B2B CPA (Cost Per Acquisition) invoices.
"""

import time
import random
import logging
import json
import uuid
import sys
import threading
import copy
import math
import difflib
from datetime import datetime, timedelta
from decimal import Decimal, getcontext
from typing import List, Dict, Optional, Tuple, Any
from collections import defaultdict, Counter
from pathlib import Path

# Configuration Decimal (Financial Precision)
getcontext().prec = 6

# --- ROBUST IMPORTS ---
try:
    from core.database import NexusDB
    from core.settings import settings
    from core.time_manager import CircadianCycle 
except ImportError:
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from core.database import NexusDB
    from core.settings import settings
    from core.time_manager import CircadianCycle

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | [DISPATCHER] %(message)s"
)
logger = logging.getLogger("YieldOptimizer")

# --- CONSTANTS ---
DEFAULT_BATCH_SIZE = 50
MIN_BATCH_SIZE = 5
MAX_BATCH_SIZE = 200
SIGNAL_QUALITY_THRESHOLD = 3
GARBAGE_COLLECTION_TIMEOUT = 1800 

# --- PARTNER TIERS (REVENUE POTENTIAL V3.0) ---
# Aligned with Business Plan V3.0: Prop Firms > SaaS/Crypto > Fallback
PARTNER_YIELD_TIERS = {
    "TIER_1_HIGH_YIELD_PROP": {
        # STRATEGY V3: Global Prop Firms (Capital Allocation)
        "protocols": ["APEX", "TOPSTEP", "FTMO", "EVALUATION"], 
        "base_payout": 150.0, 
        "description": "High Ticket Global CPA / Prop Firms"
    },
    "TIER_2_INFRA_SECURITY": {
        # STRATEGY V3: Hardware Wallets & PSAN (Security Focus)
        "protocols": ["LEDGER", "MERIA", "TREZOR"],
        "base_payout": 60.0,
        "description": "Crypto Infrastructure & Security"
    },
    "TIER_3_SAAS_TOOLS": {
        # STRATEGY V3: Recurring Revenue & Tools (Low friction)
        "protocols": ["TRADINGVIEW", "REALVISION", "VPN"],
        "base_payout": 30.0,
        "description": "SaaS / Charting / Data Tools"
    },
    "TIER_4_LEGACY_CRYPTO": {
        # STRATEGY V3: Exchanges (Secondary priority)
        # NOTE: Penalized in EU if non-PSAN
        "protocols": ["BINANCE", "BYBIT", "OKX"],
        "base_payout": 40.0,
        "description": "Standard Crypto Exchanges"
    }
}

class ComputeGridOrchestrator:
    """
    Core Dispatcher for Nexus Agency.
    Manages traffic allocation to maximize CPA revenue (Yield).
    Includes PID Controller for flow regulation.
    """
    def __init__(self, db: NexusDB):
        self.db = db
        # Cache for Active Partners
        self._nodes_snapshot: Dict[str, List[Dict]] = {}
        # Cache for Partner Health (Dead links, etc.)
        self._node_error_rates: Dict[str, float] = defaultdict(float)
        # Cache for Partner Conversion Performance
        self._node_conversion_scores: Dict[str, float] = defaultdict(float)
        
        # Real-Time Bonus Cache (from Offer Hunter)
        self._active_bonuses: Dict[str, float] = {}
        
        # Stats for Round-Robin distribution & UCB1
        self._selection_stats: Dict[str, int] = defaultdict(int)
        
        # Temp exclusion list for velocity checks
        self._velocity_exclusion: Dict[str, float] = {}

        # PID Controller State
        self.current_batch_size = DEFAULT_BATCH_SIZE
        self.error_trend = 0.0

        self._cache_lock = threading.RLock()
        self.semantic_map = self._load_routing_rules()
        
        # Lightweight instance for Cycle Analysis
        self.cycle_analyzer = CircadianCycle()

    def _load_routing_rules(self) -> Dict[str, Dict[str, int]]:
        """
        Loads semantic routing rules for lead qualification.
        V3.0 UPDATE: Removed all Banking keywords. Added SaaS & Security.
        """
        config_path = settings.BASE_DIR / "config" / "semantic_map.json"
        
        # V3 DEFAULTS: Clean Sheet (No Banking Legacy)
        defaults = {
            "prop_firm": {
                "funding": 5, "capital": 5, "evaluation": 4, "challenge": 4,
                "drawdown": 3, "profit": 3, "trade": 2, "financed": 4
            },
            "tradingview": {
                "chart": 5, "graphique": 4, "analyse": 3, "indicateur": 4, 
                "backtest": 5, "données": 3, "pro": 2, "premium": 2
            },
            "ledger": {
                "ledger": 5, "nano": 5, "sécurité": 4, "hack": 4, "clef": 3,
                "stockage": 3, "cold": 4, "wallet": 3
            },
            "meria": {
                "meria": 5, "mining": 4, "just-mining": 4, "yield": 3, "staking": 3,
                "rente": 2, "passive": 2, "masternode": 3
            },
            "binance": {
                "binance": 5, "crypto": 4, "bitcoin": 3, "btc": 3, "eth": 2, 
                "trading": 2, "usdt": 3, "altcoin": 2
            }
        }

        if config_path.exists():
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, dict): return data
            except Exception: pass
        return defaults

    def _update_health_scores(self):
        """
        Monitors Link Quality to isolate dead or banned Partners.
        Includes PREDICTIVE VELOCITY CHECK to catch failing nodes early.
        """
        try:
            new_rates = {}
            active_ids = set()
            for prog, nodes in self._nodes_snapshot.items():
                for s in nodes:
                    active_ids.add(s['id'])
            
            # Cleanup expired exclusions
            now = time.time()
            self._velocity_exclusion = {k: v for k, v in self._velocity_exclusion.items() if v > now}

            for s_id in active_ids:
                current_rate = self.db.get_sponsor_failure_rate(s_id, hours=1)
                previous_rate = self._node_error_rates.get(s_id, 0.0)
                
                # Predictive Analysis: Velocity of Failure
                velocity = current_rate - previous_rate
                
                new_rates[s_id] = current_rate
                
                if current_rate > 0.5:
                    logger.warning(f"📉 PARTNER ALERT: Partner {s_id} links are failing (Error Rate: {current_rate:.1%}). Pausing traffic.")
                
                # Preventive Pause if errors surge suddenly (+15% in one cycle)
                elif velocity > 0.15 and current_rate > 0.2:
                    logger.warning(f"📉 VELOCITY ALERT: Partner {s_id} unstable (+{velocity:.1%} errors). Preventive pause for 10min.")
                    self._velocity_exclusion[s_id] = now + 600

            self._node_error_rates = new_rates

        except Exception as e:
            logger.error(f"⚠️ Health Score Update Error: {e}")

    def _refresh_bonus_data(self):
        """
        Retrieves the latest bonus amounts detected by the Offer Hunter (MarketMonitor).
        This drives the Yield Optimization.
        """
        try:
            # We assume Offer Hunter updates 'campaigns' table or config
            for proto in self.semantic_map.keys():
                info = self.db.get_campaign_info(proto)
                if info and info.get('amount'):
                    try:
                        self._active_bonuses[proto] = float(info['amount'])
                    except: pass
        except Exception as e:
            logger.warning(f"⚠️ Bonus Data Refresh Error: {e}")

    def _refresh_nodes_snapshot(self):
        """
        Refreshes the pool of available Partners (Affiliates).
        """
        new_snapshot = {}
        known_protocols = list(self.semantic_map.keys())
        self._refresh_bonus_data()
        
        for proto in known_protocols:
            try:
                nodes = self.db.get_program_sponsors_stats(proto)
                if nodes:
                    processed_nodes = []
                    for n in nodes:
                        try:
                            # Assign Dynamic Yield Score
                            base_payout = self._active_bonuses.get(proto, 0.0)
                            if base_payout == 0.0:
                                # Fallback to Tier System
                                upper_proto = proto.upper()
                                for tier, conf in PARTNER_YIELD_TIERS.items():
                                    if any(k in upper_proto for k in conf["protocols"]):
                                        base_payout = conf["base_payout"]
                                        break
                            
                            n['estimated_payout'] = base_payout
                            n['monthly_limit_hard'] = self.db.to_decimal(n.get('monthly_limit_hard', '20'))
                            processed_nodes.append(n)
                        except Exception: pass
                    
                    if processed_nodes:
                        new_snapshot[proto] = processed_nodes
            except Exception as e: pass
        
        with self._cache_lock:
            self._nodes_snapshot = new_snapshot
            
        self._update_health_scores()

    def infer_process_type(self, data_packet: Dict[str, Any]) -> Optional[str]:
        """
        Infers the best Affiliate Program for the lead using Fuzzy Matching.
        Robust to typos. V3 Updated to favor Prop Firms & SaaS.
        """
        if not data_packet: return None
        
        # 1. Check for Pre-calculated Intent (Explicit Intent)
        if data_packet.get('intent') and "PROTOCOL_" in data_packet['intent']:
            raw_intent = data_packet['intent']
            
            # V3 Mapping Updates - STRICT ROUTING
            if any(x in raw_intent for x in ["PROP", "FUNDING", "CAPITAL"]): return "prop_firm"
            if any(x in raw_intent for x in ["SAAS", "TOOL", "CHART", "TRADINGVIEW"]): return "tradingview"
            if any(x in raw_intent for x in ["SECURITY", "LEDGER", "HARDWARE"]): return "ledger"
            if any(x in raw_intent for x in ["PASSIVE", "YIELD", "MERIA"]): return "meria"
            if any(x in raw_intent for x in ["CRYPTO", "CHAIN", "BINANCE"]): return "binance"
            
            return "prop_firm" # Strategic Default if ambiguous but technical

        try:
            if data_packet.get('ai_process_info'):
                raw_info = data_packet['ai_process_info']
                meta = json.loads(raw_info) if isinstance(raw_info, str) else (raw_info or {})
                suggested = meta.get('suggested_program')
                if suggested and suggested in self.semantic_map:
                    return suggested
        except Exception: pass

        # Semantic Analysis with Fuzzy Matching
        text_corpus = (str(data_packet.get('comment_text') or '') + " " + str(data_packet.get('ai_draft') or '')).lower()
        scores = Counter()
        negations = ["non", "pas", "déteste", "arnaque", "nul", "jamais"]

        # Tokenize roughly for simple matching
        corpus_tokens = text_corpus.split()

        for process_name, keywords in self.semantic_map.items():
            for word, weight in keywords.items():
                # Direct match or Fuzzy match
                found = False
                if word in text_corpus:
                    found = True
                    # Check context for negations
                    idx = text_corpus.find(word)
                    context_before = text_corpus[max(0, idx-20):idx]
                    if any(neg in context_before for neg in negations):
                         scores[process_name] -= (weight * 2)
                         continue
                
                # If not found directly, try fuzzy on tokens (if word is long enough)
                if not found and len(word) > 4:
                    for token in corpus_tokens:
                        if len(token) > 4:
                            ratio = difflib.SequenceMatcher(None, word, token).ratio()
                            if ratio > 0.85: # 85% similarity
                                found = True
                                break
                
                if found:
                    scores[process_name] += weight

        if not scores: return None
        best_process, score = scores.most_common(1)[0]
        if score < SIGNAL_QUALITY_THRESHOLD: return None
        return best_process

    def _calculate_ucb1_score(self, node: Dict) -> float:
        """
        Implement UCB1 (Upper Confidence Bound) algorithm for Multi-Armed Bandit problem.
        Balances EXPLOITATION (proven yield) and EXPLORATION (testing new/unsure partners).
        """
        # Normalize Payout (Max assumed ~150 to keep ratio 0.0-1.0)
        payout_score = float(node.get('estimated_payout', 0)) / 150.0
        
        # Total visits for this specific node
        visits = float(node.get('total_leads_assigned', 0))
        
        # If never visited, infinite priority (Pure Exploration)
        if visits == 0:
            return float('inf')

        # Win Rate (Conversion Rate)
        wins = float(node.get('verified_count_month', 0))
        win_rate = wins / visits
        
        # Total system visits (logarithm base)
        total_system_visits = sum(self._selection_stats.values()) or 1
        
        # Exploration Factor (C=1.41 is standard, tuned to 1.0 for stability)
        # The less we visit, the higher this term grows
        exploration_factor = 1.0 * math.sqrt(math.log(total_system_visits) / visits)
        
        # Final Score: Expected Value + Exploration Bonus
        return (payout_score * win_rate) + exploration_factor

    def attempt_atomic_allocation(self, packet_id: str, protocol: str, candidates: List[Dict], lead_score: float = 0.5) -> Optional[Dict]:
        """
        YIELD OPTIMIZATION LOGIC (SCARCITY CURVE + UCB1):
        Selects the Partner providing the Maximum Expected Value while respecting quotas.
        """
        if not candidates: return None

        # 1. Filter Eligible Partners with Scarcity Logic
        eligible_candidates = []
        for n in candidates:
            # Check Critical Health
            if self._node_error_rates.get(n.get('id'), 0.0) > 0.5:
                continue

            # Check Predictive Health (Velocity Exclusion)
            if n.get('id') in self._velocity_exclusion:
                continue
            
            # Check Quotas & Scarcity
            limit = float(n.get('monthly_limit_hard', 20))
            if limit <= 0: continue 

            used = float(n.get('verified_count_month', 0))
            pending = float(n.get('pending_leads_count', 0))
            total_load = used + pending
            
            if total_load >= limit:
                continue
            
            # --- V3 COMPLIANCE FILTER (FRENCH ZONE) ---
            # Penalize Unregulated Exchanges (Binance) for High Quality Traffic
            # We assume high lead score = High Value Target (needs PSAN)
            if protocol == "binance" and lead_score > 0.6:
                # If we have a high quality lead, we prefer Meria/Ledger
                # We artificially skip this candidate to force fallback to Tier 2
                continue

            # --- SCARCITY LOGIC (YIELD MANAGEMENT) ---
            utilization_rate = total_load / limit
            required_quality = 0.0
            
            # If partner is >90% full, only give to Top Tier leads (>0.8 score)
            if utilization_rate > 0.90:
                required_quality = 0.8
            # If partner is >75% full, only give to Good leads (>0.5 score)
            elif utilization_rate > 0.75:
                required_quality = 0.5
            
            # Lead Score (0.0 to 1.0) vs Required Quality
            if lead_score < required_quality:
                # Skip this partner to save the slot for a better lead
                continue
                
            eligible_candidates.append(n)

        if not eligible_candidates:
            return None

        # 2. Sort by UCB1 Score (Intelligent Bandit)
        # Ties broken by 'verified_count_month' (favor underdogs if scores equal)
        eligible_candidates.sort(key=lambda x: (
            self._calculate_ucb1_score(x),
            -float(x.get('verified_count_month', 0))
        ), reverse=True)

        winner = eligible_candidates[0]
        
        # 3. Reserve Slot
        self._selection_stats[winner['id']] += 1
        
        # Execute Reservation in DB
        success = self.db.atomic_dispatch_transaction(
            lead_id=packet_id, 
            sponsor_id=winner['id'], 
            program=protocol, 
            estimated_cost=Decimal("0.00") # No cost for Agency model (User side)
        )
        
        if success:
            return winner
        else:
            # Retry with next candidate
            remaining = [c for c in eligible_candidates if c['id'] != winner['id']]
            return self.attempt_atomic_allocation(packet_id, protocol, remaining, lead_score)

    def _collect_garbage(self):
        """Release stuck leads."""
        try:
            timeout_limit = time.time() - GARBAGE_COLLECTION_TIMEOUT
            with self.db.session(immediate=True) as conn:
                query = """
                    SELECT id, assigned_sponsor_id, status 
                    FROM leads 
                    WHERE status IN ('DISPATCHING', 'READY_TO_SEND') 
                    AND updated_at < ?
                """
                cursor = conn.execute(query, (timeout_limit,))
                zombies = cursor.fetchall()
                
                if not zombies: return

                logger.info(f"🧹 GARBAGE COLLECTOR: {len(zombies)} leads released.")
                
                for row in zombies:
                    z_id = row['id'] if self.db.use_postgres else row[0]
                    conn.execute("""
                        UPDATE leads 
                        SET status='QUALIFIED', 
                            assigned_sponsor_id=NULL, 
                            updated_at=? 
                        WHERE id=?
                    """, (time.time(), z_id))
                    
        except Exception as e:
            logger.error(f"⚠️ Garbage Collector Error: {e}")

    def _adjust_batch_size(self, success_rate: float):
        """
        PID Controller to regulate Batch Size.
        Aim: Maximize throughput while minimizing errors.
        """
        if success_rate > 0.95:
            # Acceleration Phase (Integral Windup limited by MAX_BATCH_SIZE)
            self.current_batch_size = int(self.current_batch_size * 1.2)
        elif success_rate < 0.80:
            # Deceleration Phase (Proportional drop)
            self.current_batch_size = int(self.current_batch_size * 0.7)
        
        # Clamping
        self.current_batch_size = max(MIN_BATCH_SIZE, min(MAX_BATCH_SIZE, self.current_batch_size))
        
        logger.info(f"🎛️ PID: Success Rate {success_rate:.2f} -> Batch Size adjusted to {self.current_batch_size}")

    def _update_hunting_orders(self, protocol_stats: Counter):
        """
        INTELLIGENCE N°4 (FEEDBACK LOOP):
        Updates the Hunting Orders based on current cycle yield.
        Steers the Hunter bots towards successful protocols.
        """
        try:
            orders = {}
            for proto, count in protocol_stats.items():
                if count == 0: continue
                
                # Heuristic: If we have massive demand (dispatched), go AGGRESSIVE
                if count > 5:
                    orders[proto] = "AGGRESSIVE"
                elif count > 0:
                    orders[proto] = "NORMAL"
            
            # Write to shared file (Atomic-ish)
            if orders:
                temp_file = settings.HUNTING_ORDERS_PATH.with_suffix(".tmp")
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(orders, f)
                
                # Update main file
                try:
                    current_orders = {}
                    if settings.HUNTING_ORDERS_PATH.exists():
                        with open(settings.HUNTING_ORDERS_PATH, 'r') as f:
                            current_orders = json.load(f)
                    
                    # Merge
                    current_orders.update(orders)
                    
                    with open(settings.HUNTING_ORDERS_PATH, 'w', encoding='utf-8') as f:
                        json.dump(current_orders, f)
                except Exception: pass
                
        except Exception as e:
            logger.warning(f"⚠️ Failed to update Hunting Orders: {e}")

    def process_dispatch_cycle(self):
        """Main Yield Cycle with PID Flow Control."""
        start_time = time.time()
        self._collect_garbage()
        self._refresh_nodes_snapshot()
        
        # Batch Reserve using Dynamic Size
        batch_id = str(uuid.uuid4())[:8]
        batch = self.db.reserve_leads_for_dispatch(self.current_batch_size, batch_id)
        
        if not batch: return

        logger.info(f"🚚 Processing Agency Batch {batch_id}: {len(batch)} leads (Limit: {self.current_batch_size})...")
        stats = Counter()
        protocol_demand = Counter() # Tracks successful dispatches per protocol

        for data_packet in batch:
            try:
                # Extract Lead Quality Score (normalized 0.0 - 1.0) from Meta Analysis
                # SecureTelemetryStore puts 'initial_entropy_score' (0-100) in meta_analysis
                quality_score = 0.5 # Default
                try:
                    meta = json.loads(data_packet.get('meta_analysis', '{}'))
                    raw_score = meta.get('initial_entropy_score', 50)
                    quality_score = max(0.0, min(1.0, float(raw_score) / 100.0))
                except: pass

                # 1. Qualify Intent (Includes SaaS & Security checks now)
                workload_type = self.infer_process_type(data_packet)

                if not workload_type:
                    # Archive Low Intent
                    self.db.fail_lead(data_packet['id'], "ARCHIVED_LOW_INTENT")
                    stats['skipped'] += 1
                    continue

                # 2. Get Candidates
                candidates = self._nodes_snapshot.get(workload_type, [])
                
                # 3. Optimize Allocation (Yield)
                winner = self.attempt_atomic_allocation(data_packet['id'], workload_type, candidates, quality_score)

                if winner:
                    payout = winner.get('estimated_payout', 0)
                    logger.info(f"✅ DISPATCH {data_packet['id'][:8]} -> {winner['label']} (Est. Gain: {payout}€, Q-Score: {quality_score:.2f})")
                    stats['dispatched'] += 1
                    protocol_demand[workload_type] += 1
                else:
                    try:
                        with self.db.session() as conn:
                            conn.execute(
                                f"UPDATE leads SET status='WAITING_PARTNER', updated_at=? WHERE id=?", 
                                (time.time(), data_packet['id'])
                            )
                    except: pass
                    stats['waiting'] += 1
            
            except Exception as e:
                logger.error(f"❌ Dispatch Error lead {data_packet.get('id')}: {e}")
                self.db.fail_lead(data_packet.get('id'), "DISPATCH_ERROR")
                stats['errors'] += 1

        # --- PID FEEDBACK LOOP ---
        total_processed = sum(stats.values())
        if total_processed > 0:
            success_rate = (stats['dispatched'] + stats['skipped'] + stats['waiting']) / total_processed
            # Errors count negatively
            if stats['errors'] > 0:
                 success_rate = success_rate * (1.0 - (stats['errors'] / total_processed))
            
            self._adjust_batch_size(success_rate)
            
            # INTELLIGENCE N°4: Update Global State
            self._update_hunting_orders(protocol_demand)

        duration = time.time() - start_time
        if total_processed > 0:
            logger.info(f"🏁 Batch finished in {duration:.2f}s. Stats: {dict(stats)}")

def main():
    try:
        db = NexusDB()
        orchestrator = ComputeGridOrchestrator(db)
        orchestrator.process_dispatch_cycle()
        db.close_local_connection()
    except Exception as e:
        logger.critical(f"🔥 Orchestrator Crash: {e}")

if __name__ == "__main__":
    main()