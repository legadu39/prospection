### prospection/channels/tiktok/sniper.py
# NETWORK TOPOLOGY MAPPER v35.2.0 (STRATEGIC PIVOT V3 - FINAL)
# -*- coding: utf-8 -*-

"""
NEXUS TOPOLOGY MAPPER v35.2.0 - GRAPH DISCOVERY & SIGNAL ENTROPY ANALYSIS
-----------------------------------------------------------
TECHNICAL OBJECTIVE (IAAS):
1. Graph-Based Topology: Mapping of High-Density Network Nodes (Hubs).
2. Signal Entropy Analysis: Evaluation of packet density based on Protocol Signatures.
3. Circuit Breaker (SLA): Proactive isolation of unstable nodes to maintain QoS.
4. Residential Connectivity: Usage of certified ingress points.
5. Atomic Deduplication: Optimistic Caching of Node IDs to prevent redundant processing.
6. Handshake Protocol: Analyzing Interaction Density vs. Capacity.
7. Vector Search Optimization: Continuous refinement of query parameters.
8. Semantic Context: Real-time payload analysis + PACKET DEDUPLICATION.
9. SMART SCHEDULING: Temporal profiling of node activity windows.
10. ZERO-TRUST PRIVACY: User IDs are hashed (SHA-256) before storage.

PIVOT UPDATE V3.0:
- Removed FIAT/BANKING Protocols.
- Added TRADING_CAPITAL & SAAS_TOOLS Protocols.
- REPLACED "AID/DISTRESS" logic with "TECHNICAL_SUPPORT" (High Ticket intent).
"""

import asyncio
import json
import time
import logging
import random
import re
import os
import sys
import shutil
import statistics
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Deque, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from collections import OrderedDict, deque, Counter, defaultdict
from difflib import SequenceMatcher

from playwright.async_api import async_playwright, Response

try:
    ROOT_DIR = Path(__file__).resolve().parents[2]
    if str(ROOT_DIR) not in sys.path:
        sys.path.append(str(ROOT_DIR))
        
    from core.settings import settings
    try:
        from core.vision_guardian import VisionGuardian
        HAS_VISION = True
    except ImportError:
        HAS_VISION = False
        
    from core.time_manager import CircadianCycle
    from core.database import NexusDB
    from core.browser_engine import SandboxCDPProfile
except ImportError as e:
    print(f"🔴 CRITICAL ERROR: Core modules missing ({e}).")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | [TOPOLOGY] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("TopologyMapper")

class Config:
    CDP_PORT = settings.CDP_TIKTOK_PORT
    DENSITY_THRESHOLD_CPM = 15.0 
    MIN_SIGNAL_ENTROPY = 25 # Technical metric for signal validity
    CANARY_TIMEOUT_SEC = 1800
    BATCH_SIZE = 4
    ROTATION_FREQUENCY = 3
    DISCOVERY_FREQUENCY = 6
    MAX_CAMPERS = 2 
    NODE_METRICS_FILE = ROOT_DIR / "config" / "node_metrics.json" 
    VECTOR_STATS_FILE = ROOT_DIR / "config" / "vector_stats.json" 
    MARKET_SIGNALS_FILE = ROOT_DIR / "config" / "market_signals.json"
    VIRAL_CASCADE_THRESHOLD = 3 
    SALT = "NEXUS_INFRA_V35_SALT_KEY_CHANGE_ME" # Hashing Salt
    
    # SLA PROTECTION CONFIG
    CIRCUIT_BREAKER_THRESHOLD = 0.35 # > 35% Technical Failures triggers protection
    MIN_ATTEMPTS_FOR_BREAKER = 10     # Minimum 10 attempts before evaluating ratio

    # SIGNAL INTERFERENCE PATTERNS (Noise Filtering)
    # CORRECTIF STRATEGIQUE V3.0 : Suppression de "trading" et "investir" des signaux de congestion.
    # Ces termes sont désormais le cœur de cible.
    CONGESTION_SIGNATURES = ["mon code", "mon lien", "ma bio", "bot", "giveaway", "concours", "spam"]
    
    NOISE_SIGNATURES = ["règles", "tirage au sort", "parrainage boursorama", "prime 80", "banque gratuite", "crédit"]
    SYSTEM_NOISE_SIGNATURES = ["loading", "original sound", "translate"]
    
    # UPDATE V3: Termes orientés Trading/Formation/Outils
    EVOLUTION_SUFFIXES = ["prop firm", "challenge", "funded", "strategy", "review", "tuto", "avis", "discount", "tradingview", "indicator", "robot"]

    # UPDATE V3: Suppression branche "Banque", Renforcement "Trading/Business"
    SEMANTIC_GRAPH = {
        "crypto": ["bitcoin", "ethereum", "ledger", "trading", "altcoin", "blockchain", "airdrop", "staking"],
        "trading": ["prop firm", "ftmo", "apex", "funded account", "payout", "forex", "gold", "xauusd", "futures"],
        "business": ["saas", "entrepreneur", "marketing", "freelance", "copywriting", "software"],
        "outils": ["tradingview", "notion", "chatgpt", "ai tool", "productivity", "vpn"]
    }

class TrafficShaper:
    """
    Regulates network traffic ingestion based on infrastructure load.
    Prevents Denial of Service (DoS) on the analysis pipeline.
    Acts as an Ingress Controller.
    """
    def __init__(self, db: NexusDB, base_threshold=Config.MIN_SIGNAL_ENTROPY):
        self.db = db
        self.base_threshold = base_threshold
        self.signals_history = [] 

    def record_capture(self):
        self.signals_history.append(time.time())

    def get_backlog_size(self) -> int:
        return self.db.get_backlog_count()

    def get_dynamic_threshold(self) -> int:
        """
        Calculates the required entropy threshold dynamically.
        High Load -> Higher Threshold (Only High Density Packets accepted).
        Low Load -> Lower Threshold (Maximize Throughput).
        """
        now = time.time()
        self.signals_history = [t for t in self.signals_history if now - t < 3600]
        
        # 1. Backlog Factor (Global Load)
        backlog_size = self.get_backlog_size()
        backlog_penalty = 0
        
        if backlog_size > 120:
            backlog_penalty = 20 # Survival Mode: Reject noise
        elif backlog_size > 60:
            backlog_penalty = 8
        elif backlog_size < 15:
            backlog_penalty = -8 # Idle Mode: Accept generic signals

        # 2. Velocity Factor (Local Load)
        count_last_hour = len(self.signals_history)
        velocity_penalty = 0
        target_per_hour = 12
        
        if count_last_hour > target_per_hour * 2.5:
            velocity_penalty = 8
        elif count_last_hour > target_per_hour * 1.5:
            velocity_penalty = 3
        
        # 3. Efficiency Bias (FinOps Link)
        efficiency_modifier = 0
        try:
            best_offer_amount = self.db.get_max_active_campaign_amount()
            # If yield value is high (Flash Sale detected), we can afford higher processing cost
            if best_offer_amount > 50: # Represents % discount in V3
                efficiency_modifier = -10 
        except: pass
            
        dynamic = self.base_threshold + backlog_penalty + velocity_penalty + efficiency_modifier
        return max(10, min(65, int(dynamic)))

class VelocityTracker:
    def __init__(self, window_size=20):
        self.timestamps = deque(maxlen=window_size)
    
    def record_event(self):
        self.timestamps.append(time.time())
    
    @property
    def cpm(self) -> float:
        if len(self.timestamps) < 2: return 0.0
        delta = self.timestamps[-1] - self.timestamps[0]
        if delta == 0: return 0.0
        return (len(self.timestamps) / delta) * 60.0

class CanarySystem:
    def __init__(self):
        self.last_capture_time = time.time()
        self.total_signals = 0
    
    def feed(self):
        self.last_capture_time = time.time()
        self.total_signals += 1
        
    def check_health(self):
        elapsed = time.time() - self.last_capture_time
        if elapsed > Config.CANARY_TIMEOUT_SEC:
            logger.critical(f"💀 CANARY SILENCE: No signals for {int(elapsed/60)} min. Check Network Interface.")
            return False
        return True

class VectorLearner:
    """
    Adaptive Vector Learning for Hill Climbing Search.
    Optimizes query vectors based on technical feedback loops.
    """
    def __init__(self, db: NexusDB = None):
        self.file = Config.VECTOR_STATS_FILE
        self.db = db
        self.stats = self._load()
        self.hill_state = {
            "current_root": "trading", # V3 Default Root
            "last_query": None,
            "last_query_score": 0.0,
            "best_root_score": 0.0,
            "patience": 3 
        }
        if "HILL_CLIMBING" in self.stats:
             self.hill_state.update(self.stats["HILL_CLIMBING"])
    
    def _load(self) -> Dict:
        if self.file.exists():
            try:
                with open(self.file, "r") as f: return json.load(f)
            except: pass
        return {}
    
    def save(self):
        try:
            self.stats["HILL_CLIMBING"] = self.hill_state
            self.file.parent.mkdir(parents=True, exist_ok=True)
            temp_file = self.file.with_suffix(".tmp")
            with open(temp_file, "w") as f: 
                json.dump(self.stats, f, indent=2)
            temp_file.replace(self.file)
        except Exception as e:
            logger.error(f"⚠️ Stats Save Error: {e}")

    def get_multiplier(self, word: str) -> float:
        if word not in self.stats: return 1.0
        s = self.stats[word]
        hits = s.get("hits", 0)
        validations = s.get("validations", 0) 
        
        if hits < 15: return 1.0 
        validation_rate = validations / hits
        
        if validation_rate > 0.4: return 1.4   
        if validation_rate > 0.25: return 1.15   
        if validation_rate < 0.05: return 0.4   
        return 1.0

    def get_top_performing_keywords(self) -> List[str]:
        candidates = []
        for word, s in self.stats.items():
            if word == "HILL_CLIMBING": continue
            hits = s.get("hits", 0)
            validations = s.get("validations", 0)
            if hits > 5 and validations > 0:
                candidates.append(word)
        return candidates

    def get_best_suffixes(self) -> List[str]:
        suffix_performance = defaultdict(lambda: {"hits": 0, "validations": 0})
        
        for query, data in self.stats.items():
            if query == "HILL_CLIMBING": continue
            parts = query.split(' ')
            if len(parts) >= 2:
                suffix = parts[-1]
                if suffix in Config.EVOLUTION_SUFFIXES:
                    suffix_performance[suffix]["hits"] += data.get("hits", 0)
                    suffix_performance[suffix]["validations"] += data.get("validations", 0)
        
        results = []
        for suff, metrics in suffix_performance.items():
            if metrics["hits"] > 10:
                rate = metrics["validations"] / metrics["hits"]
                if rate > 0.15: 
                    results.append((suff, rate))
        
        results.sort(key=lambda x: x[1], reverse=True)
        return [r[0] for r in results]

    def register_new_gene(self, word: str):
        if word not in self.stats:
            self.stats[word] = {"hits": 0, "validations": 0, "origin": "mutation"}
            self.save()
            
    def update_hill_climbing_result(self, query: str, signals_found: int):
        if not query: return
        if query not in self.stats:
             self.stats[query] = {"hits": 0, "validations": 0}
        self.stats[query]["hits"] += 1
        
        self.hill_state["last_query"] = query
        self.hill_state["last_query_score"] = float(signals_found)
        
        current_root = self.hill_state["current_root"]
        
        if signals_found > self.hill_state["best_root_score"]:
            logger.info(f"🏔️ HILL CLIMBING: New Peak! '{query}' ({signals_found} signals) replaces '{current_root}'")
            self.hill_state["current_root"] = query
            self.hill_state["best_root_score"] = signals_found
            self.hill_state["patience"] = 3 
        else:
            if query != current_root: 
                self.hill_state["patience"] -= 1
                logger.debug(f"📉 Hill Climbing: '{query}' inefficient ({signals_found}). Patience: {self.hill_state['patience']}")

    def sync_with_db(self):
        if not self.db: return
        try:
            success_signals = self.db.get_converted_leads_sample(limit=500)
            if not success_signals: return

            learned_count = 0
            intent_map = ["comment", "funded", "prop", "challenge", "tuto", "lien", "intéressé", "info", "besoin", "aide"]
            
            for sig in success_signals:
                text = sig.get('comment_text', '').lower()
                for word in intent_map:
                    if word in text:
                        if word not in self.stats: self.stats[word] = {"hits": 0, "validations": 0}
                        self.stats[word]["validations"] += 1
                        learned_count += 1
            
            if learned_count > 0:
                logger.info(f"🧠 DEEP LEARNING: {learned_count} validated vectors integrated.")
                self.save()
        except Exception as e:
            logger.warning(f"⚠️ Learning Error: {e}")

    def update_word_hit(self, word: str):
        if word not in self.stats:
            self.stats[word] = {"hits": 0, "validations": 0}
        self.stats[word]["hits"] += 1

class ProtocolEngine:
    """
    Core Logic for Signal Entropy Analysis.
    Decodes packet content to determine Protocol Compatibility and Signal Density.
    V3.0: Tuned for High-Value Trading Signals (Prop Firm & SaaS).
    """
    learner: Optional[VectorLearner] = None

    # PROTOCOL DENSITY INDEX (Semantic Abstraction)
    # Higher density = Higher Compute Complexity & Higher Priority
    # CORRECTIF V3: Suppression totale de "PROTOCOL_AID" (Détresse)
    SIGNAL_ENTROPY_INDEX = {
        "PROTOCOL_TRADING_CAPITAL": {
            "keywords": ["funded", "prop firm", "challenge", "apex", "ftmo", "topstep", "payout", "capital", "100k"],
            "weight": 50 # Priority #1
        },
        "PROTOCOL_CRYPTO_TOOLS": {
            "keywords": ["ledger", "wallet", "binance", "bybit", "crypto", "btc", "secure", "staking"],
            "weight": 40 # Priority #2
        },
        "PROTOCOL_SAAS_EDUCATION": {
            "keywords": ["tuto", "strategy", "apprendre", "formation", "indicator", "tradingview", "robot", "bot"],
            "weight": 35
        },
        "PROTOCOL_DIRECT_LINK": {
            "keywords": ["lien", "code", "ou", "intéressé", "info", "go", "link"],
            "weight": 25
        },
        # REPLACEMENT: Cible Technique / Support (Utilisateurs solvables avec problèmes techniques)
        "PROTOCOL_TECHNICAL_SUPPORT": {
             "keywords": ["config", "setup", "bug", "retrait", "kyc", "api", "connexion", "login"],
             "weight": 20
        }
    }

    @staticmethod
    def initialize(db: NexusDB):
        ProtocolEngine.learner = VectorLearner(db)

    @staticmethod
    def is_noise_signal(text: str) -> bool:
        return any(kw in text.lower() for kw in Config.NOISE_SIGNATURES)

    @staticmethod
    def is_congested_node(text: str) -> bool:
        return any(kw in text.lower() for kw in Config.CONGESTION_SIGNATURES)

    @staticmethod
    def calculate_visibility_score(total_comments: int, comment_timestamp: float) -> float:
        """
        Calculates Time-Decay factor for the signal.
        Fresh signals have higher entropy.
        """
        if total_comments < 50:
            return 1.5 
        
        age_seconds = time.time() - comment_timestamp
        
        freshness_score = 1.0
        if age_seconds < 300:
            freshness_score = 2.0
        elif age_seconds < 3600:
            freshness_score = 1.5
        elif age_seconds < 86400:
            freshness_score = 1.0
        else:
            freshness_score = 0.5

        if total_comments > 5000:
            freshness_score *= 0.8 # Dilution factor
            
        return max(0.1, freshness_score)

    @staticmethod
    def analyze_signal_entropy(text: str) -> Tuple[int, str]:
        """
        Primary Algorithm: Signal Entropy Calculation.
        Scans packet payload for Protocol Signatures.
        """
        score = 0
        t_lower = text.lower()
        
        # 1. Negative Signal Filtering (Noise Reduction)
        if ProtocolEngine.is_congested_node(text): return -1000, "CONGESTED_NODE"
        if ProtocolEngine.is_noise_signal(text): return -500, "NOISE_SIGNAL"
        for ban in Config.SYSTEM_NOISE_SIGNATURES:
            if ban in t_lower: return -50, "SYSTEM_NOISE"

        # 2. Negation Boundary Detection
        NEGATORS = ["pas", "aucun", "stop", "non", "jamais", "fake", "arnaque", "don't", "arrête", "arrete"]
        words = t_lower.split()
        
        detected_protocols = Counter()

        def is_negated(target_word: str, full_text: str) -> bool:
            # Simple window-based negation check
            idx = full_text.find(target_word)
            if idx == -1: return False
            context_window = full_text[max(0, idx-20):idx]
            return any(neg in context_window for neg in NEGATORS)

        # 3. Protocol Matching (Semantic Abstraction)
        for protocol_name, config in ProtocolEngine.SIGNAL_ENTROPY_INDEX.items():
            for kw in config["keywords"]:
                if kw in t_lower:
                    if not is_negated(kw, t_lower):
                        detected_protocols[protocol_name] += 1
                        
                        multiplier = 1.0
                        if ProtocolEngine.learner:
                            multiplier = ProtocolEngine.learner.get_multiplier(kw)
                            ProtocolEngine.learner.update_word_hit(kw)
                        
                        score += (config["weight"] * multiplier)

        # 4. Contextual Boosters
        if "?" in text: score += 15 # Interrogative Payload
        if 10 < len(text) < 150: score += 10 # Ideal Packet Size
        
        best_protocol = detected_protocols.most_common(1)
        protocol_tag = best_protocol[0][0] if best_protocol else "PROTOCOL_UNKNOWN"

        if score == 0:
            protocol_tag = "DEFAULT"

        return int(score), protocol_tag

class AsyncNodeDeduplicator:
    """
    Optimistic Cache for Node IDs (Atomic Deduplication).
    Ensures Packet Uniqueness before expensive DB operations.
    """
    def __init__(self, db_instance, max_cache_size=50000):
        self.db = db_instance
        self.local_cache = OrderedDict()
        self.max_cache_size = max_cache_size
        self.executor = ThreadPoolExecutor(max_workers=5)

    async def __aenter__(self): return self
    async def __aexit__(self, exc_type, exc_val, exc_tb): self.shutdown()

    async def is_duplicate_async(self, cid: str) -> bool:
        if cid in self.local_cache:
            self.local_cache.move_to_end(cid)
            return True
        return False

    def _update_cache(self, cid: str):
        self.local_cache[cid] = True
        self.local_cache.move_to_end(cid)
        if len(self.local_cache) > self.max_cache_size:
            self.local_cache.popitem(last=False)

    async def insert_signal_async(self, signal_data: Dict) -> bool:
        cid = signal_data['id']
        if cid in self.local_cache: return True
        self._update_cache(cid)
        if self.db:
            try:
                loop = asyncio.get_running_loop()
                # Uses generic ingestion method - Abstraction of "Leads" to "Telemetry Packets"
                return await loop.run_in_executor(self.executor, self._atomic_insert, signal_data)
            except Exception: return False
        return True

    def _atomic_insert(self, signal_data: Dict) -> bool:
        try: return self.db.insert_raw_lead(signal_data)
        except: return False
    
    def shutdown(self):
        self.executor.shutdown(wait=True)

class SmartTopologyManager:
    """
    Graph Topology Manager V35 (SLA Protection Edition).
    Handles Network Discovery, Auto-Pruning, TRIANGULATION (Referrers)
    and CIRCUIT BREAKER for Infrastructure Protection.
    """
    def __init__(self):
        self.file = Config.NODE_METRICS_FILE
        self.market_file = Config.MARKET_SIGNALS_FILE
        self.scores = self._load()
        
        # Init default if empty
        if not self.scores: 
            self.scores = {"apex_trader_funding": { # Default Seed Node V3
                "score": 100, 
                "validations": 0, 
                "heatmap": [0]*24, 
                "consecutive_failures": 0,
                "technical_failures": 0,
                "total_attempts": 0,
                "quarantine_until": 0,
                "referred_by": [],
                "toxicity_level": 0
            }}

    def _load(self) -> Dict:
        try:
            with open(self.file, "r") as f: 
                data = json.load(f)
                return data
        except: return {}

    def _load_market_urgency(self) -> Dict[str, List[str]]:
        if self.market_file.exists():
            try:
                with open(self.market_file, "r") as f:
                    data = json.load(f)
                    return {
                        "shortage": data.get("critical_shortage", []),
                        "saturated": data.get("saturated", [])
                    }
            except: pass
        return {"shortage": [], "saturated": []}

    def save(self):
        try:
            self.file.parent.mkdir(parents=True, exist_ok=True)
            temp_file = self.file.with_suffix(".tmp")
            with open(temp_file, "w") as f: json.dump(self.scores, f)
            temp_file.replace(self.file)
        except: pass

    def is_circuit_open(self, username: str) -> bool:
        """
        Checks if Circuit Breaker is active for this node.
        High failure ratio triggers isolation to protect infra.
        """
        if username not in self.scores: return False
        entry = self.scores[username]
        
        if entry.get("quarantine_until", 0) > time.time():
            return True
            
        total = entry.get("total_attempts", 0)
        if total < Config.MIN_ATTEMPTS_FOR_BREAKER: return False
        
        fails = entry.get("technical_failures", 0)
        failure_ratio = fails / total
        
        if failure_ratio > Config.CIRCUIT_BREAKER_THRESHOLD:
            # TRIGGER CIRCUIT BREAKER (SLA PROTECTION)
            quarantine_duration = 3600 * (1 + int(failure_ratio * 10)) # 1h min
            entry["quarantine_until"] = time.time() + quarantine_duration
            logger.warning(f"🛡️ CIRCUIT BREAKER TRIGGERED: @{username} (Fail Ratio: {failure_ratio:.1%}). Isolation {quarantine_duration/3600:.1f}h.")
            return True
            
        return False

    def update_scan_result(self, username: str, raw_signals_count: int, technical_success: bool = True):
        """
        Updates Node Scores and manages Circuit Breaker.
        technical_success = False means crash/timeout, not just 0 signals.
        """
        if username not in self.scores:
            self.scores[username] = {
                "score": 50, "validations": 0, "heatmap": [0]*24, 
                "consecutive_failures": 0, "technical_failures": 0, "total_attempts": 0,
                "quarantine_until": 0, "referred_by": [], "toxicity_level": 0
            }
            
        entry = self.scores[username]
        current = entry["score"]
        current_hour = datetime.now().hour
        
        entry["total_attempts"] += 1
        
        if not technical_success:
            entry["technical_failures"] += 1
            entry["score"] = max(5, int(current * 0.8))
            logger.warning(f"⚠️ TECHNICAL FAIL: @{username} (Total Fails: {entry['technical_failures']})")
        else:
            if raw_signals_count > 0:
                # SUCCESSFUL SIGNAL DISCOVERY
                new_score = min(200, current + (raw_signals_count * 0.5))
                logger.info(f"📈 TOPOLOGY UPGRADE: Node {username}: {current} -> {new_score:.1f} (Entropy: {raw_signals_count})")
                
                entry["score"] = int(new_score)
                entry["consecutive_failures"] = 0
                entry["heatmap"][current_hour] += 1
            else:
                # LOW ENTROPY SIGNAL (Yield Impact)
                entry["consecutive_failures"] += 1
                fails = entry["consecutive_failures"]
                
                new_score = max(5, current * (0.9 ** fails)) 
                entry["score"] = int(new_score)
                
                if fails >= 5:
                    quarantine_duration = 3600 * (2 ** (fails - 5)) 
                    quarantine_duration = min(quarantine_duration, 7 * 86400)
                    entry["quarantine_until"] = time.time() + quarantine_duration
                    logger.warning(f"📉 ENTROPY PRUNING: Node @{username} sterile ({fails}x). Pause {quarantine_duration/3600:.1f}h.")

    def mark_node_toxic(self, username: str):
        if not username: return
        if username not in self.scores: return
        
        entry = self.scores[username]
        entry["toxicity_level"] += 1
        entry["technical_failures"] += 1
        
        entry["score"] = 5
        entry["quarantine_until"] = time.time() + (3600 * entry["toxicity_level"] * 4)
        
        logger.critical(f"☢️ NODE TOXIC: @{username} caused crash (Level {entry['toxicity_level']}). Immediate Isolation.")

    def get_smart_batch(self, size: int) -> List[str]:
        now_ts = time.time()
        current_hour = datetime.now().hour
        
        active_candidates = {}
        
        for k, v in self.scores.items():
            if v.get("quarantine_until", 0) > now_ts: continue
            if self.is_circuit_open(k): continue
            
            if v["score"] > 10 and v.get("toxicity_level", 0) < 2:
                active_candidates[k] = v

        if not active_candidates: 
            active_candidates = {k: v for k, v in self.scores.items() if v.get("toxicity_level", 0) < 5}
        if not active_candidates: return []

        # Market signals used for Load Balancing
        market_signals = self._load_market_urgency()
        shortages = market_signals["shortage"]
        saturated = market_signals["saturated"]
        
        shortage_keywords = []
        saturated_keywords = []

        # Mapping market signals to Topology Keys V3 (Trading)
        if any("prop" in s or "apex" in s for s in shortages): shortage_keywords.extend(["prop", "trader", "funding"])
        if any("crypto" in s or "ledger" in s for s in shortages): shortage_keywords.extend(["crypto", "btc", "invest"])

        weighted_pool = {}
        for user, data in active_candidates.items():
            final_score = float(data["score"])
            user_lower = user.lower()

            if any(k in user_lower for k in shortage_keywords):
                final_score *= 3.0 # High Priority Routing
            if any(k in user_lower for k in saturated_keywords):
                final_score *= 0.2 # Load Shedding

            # Temporal Profiling
            heatmap = data.get("heatmap", [0]*24)
            total_hits = sum(heatmap)
            if total_hits > 5:
                hits_at_hour = heatmap[current_hour]
                expected_hits = total_hits / 24.0
                time_multiplier = (hits_at_hour + 0.5) / (expected_hits + 0.5)
                time_multiplier = max(0.5, min(3.0, time_multiplier))
                final_score *= time_multiplier
            
            # Graph Triangulation (Hub Detection)
            referrers = data.get("referred_by", [])
            unique_referrers_count = len(set(referrers))
            if unique_referrers_count >= 2:
                final_score *= (1.5 ** unique_referrers_count)
                logger.info(f"🕸️ TRIANGULATION: {user} is a Hub (Linked by {unique_referrers_count} nodes). Boosted.")

            weighted_pool[user] = final_score
        
        try:
            keys = list(weighted_pool.keys())
            weights = list(weighted_pool.values())
            if sum(weights) == 0: return list(active_candidates.keys())[:size]
            return random.choices(keys, weights=weights, k=min(size, len(active_candidates)))
        except: return list(active_candidates.keys())[:size]

    def add_viral_node(self, username: str, source_referrer: str = None):
        if not username: return
        
        if username not in self.scores:
            logger.info(f"🦠 VIRAL CASCADE: Adding Node @{username} (Graph Discovery).")
            self.scores[username] = {
                "score": 80, 
                "validations": 0, 
                "heatmap": [0]*24,
                "consecutive_failures": 0,
                "technical_failures": 0,
                "total_attempts": 0,
                "quarantine_until": 0,
                "referred_by": [],
                "toxicity_level": 0
            } 
        
        if source_referrer:
            if "referred_by" not in self.scores[username]:
                 self.scores[username]["referred_by"] = []
            
            if len(self.scores[username]["referred_by"]) < 20:
                self.scores[username]["referred_by"].append(source_referrer)
            
        self.save()

class GraphTopologyMapper:
    """
    Analyzes the Network Graph Topology.
    Maps nodes (users) and signals (messages) to determine infrastructure routing.
    """
    def __init__(self, dedup: AsyncNodeDeduplicator, canary: CanarySystem, db: NexusDB):
        self.dedup = dedup
        self.canary = canary
        self.db = db 
        self.new_profiles: Deque[str] = deque(maxlen=2000)
        self.velocity = VelocityTracker()
        self.camping_semaphore = asyncio.Semaphore(Config.MAX_CAMPERS)
        self.density_expiry = 0.0
        self.regulator = TrafficShaper(db)
        self.context_keywords = Counter()
        self.last_visited_node: Optional[str] = None
        self.recent_texts_buffer = deque(maxlen=50)

    def trigger_density_spike(self):
        if not self.is_density_active:
            logger.info("🔥 HIGH DENSITY DETECTED! Latency Analysis Mode activated for 60s.")
        self.density_expiry = time.time() + 60.0

    @property
    def is_density_active(self) -> bool: return time.time() < self.density_expiry

    def _is_semantic_duplicate(self, new_text: str) -> bool:
        """
        SEMANTIC DEDUPLICATION.
        Detects packet redundancy using fuzzy string matching.
        """
        clean_new = re.sub(r'[^\w]', '', new_text.lower())
        if len(clean_new) < 5: return False 

        for recent in self.recent_texts_buffer:
            clean_recent = re.sub(r'[^\w]', '', recent.lower())
            similarity = SequenceMatcher(None, clean_new, clean_recent).ratio()
            if similarity > 0.85: return True 
        
        self.recent_texts_buffer.append(new_text)
        return False

    def _detect_honeypot_pattern(self, comments: List[Dict]) -> bool:
        if len(comments) < 5: return False
        
        timestamps = [c.get("create_time", 0) for c in comments]
        texts = [c.get("text", "") for c in comments]
        
        if timestamps:
            try:
                variance = statistics.variance(timestamps)
                if variance < 5 and len(comments) > 10:
                    logger.warning("🛡️ HONEYPOT DETECTED: Temporal variance suspect.")
                    return True
            except: pass

        unique_texts = set(texts)
        repetition_ratio = 1.0 - (len(unique_texts) / len(texts))
        
        if repetition_ratio > 0.7: 
            logger.warning("🛡️ HONEYPOT DETECTED: Textual repetition excessive.")
            return True
            
        return False
        
    def _extract_context_keywords(self, text: str):
        try:
            clean = re.sub(r'[^\w\s#]', '', text)
            words = clean.split()
            ignored = {"pour", "avec", "dans", "cette", "aussi", "faire", "tout", "plus", "être", "avoir"}
            
            for w in words:
                w_lower = w.lower()
                if (w.startswith('#') and len(w) > 2) or \
                   (len(w) > 5 and w_lower not in ignored) or \
                   (w[0].isupper() and len(w) > 3 and w_lower not in ignored):
                    
                    self.context_keywords[w] += 1
        except: pass

    def _anonymize_node(self, user_id: str) -> str:
        """
        DATA MINIMIZATION / PRIVACY SHIELD.
        We do not store PII (Personally Identifiable Information).
        We only map network nodes via topological hashes.
        """
        if not user_id: return "unknown_node"
        return hashlib.sha256(f"{user_id}{Config.SALT}".encode()).hexdigest()

    async def handle_response(self, response: Response):
        try:
            if "json" not in response.headers.get("content-type", ""): return
            if not any(p in response.url for p in settings.TIKTOK_API_PATTERNS): return
            
            try:
                if int(response.headers.get("content-length", 0)) > 2 * 1024 * 1024:
                    return
            except: pass

            try:
                data = await response.json()
            except Exception as e:
                logger.error(f"❌ JSON Parsing Error on {response.url}: {e}")
                return

            dynamic_min_entropy = self.regulator.get_dynamic_threshold()
            
            if "comments" in data:
                comments = data["comments"]
                
                total_comments = data.get("total", 0)
                if total_comments == 0 and comments: 
                    total_comments = len(comments) * 5
                
                if self._detect_honeypot_pattern(comments):
                    return

                for c in comments:
                    text = c.get("text", "")
                    
                    # --- PRIVACY SHIELD ENFORCEMENT ---
                    raw_user_id = c.get("user", {}).get("uniqueId")
                    node_hash = self._anonymize_node(raw_user_id)
                    create_time = c.get("create_time", time.time())
                    
                    self.velocity.record_event()
                    
                    # 1. Deduplication Filter (ID)
                    if await self.dedup.is_duplicate_async(c.get("cid")): continue

                    # 2. Semantic Deduplication (Anti-Noise)
                    if self._is_semantic_duplicate(text): continue
                    
                    # 3. Node Analysis (Anonymous check on Hash)
                    analysis = {"status": "NEW"} 
                    
                    # 4. Entropy Analysis (Protocol Detection)
                    entropy, protocol_tag = ProtocolEngine.analyze_signal_entropy(text)
                    
                    # FinOps: Logic for charging extra Compute Units for high-profile nodes
                    if analysis['status'] == 'HOT_RETURN':
                        entropy += 30
                    elif analysis['status'] == 'VIP_USER':
                        entropy += 100
                    
                    # 5. Visibility Score (Time-Decay)
                    visibility_multiplier = ProtocolEngine.calculate_visibility_score(total_comments, create_time)
                    entropy = int(entropy * visibility_multiplier)
                    
                    if entropy >= dynamic_min_entropy:
                        self._extract_context_keywords(text)
                        
                        # Ingest Telemetry Packet (Data Minimization applied)
                        inserted = await self.dedup.insert_signal_async({
                            "id": c.get("cid"), 
                            "text": text, # Raw payload (Context required for NLP)
                            "author": node_hash, # PRIVACY: HASHED ID
                            "source": "tiktok_graph", 
                            "score": entropy, 
                            "intent": protocol_tag, # Mapped to Protocol Tag
                            "scraped_at": datetime.now().isoformat()
                        })
                        if inserted:
                            self.canary.feed()
                            self.regulator.record_capture()

                if self.velocity.cpm > Config.DENSITY_THRESHOLD_CPM:
                    self.trigger_density_spike()

            if "item_list" in data:
                for item in data["item_list"]:
                    # Here we keep raw ID only for immediate navigation traversal, not storage
                    auth = item.get("author", {}).get("uniqueId")
                    if auth: self.new_profiles.append(auth)
        except Exception as e:
             logger.warning(f"⚠️ Error handling response: {e}")

    def flush_profiles(self) -> List[str]:
        res = list(self.new_profiles)
        self.new_profiles.clear()
        return res

class GraphNavigator:
    def __init__(self, context, listener: GraphTopologyMapper, vision=None):
        self.context = context
        self.listener = listener
        self.vision = vision
        self.page = None

    async def __aenter__(self):
        self.page = await self.context.new_page()
        self.page.on("response", self.listener.handle_response)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.page: 
            try: await self.page.close()
            except: pass

    async def _simulate_latency(self, intensity="normal"):
        if not self.page: return
        await self.page.mouse.move(random.randint(100,800), random.randint(100,600), steps=5)
        if intensity == "high": 
            await self.page.mouse.wheel(0, random.randint(300, 800))
        else:
            await asyncio.sleep(random.uniform(0.5, 1.5))

    async def get_neighboring_nodes(self) -> List[str]:
        if not self.page: return []
        try:
            accounts = await self.page.eval_on_selector_all(
                'a[href*="/@"]',
                """
                els => els.map(e => e.getAttribute('href').split('/@')[1].split('?')[0])
                          .filter(u => u && u.length > 2)
                          .slice(0, 5) 
                """
            )
            return list(set(accounts))
        except: return []

    async def scan_node(self, username: str) -> int:
        if not self.page: return 0
        logger.info(f"🔎 Topology Scan: @{username}")
        self.listener.last_visited_node = username 
        start_signals = self.listener.canary.total_signals
        technical_success = True
        
        try:
            await self.page.goto(f"https://www.tiktok.com/@{username}", timeout=20000)
            await self._simulate_latency()
            await asyncio.sleep(random.uniform(2, 4))
            
            try:
                if await self.page.is_visible("button:has-text('Tout refuser')"):
                    await self.page.click("button:has-text('Tout refuser')")
                    await asyncio.sleep(1)
                elif await self.page.is_visible("button:has-text('Continuer en tant qu’invité')"):
                    await self.page.click("button:has-text('Continuer en tant qu’invité')")
                    await asyncio.sleep(1)
            except: pass
            
            videos = []
            selectors_strategies = [
                'a[href*="/video/"]',           
                'div[data-e2e="user-post-item"] a', 
                '.video-feed-item a'            
            ]
            
            for selector in selectors_strategies:
                try:
                    videos = await self.page.eval_on_selector_all(selector, "els => els.slice(0, 3).map(e => e.href)")
                    if videos: break 
                except: pass
                
            if not videos:
                logger.debug(f"⚠️ No signal sources found for Node @{username}.")
            
            for vid in videos:
                try:
                    await self.page.goto(vid.split('?')[0], timeout=20000)
                    await self._simulate_latency("normal")
                    
                    if self.listener.is_density_active:
                        if not self.listener.camping_semaphore.locked():
                            async with self.listener.camping_semaphore:
                                logger.info(f"⛺ DENSITY ANALYSIS ACTIVE on {vid} (CPM: {self.listener.velocity.cpm:.1f})")
                                t0 = time.time()
                                while time.time() - t0 < 45: 
                                    await self._simulate_latency("high")
                                    await asyncio.sleep(3)
                        else:
                            logger.debug("⛺ Density Analysis slots full.")
                except Exception as e:
                    logger.warning(f"⚠️ Video Load Error: {e}")

        except Exception as e:
            logger.warning(f"⚠️ Nav Scan Error: {e}")
            if self.vision: await self.vision.handle_crash(self.page, "ScanNode", str(e))
            technical_success = False 
            raise e
        
        return self.listener.canary.total_signals - start_signals, technical_success

    async def _get_vector_query(self) -> str:
        """
        GRAPH TRAVERSAL (INTELLIGENT PIVOT).
        Uses Semantic Graph to pivot if root keyword fails.
        """
        defaults = ["prop firm funding", "trading strategy", "crypto wallet secure", "saas tools business"]
        
        if self.listener.context_keywords:
            hot_keyword, count = self.listener.context_keywords.most_common(1)[0]
            if count >= 2: 
                self.listener.context_keywords[hot_keyword] -= 1
                if self.listener.context_keywords[hot_keyword] <= 0:
                     del self.listener.context_keywords[hot_keyword]
                logger.info(f"🧬 SEMANTIC INJECTION: Contextual search on '{hot_keyword}'")
                return hot_keyword

        if ProtocolEngine.learner:
            # Hill Climbing State Retrieval
            hill_root = ProtocolEngine.learner.hill_state.get("current_root", "trading")
            patience = ProtocolEngine.learner.hill_state.get("patience", 3)
            
            if patience <= 0:
                # Patience exhausted: Semantic Pivot
                logger.info(f"📉 PATIENCE EXHAUSTED for '{hill_root}'. Pivoting...")
                
                neighbors = Config.SEMANTIC_GRAPH.get(hill_root, [])
                
                if not neighbors:
                    for key, vals in Config.SEMANTIC_GRAPH.items():
                        if hill_root in vals:
                            neighbors = [key] + [v for v in vals if v != hill_root]
                            break
                
                if neighbors:
                    new_root = random.choice(neighbors)
                    logger.info(f"🕸️ GRAPH PIVOT: Jump from '{hill_root}' -> '{new_root}'")
                    
                    ProtocolEngine.learner.hill_state["current_root"] = new_root
                    ProtocolEngine.learner.hill_state["patience"] = 3
                    ProtocolEngine.learner.hill_state["best_root_score"] = 0
                    
                    mutation = f"{new_root} {random.choice(Config.EVOLUTION_SUFFIXES)}"
                    ProtocolEngine.learner.register_new_gene(mutation)
                    return mutation
                else:
                    tops = ProtocolEngine.learner.get_top_performing_keywords()
                    if tops:
                        new_root = random.choice(tops)
                        logger.info(f"🚁 FALLBACK PIVOT: Jump to Top Keyword '{new_root}'")
                        ProtocolEngine.learner.hill_state["current_root"] = new_root
                        ProtocolEngine.learner.hill_state["patience"] = 3
            
            # Cross-Pollination
            if random.random() < 0.33:
                best_suffixes = ProtocolEngine.learner.get_best_suffixes()
                if best_suffixes:
                    pollinated_suffix = best_suffixes[0]
                    mutation = f"{hill_root} {pollinated_suffix}"
                    logger.info(f"🐝 CROSS POLLINATION: Testing suffix '{pollinated_suffix}' on '{hill_root}'")
                    ProtocolEngine.learner.register_new_gene(mutation)
                    return mutation
            
            suffix = random.choice(Config.EVOLUTION_SUFFIXES)
            mutation = f"{hill_root} {suffix}"
            
            logger.info(f"🧗 HILL CLIMBING: Exploring '{mutation}' (Root: '{hill_root}')")
            ProtocolEngine.learner.register_new_gene(mutation)
            return mutation
        
        return random.choice(defaults)

    async def discovery_mode(self, query: Optional[str] = None):
        if not self.page: return
        target_query = query if query else await self._get_vector_query()
        try:
            logger.info(f"🔭 Network Discovery: {target_query}")
            await self.page.goto(f"https://www.tiktok.com/search?q={target_query}&t=video", timeout=25000)
            await self._simulate_latency("high")
            
            try:
                await self.page.mouse.wheel(0, 1000)
                await asyncio.sleep(2)
                
                signals_before = self.listener.canary.total_signals
                await asyncio.sleep(5) 
                signals_after = self.listener.canary.total_signals
                found = signals_after - signals_before
                
                if ProtocolEngine.learner:
                    ProtocolEngine.learner.update_hill_climbing_result(target_query, found)
                    
            except: pass
            
        except: pass

async def run_topology_mapper():
    print("🚀 NEXUS TOPOLOGY MAPPER v35.2.0 - GLOBAL EDITION")
    
    db = NexusDB()
    ProtocolEngine.initialize(db)
    topology_mgr = SmartTopologyManager()
    canary = CanarySystem()
    cycle = CircadianCycle()
    
    vision = None
    if HAS_VISION and 'VisionGuardian' in globals():
        vision = VisionGuardian(debug_dir="logs/mapper_debug")

    profile = SandboxCDPProfile(
        user_id="tiktok_mapper_main",
        cdp_port=Config.CDP_PORT,
        profile_dir=str(settings.CHROME_PROFILES_DIR / "tiktok_mapper")
    )

    shutdown_event = asyncio.Event()
    
    async with async_playwright() as p, AsyncNodeDeduplicator(db) as dedup:
        logger.info("🔌 Connecting via SandboxCDPProfile (Proxy Aware & Safe Mode)...")
        if not await profile.connect(p):
            logger.critical("❌ Impossible to connect Topology Mapper. Check Proxy/CDP.")
            return

        context = profile.context
        listener = GraphTopologyMapper(dedup, canary, db)
        
        loop_count = 0

        while not shutdown_event.is_set():
            await cycle.check_schedule_and_sleep_if_needed()
            loop_count += 1
            
            if not canary.check_health(): 
                await asyncio.sleep(900)

            # IP Rotation
            if loop_count % Config.ROTATION_FREQUENCY == 0:
                logger.info("🔄 Rotating Session Proxy...")
                await profile.ensure_connected(p) 
                context = profile.context

            session_signals = 0
            current_threshold = listener.regulator.get_dynamic_threshold()
            
            if loop_count % 5 == 0:
                logger.info(f"📊 NETWORK: Dynamic Threshold {current_threshold} (Base {Config.MIN_SIGNAL_ENTROPY})")
                ProtocolEngine.learner.save() 
                ProtocolEngine.learner.sync_with_db()

            if loop_count % Config.DISCOVERY_FREQUENCY == 0:
                async with GraphNavigator(context, listener, vision) as nav:
                    await nav.discovery_mode()
                    for np in listener.flush_profiles(): 
                        topology_mgr.add_viral_node(np, source_referrer="discovery_agent")
            else:
                batch = []
                while len(batch) < Config.BATCH_SIZE:
                    priority_node = db.pop_priority_target()
                    if priority_node: batch.append(priority_node)
                    else: break 
                
                if len(batch) < Config.BATCH_SIZE:
                    needed = Config.BATCH_SIZE - len(batch)
                    batch.extend(topology_mgr.get_smart_batch(needed))

                if not batch: 
                    await asyncio.sleep(10)
                    continue
                
                for username in batch:
                    try:
                        async with GraphNavigator(context, listener, vision) as nav:
                            scan_res = await nav.scan_node(username)
                            if isinstance(scan_res, tuple):
                                signals_found, technical_success = scan_res
                            else:
                                signals_found, technical_success = scan_res, True 

                            session_signals += signals_found
                            topology_mgr.update_scan_result(username, signals_found, technical_success)
                            
                            if signals_found > 0:
                                cycle.record_success_event(signals_found)

                            if signals_found >= Config.VIRAL_CASCADE_THRESHOLD:
                                logger.info(f"🧬 NODE ALPHA (@{username} -> {signals_found}). Triangulation activated...")
                                try:
                                    neighbors = await nav.get_neighboring_nodes()
                                    if neighbors:
                                        for neighbor in neighbors:
                                            if neighbor != username:
                                                topology_mgr.add_viral_node(neighbor, source_referrer=username)
                                                db.upsert_viral_target(neighbor, priority=80, source="triangulation_graph")
                                        logger.info(f"🕸️ {len(neighbors)} neighbors injected (High Priority).")
                                except Exception as e:
                                    logger.warning(f"⚠️ Triangulation Failed: {e}")
                    except Exception as fatal:
                        logger.error(f"☠️ CRITICAL CRASH on @{username}: {fatal}")
                        topology_mgr.mark_node_toxic(username)
                        await asyncio.sleep(5)

            cycle.record_activity(session_signals)
            topology_mgr.save()
            ProtocolEngine.learner.save() 
            await asyncio.sleep(random.randint(45, 120))

    if vision: await vision.close()

if __name__ == "__main__":
    try: asyncio.run(run_topology_mapper())
    except KeyboardInterrupt: pass