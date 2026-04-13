### prospection/channels/tiktok/partner_sniper.py
# channels/tiktok/topology_mapper.py - NETWORK TOPOLOGY MAPPER V37.3 (STRATEGIC ALIGNMENT)
# -*- coding: utf-8 -*-

"""
TIKTOK TOPOLOGY MAPPER V37.3 - STRATEGY V3.0
--------------------------------------------
OBJECTIVE: Maps the network topology to identify 'Super Editors' and High-Throughput Nodes.
PRIVACY: Zero-Knowledge Storage. All node identifiers are hashed (SHA-256).
INTELLIGENCE: 
  - Ecosystem Scoring (Cross-Platform detection)
  - Dynamic Signal Weighting (Feedback Loop)
  - VIP Identification & Automatic Escalation (Workflow Automation)
  - Viral Triangulation (Graph Exploration)

V3.0 UPDATE:
  - Global Strategy: Keywords shifted to English (US/Global focus).
  - High Yield Targets: Prop Firms, SaaS, Hardware Security.
"""

import asyncio
import logging
import random
import json
import sys
import unicodedata
import hashlib
import re
from datetime import datetime
from collections import OrderedDict, defaultdict
from typing import Set, Dict, List, Optional, Tuple
from pathlib import Path

from playwright.async_api import Response, async_playwright

# --- PATCH IMPORTS ---
ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from core.settings import settings
from core.database import NexusDB
from core.browser_engine import SandboxCDPProfile
from core.humanizer import PhysicsHumanizer
from core.time_manager import CircadianCycle

logger = logging.getLogger("TikTokTopologyMapper")

class TikTokTopologyMapper:
    """
    TikTok Network Topology Mapper V37.3.
    
    Technical Improvements:
    1. Zero-Knowledge Identity: Uses SHA-256 for Node IDs.
    2. Ecosystem Score: Detects professional infrastructure (TradingView/Ledger focus).
    3. Auto-Escalation: Triggers B2B workflows immediately for VIPs.
    4. Viral Triangulation: Recursively explores social graphs of VIPs.
    """
    
    def __init__(self):
        self.running = True
        self.db = NexusDB()
        self.circadian = CircadianCycle(start_hour=8, end_hour=23)
        self.infra_signals_file = ROOT_DIR / "config" / "market_signals.json"
        
        # MAPPING: Business Keywords -> Technical Signals (V3.0 Updated - English/Global)
        self.signal_patterns = [
            # -- SAAS / Trading Tools (Global) --
            "tradingview best indicators",
            "tradingview tutorial for beginners",
            "best chart setup trading",
            "backtesting strategy free",
            "luxalgo vs tradingview",

            # -- Crypto Security / Hardware (Global) --
            "ledger nano x review",
            "best hardware wallet 2026",
            "how to store crypto safely",
            "ledger vs trezor",
            "metamask security tips",

            # -- Prop Firms (Capital - High Yield) --
            "best prop firm 2026",
            "how to pass ftmo challenge",
            "apex trader funding payout",
            "topstep vs apex",
            "funded trader daily payout",
            "prop firm discount code",
            
            # -- Generic Trading (Qualified English) --
            "day trading setup tour",
            "full time trader lifestyle",
            "forex trading strategy 2026"
        ]
        
        # Dynamic Weighting System (Feedback Loop)
        self.signal_performance: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(lambda: 1.0))
        self.consecutive_failures: Dict[str, int] = {k: 0 for k in self.signal_patterns}
        self.quarantine_list: Dict[str, float] = {} 
        
        self.base_sleep_min = 90
        self.base_sleep_max = 240
        self.current_sleep_factor = 1.0 
        self.saturation_threshold = 0.5 
        
        self.current_signal = "" 
        self.local_cache = OrderedDict()
        self.CACHE_LIMIT = 50000 

    def _check_network_density_needs(self) -> str:
        """
        Checks if the infrastructure needs more nodes.
        """
        if not self.infra_signals_file.exists():
            return "NOMINAL"
        try:
            with open(self.infra_signals_file, "r") as f:
                data = json.load(f)
                if data.get("global_state") == "saturated":
                    return "RELAX"
                if data.get("congestion_critical"):
                    return "URGENT"
        except Exception:
            pass
        return "NOMINAL"

    def _hash_identity(self, raw_id: str) -> str:
        """
        PRIVACY ENGINE: Irreversible Hash of Node Identity.
        """
        if not raw_id: return "unknown"
        salt = settings.SECURITY_MASTER_KEY.get_secret_value()[:16] 
        return hashlib.sha256((raw_id + salt).encode()).hexdigest()

    async def run(self):
        logger.info(f"📡 Topology Mapper V37.3 Started (Strategy: Arbitrage Global).")
        
        profile = SandboxCDPProfile(
            user_id="nexus_cartographer", 
            cdp_port=settings.CDP_TIKTOK_PORT,
            profile_dir=str(settings.CHROME_PROFILES_DIR / "nexus_cartographer")
        )
        
        async with async_playwright() as p:
            if not await profile.connect(p):
                logger.error("❌ CDP Connection Failed. Retry 60s.")
                await asyncio.sleep(60)
                return

            page = await profile.get_page()
            humanizer = PhysicsHumanizer(page)
            
            page.on("response", self._handle_network_response)
            logger.info("🔭 Packet Sniffer Active (Protocol: Safe-Parse).")

            while self.running:
                network_mode = self._check_network_density_needs()
                
                if network_mode == "URGENT":
                    self.current_sleep_factor = 0.5 
                    self.saturation_threshold = 0.3 
                elif network_mode == "RELAX":
                    self.current_sleep_factor = 2.0
                    self.saturation_threshold = 0.7
                    await asyncio.sleep(600) 
                else:
                    self.current_sleep_factor = 1.0
                    self.saturation_threshold = 0.5

                if network_mode != "URGENT":
                    await self.circadian.check_schedule_and_sleep_if_needed()
                
                try:
                    now_ts = datetime.utcnow().timestamp()
                    recovered = [k for k, v in self.quarantine_list.items() if v < now_ts]
                    for k in recovered:
                        del self.quarantine_list[k]
                        self.consecutive_failures[k] = 0
                        logger.info(f"♻️ Signal restored: '{k}'")

                    selected_signal = await self._select_best_signal_for_now()
                    
                    if not selected_signal:
                        logger.warning(f"⚠️ All signals quarantined. Forced Standby 10min.")
                        await asyncio.sleep(600)
                        self.quarantine_list.clear() 
                        continue

                    self.current_signal = selected_signal
                    q_encoded = self.current_signal.replace(' ', '+')
                    
                    search_url = f"https://www.tiktok.com/search?q={q_encoded}&t=video&publish_time=1"
                    
                    logger.info(f"🔎 Scanning Topology: '{self.current_signal}' (Weight: {self.signal_performance[self.current_signal][str(datetime.utcnow().hour)]:.2f})")
                    
                    await page.goto(search_url, timeout=60000, wait_until="domcontentloaded")
                    
                    scrolls = random.randint(5, 12)
                    if network_mode == "URGENT": scrolls += 5
                    
                    for _ in range(scrolls):
                        await humanizer.scroll_organic(intensity="high" if random.random() > 0.7 else "medium")
                        if random.random() < 0.3:
                            await asyncio.sleep(random.uniform(1.2, 3.5))

                except Exception as e:
                    logger.warning(f"⚠️ Navigation / Timeout Error: {e}")
                
                final_min = max(30, int(self.base_sleep_min * self.current_sleep_factor))
                final_max = min(600, int(self.base_sleep_max * self.current_sleep_factor))
                
                sleep_time = random.randint(final_min, final_max)
                await asyncio.sleep(sleep_time)

    async def _select_best_signal_for_now(self) -> Optional[str]:
        available = [q for q in self.signal_patterns if q not in self.quarantine_list]
        if not available: return None

        current_hour = str(datetime.utcnow().hour)
        weights = []
        for kw in available:
            # Dynamic weighting based on past success (Feedback Loop)
            score = max(0.1, self.signal_performance[kw][current_hour])
            discovery_boost = random.uniform(0, 0.2)
            weights.append(score + discovery_boost)

        try:
            return random.choices(available, weights=weights, k=1)[0]
        except (ValueError, IndexError):
            return random.choice(available)

    async def _handle_network_response(self, response: Response):
        url = response.url
        if not any(ep in url for ep in ["/api/search/item", "/api/item_list", "/api/recommend/item_list"]):
            return
        if response.request.resource_type not in ["xhr", "fetch"]:
            return

        try:
            try:
                if int(response.headers.get("content-length", 0)) > 2_000_000: return 
            except: pass 

            json_data = await response.json()
            await self._process_batch(json_data)

        except Exception:
            pass
    
    def _calculate_ecosystem_score(self, bio_text: str) -> int:
        """
        INTELLIGENCE LAYER: Detects Cross-Platform Ecosystems.
        A 'Super Editor' rarely relies on TikTok alone.
        V3 Update: Values TradingView/Discord for SaaS relevance.
        """
        score = 0
        bio_lower = bio_text.lower() if bio_text else ""

        # 1. Hub Detection (Link centralization)
        if any(x in bio_lower for x in ["linktr.ee", "beacons.ai", "whop", "campsite.bio"]):
            score += 30 

        # 2. Authority Channels (Long-form content)
        if "youtube" in bio_lower or "youtu.be" in bio_lower:
            score += 25
        if "t.me" in bio_lower or "telegram" in bio_lower:
            score += 20
        if "discord" in bio_lower: # High relevance for Prop Firms/SaaS
            score += 25

        # 3. B2B / Pro Vocabulary (English/French compat)
        if any(x in bio_lower for x in ["trader", "investor", "analyst", "coach", "mentor", "education", "founder", "ceo"]):
            score += 40
        
        return score

    def _calculate_node_quality(self, item: dict, reputation_data: dict) -> int:
        """
        Calculates Signal-to-Noise Ratio (SNR) + Ecosystem Score.
        """
        if reputation_data['status'] == 'BANNED':
            return 0 
        
        base_reputation = reputation_data.get('reputation_score', 50)
        score = base_reputation
        author = item.get("author", {})
        
        # 1. Capacity Ratio
        following = author.get("following_count", 0)
        followers = author.get("follower_count", 0)
        
        if following > 0:
            ratio = followers / following
            if ratio > 5000: score += 15 # Super Editor / Celebrity
            elif 0.5 < ratio < 5.0: score += 10 
            elif ratio < 0.1: score -= 20 
            elif ratio > 100: score -= 5 # Potential Bot
        
        # 2. Visual & Metadata Identity
        bio = author.get("signature", "")
        if len(bio) > 10: score += 5
        if author.get("avatar_thumb"): score += 5
        
        # 3. Ecosystem & Professionalism
        eco_score = self._calculate_ecosystem_score(bio)
        score += eco_score

        return max(0, min(100, score))

    async def _process_batch(self, data: dict):
        items = data.get("item_list", []) or data.get("data", [])
        current_hour = str(datetime.utcnow().hour)
        
        if not items:
            self._handle_failure()
            if self.current_signal:
                # Feedback Loop: Penalty
                self.signal_performance[self.current_signal][current_hour] *= 0.95
            return

        candidates = []
        super_editors_found = 0
        
        for item in items:
            item_id = str(item.get("cid") or item.get("aweme_id") or "")
            if not item_id or item_id in self.local_cache: 
                continue

            raw_author_id = item.get("author", {}).get("unique_id")
            if not raw_author_id: continue
            
            # --- PRIVACY: HASH IDENTITY IMMEDIATELY ---
            hashed_node_id = self._hash_identity(raw_author_id)

            reputation = await asyncio.to_thread(self.db.get_author_reputation, hashed_node_id, "tiktok")
            if reputation['status'] == 'BANNED' and reputation['expiry_ts'] > datetime.utcnow().timestamp():
                continue

            stats = item.get("statistics", {})
            digg_count = stats.get("digg_count", 0)
            share_count = stats.get("share_count", 0)
            
            # FILTER 1: Anti-Congestion (Relaxed for Super Editors)
            is_viral = digg_count > 5000

            # FILTER 2: Semantic Signal Analysis
            text = item.get("desc", "") or item.get("text", "")
            match_score, is_match = self._analyze_signal_strength(text)
            
            if not is_match and not is_viral:
                continue
            
            # FILTER 3: Hybrid Scoring (SNR + Ecosystem)
            node_quality = self._calculate_node_quality(item, reputation)
            
            if node_quality < 20:
                await asyncio.to_thread(self.db.update_author_reputation, hashed_node_id, "tiktok", -5)
                continue
            
            # Identify VIPs
            is_vip = node_quality > 75
            if is_vip: 
                super_editors_found += 1
                logger.info(f"🚨 VIP DETECTED: {hashed_node_id} (Quality: {node_quality})")
                
                # --- INTELLIGENCE N°4 (VIP WORKFLOWS) ---
                try:
                    high_priority_task = {
                        "type": "VIP_OUTREACH_SEQUENCE",
                        "target_id": hashed_node_id,
                        "priority": 100, 
                        "protocol": "B2B_PARTNERSHIP_V3", # Updated Protocol
                        "source_module": "topology_mapper",
                        "payload": {
                            "origin_url": f"https://www.tiktok.com/@{raw_author_id}",
                            "signal_strength": node_quality
                        }
                    }
                    if hasattr(self.db, "inject_priority_task"):
                         await asyncio.to_thread(self.db.inject_priority_task, high_priority_task)
                         logger.info(f"🚀 Workflow B2B déclenché pour {hashed_node_id}")
                except Exception as vip_err:
                    logger.error(f"❌ Echec déclenchement B2B: {vip_err}")

                # B. TRIANGULATION: Viral Graph Exploration
                try:
                    logger.info(f"🕸️ TRIANGULATION: Ajout du VIP {hashed_node_id} à la queue d'exploration virale.")
                    await asyncio.to_thread(
                        self.db.upsert_viral_target, 
                        raw_author_id, 
                        priority=90, 
                        source="TRIANGULATION_VIP"
                    )
                except Exception as tri_err:
                    logger.error(f"❌ Echec Triangulation: {tri_err}")

            # Final Capacity Calculation
            base_score = 1.0 / (digg_count + 1)
            signal_bonus = 1.5 if match_score > 10 else 1.0
            quality_factor = node_quality / 50.0 
            
            final_capacity = base_score * signal_bonus * quality_factor
            
            if final_capacity < self.saturation_threshold and not is_vip:
                continue
            
            candidates.append((item_id, item, text, final_capacity, node_quality, hashed_node_id, is_vip))

        if not candidates:
            self._handle_failure()
            if self.current_signal:
                self.signal_performance[self.current_signal][current_hour] *= 0.95
            return

        new_nodes_count = 0
        for cid, raw_item, text, capacity, quality, hashed_author_id, is_vip in candidates:
            self.local_cache[cid] = datetime.utcnow()
            
            video_id = raw_item.get("aweme_id", "")
            target_url = f"https://www.tiktok.com/@{raw_item.get('author', {}).get('unique_id')}/video/{video_id}" 

            intent_label = "VIP_PARTNER_CANDIDATE" if is_vip else "NODE_CANDIDATE"

            node_data = {
                "id": cid,
                "source": "tiktok_mapper",
                "author": hashed_author_id,
                "text": text,
                "url": target_url,
                "ai_process_info": json.dumps({
                    "intent": intent_label,
                    "throughput_score": int(capacity * 100),
                    "signal_quality": quality,
                    "is_vip": is_vip,
                    "context": "Discovered by Topology Mapper V37.3 (Strategy 3.0)"
                }),
                "created_at": datetime.utcnow().timestamp()
            }
            
            inserted = self.db.insert_raw_lead(node_data)
            if inserted:
                new_nodes_count += 1
                trust_bonus = +10 if is_vip else +2
                await asyncio.to_thread(self.db.update_author_reputation, hashed_author_id, "tiktok", trust_bonus, "TRUSTED")
            
            if len(self.local_cache) > self.CACHE_LIMIT: self.local_cache.popitem(last=False)

        if new_nodes_count > 0:
            logger.info(f"⚡ {new_nodes_count} Nodes discovered (VIPs: {super_editors_found}). Signal '{self.current_signal}'.")
            
            # Feedback Loop: Reward
            if self.current_signal:
                current_val = self.signal_performance[self.current_signal][current_hour]
                boost = 1.5 if super_editors_found > 0 else 1.1
                self.signal_performance[self.current_signal][current_hour] = min(10.0, current_val * boost)
            
            self.current_sleep_factor = max(0.25, self.current_sleep_factor * 0.8)
            self.consecutive_failures[self.current_signal] = 0
        else:
            self._handle_failure()

    def _handle_failure(self):
        self.current_sleep_factor = min(3.0, self.current_sleep_factor * 1.2)
        if self.current_signal:
            self.consecutive_failures[self.current_signal] = self.consecutive_failures.get(self.current_signal, 0) + 1
            if self.consecutive_failures[self.current_signal] >= 3:
                logger.warning(f"📉 Signal '{self.current_signal}' degraded (3 failures). Quarantined 4h.")
                self.quarantine_list[self.current_signal] = datetime.utcnow().timestamp() + (4 * 3600)

    def _analyze_signal_strength(self, text: str) -> Tuple[int, bool]:
        """
        Analyzes text for Authority & Pro Signals.
        V3 Update: English/Global Priority.
        """
        t = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8').lower()
        score = 0
        
        # High Authority Markers (Education/Technical Focus - English/French)
        authority_markers = [
            # EN
            "review", "tutorial", "guide", "strategy", "analysis", "backtest", "indicator", "setup", 
            # FR (Legacy support)
            "avis", "tuto", "stratégie", "analyse", "configuration"
        ]
        for m in authority_markers:
            if m in t: score += 8

        # Strong B2B Markers
        strong_markers = [
            # EN
            "link in bio", "partner", "collab", "tools", "software", "discount", "code",
            # FR
            "lien en bio", "partenaire", "logiciel"
        ]
        for m in strong_markers:
            if m in t: score += 5

        # Protocol Context Markers (V3: SaaS/Prop/Crypto)
        context_markers = ["trading", "invest", "crypto", "bitcoin", "ledger", "funding", "capital", "chart", "prop firm", "funded"]
        for m in context_markers:
            if m in t: score += 3

        # Noise/Interference Markers (Begging or Banking)
        # Added "bourso" and "banque" as Negative Signals
        noise_markers = ["svp", "besoin", "qui a", "donnez moi", "arnaque", "fake", "banque", "bourso", "parrainage banque", "scam", "give me"]
        for m in noise_markers:
            if m in t: score -= 20
            
        is_match = score > 10
        return score, is_match

if __name__ == "__main__":
    try:
        mapper = TikTokTopologyMapper()
        asyncio.run(mapper.run())
    except KeyboardInterrupt:
        pass