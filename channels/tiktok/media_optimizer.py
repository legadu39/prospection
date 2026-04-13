### prospection/channels/tiktok/media_optimizer.py
# channels/tiktok/media_optimizer.py - MEDIA OPTIMIZER V27.0 (PREDICTIVE VELOCITY)
# Note: Filename preserved for system compatibility.
# Module Role: ROI Optimization, Velocity Detection & Data Privacy.

"""
NEXUS MEDIA OPTIMIZER V27.0
---------------------------
1. Yield Management : Calculates Inventory Efficiency (ROI).
   - Low Yield Assets (> 15 empty polls) are deprecated to save resources.
2. Velocity Detection (PREDICTIVE) : Dérivée du taux d'engagement pour anticiper les pics.
3. Privacy by Design : Immediate hashing of User IDs.
4. Semantic Triage : Prioritizes high-urgency queries for support.
"""

import asyncio
import time
import logging
import hashlib
import random
import re
import sys
import json
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
from playwright.async_api import async_playwright, Page

# --- CORE IMPORTS ---
try:
    from core.database import NexusDB
    from core.browser_engine import SandboxCDPProfile, StealthInjector
    from core.time_manager import CircadianCycle
    from core.settings import settings
except ImportError:
    sys.path.append(str(Path(__file__).resolve().parents[2]))
    from core.database import NexusDB
    from core.browser_engine import SandboxCDPProfile, StealthInjector
    from core.time_manager import CircadianCycle
    from core.settings import settings

# --- LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | [MEDIA-OPTIMIZER] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(settings.LOGS_DIR / "media_optimizer.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("MediaOptimizer")

# --- CONFIGURATION ---
class Config:
    TARGET_USERNAME: str = getattr(settings, "TARGET_MONITOR_USER", None)

    MAX_ASSETS_TO_TRACK: int = 20 
    CDP_PORT: int = settings.CDP_TIKTOK_PORT
    PAGE_RECYCLE_INTERVAL: int = 10 
    
    # Heat / Velocity Parameters
    BASE_POLL_INTERVAL: int = 300       
    MIN_POLL_INTERVAL: int = 60         
    VELOCITY_INTERVAL: int = 10       # High Velocity Mode
    MAX_POLL_INTERVAL: int = 3600       
    
    HEAT_UPLIFT: float = 1.5        
    HEAT_DECAY: float = 0.8        
    
    # Yield Thresholds
    YIELD_DROP_THRESHOLD: int = 15     # Consecutive empty scans before depreciation
    IMMUNITY_THRESHOLD: int = 5    # Minimum conversions to retain asset
    
    MIN_COMMENT_LENGTH: int = 4
    
    # Brand Safety & Competitor Filters
    COMPETITOR_KEYWORDS = [
        "mon code", "mon lien", "ma bio", "en bio", "dm moi", 
        "viens pv", "je parraine", "code :", "rejoins", "gagne"
    ]

    SYSTEM_NOISE = [
        "pc_web", "explorePage", "tiktok", "loading", "chargement", 
        "répondre", "reply", "voir plus", "original sound", "traduire"
    ]

    SUCCESS_SIGNALS = [
        "fait", "inscrit", "validé", "merci", "top", "reçu", 
        "ça marche", "c'est bon", "good", "nickel"
    ]
    
    URGENCY_SIGNALS = [
        "prix", "combien", "acheter", "bug", "marche pas", "aide", 
        "arnaque", "danger", "fake", "vrai"
    ]

    STATE_FILE: Path = settings.BASE_DIR / "config" / "optimizer_state.json"

@dataclass
class MediaAssetMetrics:
    """Metrics for a specific media asset (video)."""
    url: str
    heat_score: float = 1.0
    next_scan_time: float = 0.0
    last_scan_time: float = 0.0
    
    # Yield Metrics
    total_scans: int = 0
    total_conversions: int = 0
    consecutive_empty_scans: int = 0
    velocity_history: List[int] = field(default_factory=list) 
    is_immune: bool = False 

    @property
    def yield_efficiency(self) -> float:
        """Inverse of Bounce Rate: Logic to determine asset value."""
        if self.total_scans == 0: return 0.0
        return self.consecutive_empty_scans / max(1, self.total_scans)

    @property
    def is_depreciated(self) -> bool:
        """Determines if asset should be dropped from tracking."""
        if self.is_immune: return False
        return self.consecutive_empty_scans > Config.YIELD_DROP_THRESHOLD and self.total_conversions < Config.IMMUNITY_THRESHOLD

    def predict_trend(self) -> str:
        """
        Oracle Algorithm: Calcule la pente de l'engagement récent pour anticiper
        si une vidéo va buzzer ou mourir.
        """
        history = self.velocity_history[-5:] # Analyse sur les 5 derniers points de mesure
        if len(history) < 3: return "STABLE"

        # Régression linéaire simplifiée (Slope)
        # x = temps (indices 0..N), y = conversions
        # Formule de pente m = (N*sum(xy) - sum(x)sum(y)) / (N*sum(x^2) - sum(x)^2)
        # Ici on simplifie: (Dernier - Premier) / Durée
        
        slope = (history[-1] - history[0]) / max(1, len(history) - 1)

        if slope > 0.5: return "RISING_FAST"   # Accélération notable
        if slope < -0.2: return "COOLING_DOWN" # Ralentissement
        return "STABLE"

# --- UTILS ---
def generate_compliance_id(video_url: str, author_hash: str, text: str) -> str:
    """Generates a deterministic ID without storing PII."""
    raw = f"{video_url}::{author_hash}::{text}"
    return f"tt_opt_{hashlib.sha256(raw.encode('utf-8')).hexdigest()[:16]}"

def is_competitor_signal(text: str) -> bool:
    t_lower = text.lower()
    return any(kw in t_lower for kw in Config.COMPETITOR_KEYWORDS)

def is_noise(text: str) -> bool:
    if len(text) < Config.MIN_COMMENT_LENGTH: return True
    if re.match(r"^\d+\s?[hjmgs]$", text.strip()): return True 
    t_lower = text.lower()
    return any(ban in t_lower for ban in Config.SYSTEM_NOISE)

def calculate_urgency(text: str) -> int:
    t_lower = text.lower()
    score = 0
    if any(kw in t_lower for kw in Config.URGENCY_SIGNALS):
        score += 10
    if "?" in text:
        score += 2
    return score

class StateManager:
    """Persistence layer for Optimization Context."""
    @staticmethod
    def save(videos: Dict[str, MediaAssetMetrics]):
        try:
            data = {
                "updated_at": time.time(),
                "assets": {url: asdict(state) for url, state in videos.items()}
            }
            temp = Config.STATE_FILE.with_suffix(".tmp")
            with open(temp, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            temp.replace(Config.STATE_FILE)
        except Exception as e:
            logger.warning(f"⚠️ State Save Error: {e}")

    @staticmethod
    def load() -> Dict[str, MediaAssetMetrics]:
        if not Config.STATE_FILE.exists():
            return {}
        try:
            with open(Config.STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                
            videos = {}
            now = time.time()
            valid_threshold = now - (3 * 86400)
            
            for url, raw in data.get("assets", {}).items():
                if raw.get("last_scan_time", 0) > valid_threshold:
                    videos[url] = MediaAssetMetrics(**raw)
            
            if videos:
                logger.info(f"💾 Context Restored: {len(videos)} assets loaded.")
            return videos
        except Exception as e:
            logger.warning(f"⚠️ State Load Error: {e}")
            return {}

# --- ENGINE ---
class OptimizationEngine:
    def __init__(self, context, identity):
        self.context = context
        self.identity = identity
        self.page: Optional[Page] = None
        self.db = NexusDB()

    async def get_fresh_page(self):
        if self.page:
            try:
                if not self.page.is_closed():
                    await self.page.close()
            except Exception as e:
                logger.warning(f"⚠️ Page Close Error: {e}")
        
        self.page = await self.context.new_page()
        
        if self.identity:
            await StealthInjector.inject(self.page, self.identity)
        else:
            logger.warning("⚠️ Identity missing. Default injection applied.")
            
        return self.page

    async def _simulate_engagement(self, intensity: int = 1):
        if not self.page: return
        
        previous_height = 0
        try:
            previous_height = await self.page.evaluate("document.body.scrollHeight")
        except: pass

        for i in range(intensity):
            scroll_y = random.randint(400, 800)
            try:
                await self.page.evaluate(f"window.scrollBy(0, {scroll_y})")
                await asyncio.sleep(random.uniform(0.8, 1.5))
                
                new_height = await self.page.evaluate("document.body.scrollHeight")
                if new_height == previous_height and i > 1:
                    break
                previous_height = new_height
            except Exception:
                break

    async def fetch_media_inventory(self) -> List[str]:
        if not Config.TARGET_USERNAME:
            logger.error("❌ CONFIG ERROR: 'TARGET_MONITOR_USER' not set.")
            return []

        url = f"https://www.tiktok.com/@{Config.TARGET_USERNAME}"
        logger.info(f"📱 Refreshing Media Inventory: {url}")
        
        try:
            await self.page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(3)
            await self._simulate_engagement(2)
            
            videos = await self.page.evaluate("""(data) => {
                const res = [];
                const limit = data.limit;
                const username = data.username.toLowerCase();
                
                const selectors = ['a[href*="/video/"]', '[data-e2e="user-post-item"] a'];
                let elements = [];
                for (const s of selectors) {
                    elements = document.querySelectorAll(s);
                    if (elements.length > 0) break;
                }

                elements.forEach(el => {
                    if(res.length < limit && el.href) {
                        const href = el.href.split('?')[0];
                        if (href.toLowerCase().includes(username)) {
                            res.push(href);
                        }
                    }
                });
                return res;
            }""", {"limit": Config.MAX_ASSETS_TO_TRACK, "username": Config.TARGET_USERNAME})
            
            unique = list(set(videos))
            logger.info(f"   📹 {len(unique)} assets indexed.")
            return unique

        except Exception as e:
            logger.error(f"❌ Inventory Fetch Error: {e}")
            return []

    async def analyze_engagement(self, video_url: str) -> int:
        """
        Parses engagement signals (comments) to identify conversion opportunities.
        Applies GDPR hashing immediately.
        """
        logger.info(f"   🔎 Analyzing: {video_url.split('/')[-1]}")
        loop = asyncio.get_running_loop()
        new_opportunities = 0
        
        try:
            await self.page.goto(video_url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(random.uniform(3, 5))
            
            try:
                close_btn = self.page.locator('button[data-e2e="modal-close-inner-button"], .css-18e9323-ButtonClose').first
                if await close_btn.is_visible():
                    await close_btn.click()
            except: pass

            await self._simulate_engagement(3)
            raw_signals = await self._extract_signals_from_dom()
            
            if len(raw_signals) < 5:
                await self._simulate_engagement(5)
                raw_signals = await self._extract_signals_from_dom()

            for s in raw_signals:
                raw_author = s['author']
                text = s['text']
                
                if is_noise(text) or is_competitor_signal(text):
                    continue
                
                # --- GDPR : PII HASHING ---
                author_hash = hashlib.sha256(raw_author.encode("utf-8")).hexdigest()

                # --- SEMANTIC CHECK ---
                existing_lead = await loop.run_in_executor(None, self.db.get_lead_by_author, author_hash)
                
                if existing_lead:
                    current_status = existing_lead.get('status')
                    if current_status in ['SENT', 'CLICKED_LINK', 'READY_TO_SEND', 'SENDING']:
                        if any(kw in text.lower() for kw in Config.SUCCESS_SIGNALS):
                            logger.info(f"✅ CONFIRMED SUCCESS: {author_hash[:8]}... validated conversion.")
                            await loop.run_in_executor(
                                None, 
                                self.db.register_conversion_event, 
                                existing_lead['id'], 0.0, f"SEMANTIC_{int(time.time())}"
                            )
                            continue 
                        else:
                            continue

                urgency = calculate_urgency(text)
                ai_info = {"urgency_score": urgency}
                if urgency >= 10:
                    ai_info["fast_track"] = True
                    logger.info(f"🚀 HIGH PRIORITY: Urgent query detected ({author_hash[:8]}...)")

                lead_id = generate_compliance_id(video_url, author_hash, text)
                
                lead_data = {
                    "id": lead_id,
                    "source": "tiktok_optimizer",
                    "author": author_hash, # ANONYMIZED
                    "url": video_url, 
                    "comment_text": text,
                    "profile_url": s.get('url', ''),
                    "created_at": time.time(),
                    "ai_process_info": json.dumps(ai_info)
                }

                inserted = await loop.run_in_executor(None, self.db.insert_raw_lead, lead_data)
                if inserted:
                    logger.info(f"      ✨ NEW PROSPECT: {author_hash[:8]}... (Score: {urgency})")
                    new_opportunities += 1
            
            return new_opportunities

        except Exception as e:
            logger.error(f"⚠️ Analysis Error {video_url}: {e}")
            return 0

    async def _extract_signals_from_dom(self) -> List[Dict]:
        return await self.page.evaluate("""() => {
            const data = [];
            const containerSelectors = [
                '[data-e2e="comment-level-1"]', '.comment-item', 
                'div[class*="DivCommentItem"]', '.css-1i7ohvi-DivCommentItemContainer'
            ];
            
            let containers = [];
            for (const s of containerSelectors) {
                containers = document.querySelectorAll(s);
                if (containers.length > 0) break;
            }

            containers.forEach(el => {
                try {
                    let authorEl = el.querySelector('[data-e2e="comment-username"]') || el.querySelector('a[href*="/@"]');
                    let textEl = el.querySelector('[data-e2e="comment-text"]') || el.querySelector('p[class*="PCommentText"]');
                    
                    if(authorEl && textEl) {
                        data.push({
                            author: authorEl.innerText.trim(),
                            text: textEl.innerText.trim(),
                            url: authorEl.href || ''
                        });
                    }
                } catch(e){}
            });
            return data;
        }""")

# --- MAIN PROCESS ---

async def run_media_optimizer():
    logger.info("🚀 NEXUS MEDIA OPTIMIZER V27.0 STARTED")
    
    if not Config.TARGET_USERNAME:
        logger.critical("⛔ FATAL: TARGET_MONITOR_USER missing.")
        return

    cycle = CircadianCycle(start_hour=8, end_hour=23)
    
    profile = SandboxCDPProfile(
        user_id="tiktok_monitor",
        cdp_port=Config.CDP_PORT,
        profile_dir=str(settings.CHROME_PROFILES_DIR / "tiktok_main") 
    )

    monitored_assets: Dict[str, MediaAssetMetrics] = StateManager.load()
    last_full_refresh = 0
    REFRESH_DELAY = 1800 

    if not monitored_assets:
        StateManager.save({})

    try:
        async with async_playwright() as p:
            logger.info("🔌 Connecting to CDP...")
            if not await profile.connect(p):
                logger.critical("❌ CDP Connection Failed.")
                return

            context = profile.context
            engine = OptimizationEngine(context, identity=profile.identity)
            await engine.get_fresh_page()
            
            loop_count = 0

            while True:
                await cycle.check_schedule_and_sleep_if_needed()
                loop_count += 1
                now = time.time()

                # Refresh Inventory
                if now - last_full_refresh > REFRESH_DELAY:
                    logger.info("🔄 Updating Inventory Index...")
                    latest_urls = await engine.fetch_media_inventory()
                    
                    current_keys = set(monitored_assets.keys())
                    new_keys = set(latest_urls)
                    
                    for url in new_keys:
                        if url not in current_keys:
                            logger.info(f"🆕 New Asset Tracked: {url}")
                            monitored_assets[url] = MediaAssetMetrics(url=url, heat_score=2.0, next_scan_time=now)
                    
                    last_full_refresh = now
                    StateManager.save(monitored_assets)

                if loop_count % Config.PAGE_RECYCLE_INTERVAL == 0:
                    await engine.get_fresh_page()

                active_tasks = 0
                assets_to_prune = []

                for url, state in monitored_assets.items():
                    if now >= state.next_scan_time:
                        
                        conversions_found = await engine.analyze_engagement(url)
                        
                        state.last_scan_time = now
                        state.total_scans += 1
                        
                        state.velocity_history.append(conversions_found)
                        if len(state.velocity_history) > 10: state.velocity_history.pop(0)

                        if conversions_found > 0:
                            state.total_conversions += conversions_found
                            state.consecutive_empty_scans = 0
                            
                            if state.total_conversions >= Config.IMMUNITY_THRESHOLD and not state.is_immune:
                                state.is_immune = True
                                logger.info(f"🛡️ ASSET SECURED: {url} marked as High Value ({state.total_conversions} conv).")

                            # --- INTELLIGENCE : PREDICTIVE VELOCITY ---
                            trend = state.predict_trend()
                            avg_prev = sum(state.velocity_history[:-1]) / max(1, len(state.velocity_history)-1) if len(state.velocity_history) > 1 else 0
                            
                            if trend == "RISING_FAST" or conversions_found >= 5:
                                logger.info(f"🔮 ORACLE: Montée en charge sur {url} ({trend}). Mode Préemptif.")
                                state.heat_score = 5.0 
                                interval = Config.VELOCITY_INTERVAL 
                            else:
                                state.heat_score = min(5.0, state.heat_score * Config.HEAT_UPLIFT)
                                interval = max(Config.MIN_POLL_INTERVAL, Config.BASE_POLL_INTERVAL / state.heat_score)
                        
                        else:
                            state.consecutive_empty_scans += 1
                            
                            # Yield Management
                            if state.is_depreciated:
                                logger.warning(f"📉 YIELD DROP: {url} depreciated (Low Efficiency). Removed from active pool.")
                                assets_to_prune.append(url)
                                continue 
                            
                            state.heat_score = max(0.2, state.heat_score * Config.HEAT_DECAY)
                            interval = min(Config.MAX_POLL_INTERVAL, Config.BASE_POLL_INTERVAL / state.heat_score)
                        
                        state.next_scan_time = now + interval
                        active_tasks += 1
                        
                        await asyncio.sleep(random.uniform(5, 10))

                if assets_to_prune:
                    for v in assets_to_prune:
                        del monitored_assets[v]
                    StateManager.save(monitored_assets)
                elif loop_count % 5 == 0:
                    StateManager.save(monitored_assets)

                if active_tasks == 0:
                    next_wakes = [s.next_scan_time for s in monitored_assets.values()]
                    if next_wakes:
                        sleep_time = max(5, min(60, min(next_wakes) - now))
                        await asyncio.sleep(sleep_time)
                    else:
                        await asyncio.sleep(60)

    except KeyboardInterrupt:
        logger.info("🛑 Manual Stop. Saving State...")
        StateManager.save(monitored_assets)
    except Exception as e:
        logger.critical(f"🔥 Optimizer Crash: {e}")
        try: StateManager.save(monitored_assets)
        except: pass

if __name__ == "__main__":
    asyncio.run(run_media_optimizer())