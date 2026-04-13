### prospection/channels/reddit/audience_listener.py
# channels/reddit/sniper_cdp.py - AUDIENCE LISTENER V26.1 (STRATEGIC REALIGNMENT V3)
# Note: Filename preserved for system compatibility (Launcher/Docker).
# Module Role: Semantic Analysis & Qualified Audience Segment Detection.

import asyncio
import json
import time
import logging
import random
import math
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Tuple
from playwright.async_api import async_playwright, Response

# --- CORE IMPORTS ---
import sys
try:
    sys.path.append(str(Path(__file__).resolve().parents[2]))
    from core.browser_engine import SandboxCDPProfile
    from core.database import NexusDB
    from core.settings import settings
except ImportError:
    print("🔴 Modules Core missing.")
    sys.exit(1)

# --- CONFIGURATION ---
CDP_PORT = settings.CDP_REDDIT_PORT
EXPORT_DIR = settings.BASE_DIR / "exports"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)
RESTART_CYCLE = 50 
WATCHDOG_TIMEOUT = 1800
MAX_CACHE_SIZE = 10000 
MAX_BODY_SIZE = 1024 * 1024 

# Audience Targeting (Geolocated Segments)
# STRATEGY V3: Pivot to Trading/Prop Firms/Crypto. Remove Banking/LowQuality.
# NOTE: Values serve as Timezone Offsets (hours) for the Scheduler.
TARGET_SEGMENTS = {
    "Daytrading": 2,        # Global Segment (European Session align)
    "Forex": 1,             # Global Segment (London Session align)
    "PropFirm": 5,          # Core Target (Apex/FTMO) - NY/Global align
    "FundedTrading": 5,     # Core Target - NY/Global align
    "algotrading": 0,       # Tech Savvy - UTC
    "CryptoCurrency": 1,    # Secondary Target - Global
    "FuturesTrading": 4,    # Ajout V3: Marché Futures (US/CME align)
    "SecurityAnalysis": 3   # High IQ/Net Worth
    # SUPPRESSION: "sidehustle", "passiveincome" (Dilution audience)
    # SUPPRESSION: "finance", "vosfinances" (Banque France)
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s | [AUDIENCE-LISTENER] %(message)s")
logger = logging.getLogger("AudienceListener")

class SemanticIntentClassifier:
    """
    Semantic Analysis Engine for Intent Qualification.
    Ensures Brand Safety by filtering non-compliant content.
    """
    HIGH_INTENT_SIGNALS = [
        "looking for", "cherche", "besoin", "need", "want", "search", 
        "quelqu'un a", "anyone have", "code for", "valid code", "help",
        "challenge", "evaluation", "funded", "payout"
    ]
    COMMERCIAL_SIGNALS = [
        "use my", "mon code", "my link", "sign up with", "get bonus", 
        "free money", "use this", "mon lien", "voici", "here is"
    ]
    INFORMATIONAL_SIGNALS = [
        "how to", "comment", "work?", "fonctionne", "explain", "guide", "tuto", "strategy"
    ]

    @staticmethod
    def analyze_segment(title: str, body: str) -> Tuple[int, str]:
        full_text = (title + " " + body).lower()
        relevance_score = 0
        segment_type = "GENERAL_INTEREST"
        
        # --- BRAND SAFETY (Exclusion Filter) ---
        # Excludes competitors and unauthorized resellers to protect client mandates.
        UNAUTHORIZED_MARKERS = ["http", "www.", ".com", "mon code", "mon lien", "my link", "use code", "referral", "parrainage.io", "linktr.ee"]
        if any(m in full_text for m in UNAUTHORIZED_MARKERS):
            return -100, "EXCLUSION_COMPETITOR"

        # --- INTENT SCORING ---
        
        # Detected existing commercial offer (Low value for acquisition)
        if any(kw in full_text for kw in SemanticIntentClassifier.COMMERCIAL_SIGNALS):
            relevance_score = 10 
            segment_type = "COMMERCIAL_OFFER"

        # High Intent Detected (Qualified Prospect)
        if any(kw in full_text for kw in SemanticIntentClassifier.HIGH_INTENT_SIGNALS):
            relevance_score = 80
            segment_type = "HIGH_INTENT_PROSPECT"
        
        elif any(kw in full_text for kw in SemanticIntentClassifier.INFORMATIONAL_SIGNALS):
            relevance_score = 60
            segment_type = "INFORMATIONAL_QUERY"

        if "?" in title: relevance_score += 10
        
        # V3 STRATEGY: KILL SWITCH ON BANKING / BOOST ON TRADING
        if "prop firm" in full_text or "apex" in full_text or "ftmo" in full_text: 
            relevance_score += 20
        if "trading" in full_text and "profit" in full_text:
            relevance_score += 10
            
        # Negative scoring for old banking targets (Waste of resources)
        if "bourso" in full_text or "banque" in full_text or "fortuneo" in full_text or "easy money" in full_text: 
            relevance_score -= 50
        
        if "bot" in full_text or "scam" in full_text: 
            return -100, "EXCLUSION_RISK"

        return relevance_score, segment_type

class CircadianScheduler:
    """
    Optimizes polling schedules based on target audience active hours.
    Reduces resource usage during off-peak hours.
    """
    @staticmethod
    def get_time_factor(offset_hour: int) -> float:
        utc_now = datetime.utcnow().hour
        local_hour = (utc_now + offset_hour) % 24
        
        if 8 <= local_hour < 17:
            return 0.8 # Business Hours
        elif 17 <= local_hour <= 23:
            return 1.5 # Peak Engagement
        elif 0 <= local_hour < 3:
            return 0.5 # Late Night
        else:
            return 0.1 # Deep Night (Sleep)

    @staticmethod
    def adjust_priorities(base_weights: Dict[str, float]) -> Dict[str, float]:
        adjusted = {}
        for sub, weight in base_weights.items():
            offset = TARGET_SEGMENTS.get(sub, 0)
            time_factor = CircadianScheduler.get_time_factor(offset)
            adjusted[sub] = weight * time_factor
        return adjusted

class RedditStreamListener:
    def __init__(self, db: NexusDB):
        self.db = db
        self.leads_count = 0
        self.last_activity = time.time()
        
        self.queue: asyncio.Queue = asyncio.Queue()
        self.seen_ids = set() 
        self._worker_task = asyncio.create_task(self._persistence_worker())

    async def _persistence_worker(self):
        loop = asyncio.get_running_loop()
        logger.info("💾 Persistence Worker started.")
        
        while True:
            try:
                data = await self.queue.get()
                
                inserted = await loop.run_in_executor(
                    None, 
                    self.db.insert_raw_lead, 
                    data
                )
                
                if inserted:
                    # GDPR Compliance: Only partial ID logged
                    masked_id = data.get('author', 'unknown')[:8]
                    logger.info(f"🎯 SEGMENT QUALIFIED [{data.get('intent', '?')}]: User {masked_id}... -> Score {data.get('score')}")
                    self.leads_count += 1
                
                self.queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ Worker Error: {e}")

    async def handle_network_response(self, response: Response):
        url = response.url
        if "gql.reddit.com" not in url and "gateway.reddit.com" not in url:
            return

        if "application/json" not in response.headers.get("content-type", ""):
            return

        try:
            content_len = response.headers.get("content-length")
            if content_len and int(content_len) > MAX_BODY_SIZE:
                return 
        except: pass

        try:
            self.last_activity = time.time() 
            data = await response.json()
            
            posts = self._extract_posts_iterative(data)
            if posts:
                self._enqueue_posts(posts)

        except Exception:
            pass

    def _extract_posts_iterative(self, root_data):
        found = []
        stack = [root_data]

        while stack:
            data = stack.pop()
            
            if isinstance(data, dict):
                if "id" in data and "title" in data and "author" in data:
                    found.append(data)
                
                for value in data.values():
                    if isinstance(value, (dict, list)):
                        stack.append(value)
            
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, (dict, list)):
                        stack.append(item)
        
        return found

    def _enqueue_posts(self, posts):
        for post in posts:
            pid = post.get("id")
            if not pid: continue
            
            if len(self.seen_ids) > MAX_CACHE_SIZE:
                self.seen_ids.clear()
                logger.info("🧹 Cache cleared.")

            if pid in self.seen_ids: continue
            self.seen_ids.add(pid)
            
            title = post.get("title", "")
            text = post.get("selftext", "") or post.get("body", "")
            author = post.get("author", "Unknown")
            if isinstance(author, dict): author = author.get("name", "Unknown")
            
            permalink = post.get("permalink", "")
            
            # AdTech Filters
            if post.get("isSponsored") or post.get("isStickied"): continue
            if "AutoModerator" in str(author): continue
            if "promoted" in str(author).lower(): continue

            relevance, segment_type = SemanticIntentClassifier.analyze_segment(title, text)
            
            if relevance < 20: 
                continue

            # --- GDPR COMPLIANCE CORE ---
            # Irreversible hashing of PII before any storage.
            author_hash = hashlib.sha256(str(author).encode("utf-8")).hexdigest()

            # Frequency Capping Check (Prevents ad fatigue)
            if self.db.check_if_user_already_targeted(author_hash, hours=24):
                continue
            
            lead_data = {
                "id": pid,
                "source": "reddit_listener",
                "author": author_hash, # ANONYMIZED STORAGE
                "text": f"{title}\n{text}",
                "url": f"https://www.reddit.com{permalink}",
                "score": relevance,
                "intent": segment_type
            }
            
            self.queue.put_nowait(lead_data)

    def check_health(self) -> bool:
        if time.time() - self.last_activity > WATCHDOG_TIMEOUT:
            logger.warning("⚠️ WATCHDOG: No network activity detected for 30min.")
            return False
        return True
    
    async def shutdown(self):
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass

async def run_audience_listener():
    logger.info("📡 NEXUS AUDIENCE LISTENER V26.1 STARTED")
    
    db = NexusDB()
    listener = RedditStreamListener(db)
    
    profile = SandboxCDPProfile(
        user_id="reddit_main", 
        cdp_port=CDP_PORT, 
        profile_dir=str(settings.CHROME_PROFILES_DIR / "reddit_main")
    )

    base_segment_weights = {sub: 1.0 for sub in TARGET_SEGMENTS.keys()}

    async with async_playwright() as p:
        if not await profile.connect(p):
            logger.error("❌ CDP Connection Failed.")
            return

        page = await profile.get_page()
        page.on("response", listener.handle_network_response)
        logger.info("✅ Listener Attached to Browser Stream.")

        loop_count = 0

        try:
            while True:
                loop_count += 1
                if loop_count % RESTART_CYCLE == 0:
                    logger.info("♻️ MAINTENANCE: Soft memory recycle...")
                    try:
                        if profile.context:
                            await profile.context.close()
                        await asyncio.sleep(2)
                        await profile.connect(p)
                        page = await profile.get_page()
                        page.on("response", listener.handle_network_response)
                    except Exception as e:
                        logger.error(f"❌ Recycle Error: {e}")

                if not listener.check_health():
                    try: await page.reload()
                    except: pass
                    listener.last_activity = time.time()

                # --- SCHEDULING INTELLIGENCE ---
                current_weights = CircadianScheduler.adjust_priorities(base_segment_weights)
                
                subs = list(current_weights.keys())
                weights = list(current_weights.values())
                
                try:
                    target = random.choices(subs, weights=weights, k=1)[0]
                except (ValueError, IndexError):
                    target = random.choice(list(TARGET_SEGMENTS.keys()))
                
                url = f"https://www.reddit.com/r/{target}/new/"
                
                logger.info(f"🔎 Scanning Segment '{target}' (Priority: {current_weights.get(target, 0):.2f})")
                
                start_leads = listener.leads_count
                
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                    
                    # Human-like interaction for accurate rendering
                    for _ in range(random.randint(3, 6)):
                        await page.mouse.wheel(0, random.randint(500, 1000))
                        await asyncio.sleep(random.uniform(1.5, 3.0))
                    
                except Exception:
                    pass

                leads_found = listener.leads_count - start_leads
                
                # --- YIELD OPTIMIZATION ---
                # Reinforcement Learning for Segment Weights
                if leads_found > 0:
                    base_segment_weights[target] *= 1.3
                    logger.info(f"📈 YIELD UPLIFT: +{leads_found} prospects in '{target}'. Increasing allocation.")
                else:
                    base_segment_weights[target] *= 0.95
                
                # Normalization
                if loop_count % 20 == 0:
                     for k in base_segment_weights:
                         base_segment_weights[k] = (base_segment_weights[k] + 1.0) / 2.0

                pause = random.randint(15, 45)
                logger.info(f"⏳ Standby {pause}s")
                await asyncio.sleep(pause)
        finally:
            await listener.shutdown()

if __name__ == "__main__":
    asyncio.run(run_audience_listener())