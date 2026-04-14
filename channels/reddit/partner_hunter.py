### prospection/channels/reddit/partner_hunter.py
# channels/reddit/partner_hunter.py - PARTNER HUNTER V11.1 (CLEAN SHEET STRATEGY)
# -*- coding: utf-8 -*-

"""
REDDIT PARTNER HUNTER V11.1
---------------------------
Module d'identification de Partenaires & Leads Globaux.
INTELLIGENCE V11.1: 
1. Global Reach (US/UK/FR targets).
2. Distinction Vendeurs (B2B) vs Traders (Prop Firm Leads).
3. Triangulation d'Intention (Urgency Score).
4. Banking Noise Filter (Exclusion stricte des sujets banque de détail FR).
CONFORMITÉ RGPD : Anonymisation systématique.
"""

import asyncio
import logging
import random
import json
import sys
import time
from collections import OrderedDict, deque
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

from playwright.async_api import Response, async_playwright

# --- PATCH IMPORTS ---
ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from core.settings import settings  # noqa: E402
from core.database import NexusDB  # noqa: E402
from core.browser_engine import SandboxCDPProfile  # noqa: E402
from core.humanizer import PhysicsHumanizer  # noqa: E402

logger = logging.getLogger("PartnerHunter")

# Cibles Globales (Mise à jour V3.0)
TARGET_SUBREDDITS = [
    # --- TIER 1: GLOBAL PROP FIRMS (US/UK/Global) ---
    "Daytrading",  # Le coeur de cible
    "Forex",  # Marché principal Prop Firms
    "PropFunded",  # Spécifique Prop Firms
    "algotrading",  # Traders automatisés (B2B SaaS)
    "Futures",  # Cible Apex/Topstep
    "wallstreetbets",  # Volume / Gambling (High Risk, High Reward)
    "RealDayTrading",  # Sérieux / Éducatif
    # --- TIER 2: FR MARKET (SaaS & Crypto Only) ---
    "vosfinances",  # CSP+ FR (Filtrage Bancaire Strict Requis)
    "francefire",  # Investisseurs
    "cryptomonnaie",  # Crypto FR
    "finance",
    "investir",
]


class BurstEntry:
    def __init__(self, author_hash, timestamp, subreddit):
        self.author_hash = author_hash
        self.ts = timestamp
        self.subreddit = subreddit


class AuthorityClassifier:
    """
    Intelligent classifier to distinguish 'Super Editors' (B2B) from 'Traders' (Leads).
    """

    @staticmethod
    def classify(title: str, body: str) -> Tuple[str, int, str]:
        """
        Returns: (Tier, Score, Intent_Type)
        Intent_Type: 'B2B_PARTNER', 'PROP_FIRM_LEAD', 'GENERIC', 'NOISE'
        """
        base_score = 0
        text = (title + " " + body).lower()
        intent_type = "GENERIC"

        # 1. Signaux Négatifs & Bruit Bancaire (STRATÉGIE V3)
        # Exclusion du modèle "Courtier" (Crédit, Banque, Épargne réglementée)
        banking_noise = [
            "crédit",
            "prêt",
            "immobilier",
            "residence principale",
            "apport",
            "banque postale",
            "crédit agricole",
            "bnp",
            "société générale",
            "livret a",
            "ldds",
            "pel",
            "assurance vie",
            "frais bancaires",
            "svp",
            "please",
            "help me",
            "arnaque",
            "scam",
            "fake",
        ]

        if any(x in text for x in banking_noise):
            # Exception : Si le texte contient aussi "Trading" ou "Crypto", on garde
            allowed_context = ["trading", "crypto", "bitcoin", "bourse", "etf", "pea", "futures"]
            if not any(ctx in text for ctx in allowed_context):
                return "LOW_TIER", 0, "NOISE"

        # 2. Signaux d'Expertise (Contenu)
        if len(body) > 400:
            base_score += 20
        if "guide" in title or "tutorial" in title or "review" in title:
            base_score += 30
        if "analysis" in text or "vs" in text or "forecast" in text:
            base_score += 20

        # --- INTELLIGENCE V3: INTENT CLASSIFICATION ---

        # A. Cible B2B (Vendeurs de pelles)
        # Ils vendent des formations, signaux, ou ont une communauté.
        b2b_triggers = [
            "my course",
            "discord link",
            "telegram",
            "mentorship",
            "subscribe",
            "mon groupe",
        ]
        if any(x in text for x in b2b_triggers):
            intent_type = "B2B_PARTNER"
            base_score += 10  # Bonus business

        # B. Cible Prop Firm (Chercheurs d'Or)
        # Ils parlent de payout, drawdown, funded account.
        prop_triggers = [
            "payout",
            "withdrawal",
            "funded account",
            "challenge passed",
            "evaluation",
            "ftmo",
            "apex",
            "drawdown",
        ]
        if any(x in text for x in prop_triggers):
            intent_type = "PROP_FIRM_LEAD"
            base_score += 25  # Cible prioritaire V3

        # C. Cible SaaS/Crypto (Zone FR/EU Friendly)
        saas_triggers = [
            "tradingview",
            "ledger",
            "binance",
            "kraken",
            "chart",
            "indicateur",
            "bot",
            "python",
            "api",
        ]
        if any(x in text for x in saas_triggers):
            if intent_type == "GENERIC":  # Ne pas écraser Prop Firm si déjà détecté
                intent_type = "SAAS_CRYPTO_LEAD"
                base_score += 15

        # --- TRIANGULATION D'URGENCE ---
        urgency_score = 0
        urgency_triggers = ["today", "expire", "blown account", "limit", "margin call"]
        if any(x in text for x in urgency_triggers):
            urgency_score += 15

        final_score = base_score + urgency_score

        tier = "LOW_TIER"
        if final_score >= 50:
            tier = "HIGH_TIER"
        elif final_score >= 20:
            tier = "MID_TIER"

        return tier, final_score, intent_type


class PartnerHunter:
    """
    Partner Hunter V11.1 (Global Authority Focus + Banking Filter).
    """

    def __init__(self):
        self.running = True
        self.db = NexusDB()
        self.local_cache = OrderedDict()
        self.CACHE_LIMIT = 15000
        self.salt = NexusDB.PRIVACY_SALT

        self.targets_state = {
            sub: {"last_visit": 0.0, "cooldown": 300, "yield_score": 1.0}
            for sub in TARGET_SUBREDDITS
        }

        self.burst_window = deque()

        self.daily_leads_found = 0
        self.last_quota_reset = time.time()
        self.DAILY_QUOTA_TARGET = 100

    def _anonymize_identity(self, raw_id: str) -> str:
        if not raw_id or raw_id == "unknown":
            return "unknown"
        return self.db._hash_identity(raw_id)

    def _get_hunting_orders(self) -> Dict[str, str]:
        orders = {}
        try:
            if settings.HUNTING_ORDERS_PATH.exists():
                with open(settings.HUNTING_ORDERS_PATH, "r") as f:
                    orders = json.load(f)
        except Exception:
            pass
        return orders

    def _update_quota_metrics(self):
        now = time.time()
        if now - self.last_quota_reset > 86400:
            self.daily_leads_found = 0
            self.last_quota_reset = now

    async def run(self):
        logger.info(f"📡 Partner Hunter Active (Global V11.1). Targets: {len(TARGET_SUBREDDITS)}")

        profile = SandboxCDPProfile(
            user_id="reddit_hunter_global",
            cdp_port=settings.CDP_REDDIT_PORT,
            profile_dir=str(settings.CHROME_PROFILES_DIR / "reddit_master"),
        )

        async with async_playwright() as p:
            if not await profile.connect(p):
                logger.error("❌ Echec connexion CDP Reddit.")
                return

            page = await profile.get_page()
            humanizer = PhysicsHumanizer(page)

            page.on("response", self._handle_gql_response)
            logger.info("📡 Flux GQL connecté.")

            while self.running:
                self._maintain_cache()
                self._update_quota_metrics()

                target, strategy = self._select_next_target()

                if not target:
                    logger.info("⏳ Attente active (Cycles Cool-down)...")
                    await asyncio.sleep(60)
                    continue

                try:
                    self.targets_state[target]["last_visit"] = time.time()

                    # Logic spécifique Prop Firms : On veut du "New" pour sniper les besoins immédiats
                    is_prop_target = target in ["Daytrading", "Forex", "PropFunded"]
                    sort_mode = "new" if (strategy == "AGGRESSIVE" or is_prop_target) else "hot"

                    url = f"https://www.reddit.com/r/{target}/{sort_mode}/"
                    logger.info(f"🔎 Hunting in r/{target} (Mode: {sort_mode})...")

                    await page.goto(url, timeout=60000, wait_until="domcontentloaded")

                    scroll_count = (
                        random.randint(8, 15) if strategy == "AGGRESSIVE" else random.randint(5, 12)
                    )
                    for _ in range(scroll_count):
                        await humanizer.scroll_organic(intensity="high")
                        await asyncio.sleep(random.uniform(2.5, 5.0))

                    await profile.save_checkpoint()

                except Exception as e:
                    logger.warning(f"⚠️ Erreur Navigation r/{target}: {e}")
                    self.targets_state[target]["yield_score"] *= 0.9

                pause = random.randint(45, 120)
                if strategy == "AGGRESSIVE":
                    pause = int(pause * 0.5)

                await asyncio.sleep(pause)

    def _select_next_target(self) -> Tuple[Optional[str], str]:
        now = time.time()
        candidates = []
        orders = self._get_hunting_orders()

        for sub, state in self.targets_state.items():
            order = "NORMAL"
            for key, val in orders.items():
                if key in sub:
                    order = val
                    break

            if order == "STOP":
                continue

            base_cooldown = 300
            score = max(0.1, state.get("yield_score", 1.0))
            if order == "AGGRESSIVE":
                score *= 2.0

            adjusted_cooldown = int(base_cooldown / (0.5 + (score * 0.5)))
            adjusted_cooldown = max(60, adjusted_cooldown)

            if now > (state["last_visit"] + adjusted_cooldown):
                candidates.append((sub, order))

        if not candidates:
            return None, "NORMAL"
        return random.choice(candidates)

    def _maintain_cache(self):
        while len(self.local_cache) > self.CACHE_LIMIT:
            self.local_cache.popitem(last=False)
        now = time.time()
        while self.burst_window and (now - self.burst_window[0].ts > 3600):
            self.burst_window.popleft()

    async def _handle_gql_response(self, response: Response):
        url = response.url
        if "gql" not in url and "gateway" not in url:
            return
        try:
            content_type = response.headers.get("content-type", "")
            if "json" not in content_type:
                return
            if int(response.headers.get("content-length", 0)) > 2_000_000:
                return

            data = await response.json()
            posts = self._extract_posts_recursive(data)

            if posts:
                await self._process_posts_batch(posts)

        except Exception:
            pass

    def _extract_posts_recursive(self, data: Any, depth: int = 0) -> List[Dict]:
        found = []
        if depth > 12:
            return found

        if isinstance(data, dict):
            if "id" in data and "title" in data:
                if not data.get("isSponsored") and not data.get("isAd"):
                    found.append(data)

            for v in data.values():
                if isinstance(v, (dict, list)):
                    found.extend(self._extract_posts_recursive(v, depth + 1))

        elif isinstance(data, list):
            for item in data:
                found.extend(self._extract_posts_recursive(item, depth + 1))
        return found

    async def _process_posts_batch(self, posts: List[Dict]):
        candidates = []
        current_subreddit = ""

        severity_factor = min(1.0, self.daily_leads_found / self.DAILY_QUOTA_TARGET)
        dynamic_threshold = 20 + (40 * severity_factor)

        for post in posts:
            pid = post.get("id")
            if not pid:
                continue

            if not current_subreddit:
                current_subreddit = post.get("subreddit", {}).get("name", "")

            full_id = pid if pid.startswith("t3_") else f"t3_{pid}"
            if full_id in self.local_cache:
                self.local_cache.move_to_end(full_id)
                continue

            author_data = post.get("author", {})
            raw_author_name = (
                author_data.get("name", "unknown") if isinstance(author_data, dict) else "unknown"
            )
            hashed_author = self._anonymize_identity(raw_author_name)

            if hashed_author != "unknown" and hashed_author != "anonymous_node":
                burst_count = sum(1 for x in self.burst_window if x.author_hash == hashed_author)
                if burst_count > 4:
                    continue
                self.burst_window.append(BurstEntry(hashed_author, time.time(), current_subreddit))

            title = post.get("title", "")
            body = (post.get("selftext") or {}).get("markdown") or post.get("body", "")
            if not isinstance(body, str):
                body = ""

            # --- AUTHORITY & INTENT CLASSIFICATION ---
            tier, score, intent_type = AuthorityClassifier.classify(title, body)

            if tier == "LOW_TIER" or intent_type == "NOISE":
                continue
            if score < dynamic_threshold:
                continue

            is_vip = (tier == "HIGH_TIER") or (intent_type == "PROP_FIRM_LEAD")

            candidates.append(
                (full_id, post, title, body, hashed_author, score, is_vip, tier, intent_type)
            )

        if current_subreddit and current_subreddit in self.targets_state:
            found_count = len(candidates)
            current_score = self.targets_state[current_subreddit].get("yield_score", 1.0)

            if found_count > 0:
                new_score = min(10.0, current_score + found_count)
            else:
                new_score = max(0.1, current_score * 0.9)

            self.targets_state[current_subreddit]["yield_score"] = new_score

        if not candidates:
            return

        new_count = 0
        for (
            full_id,
            raw_post,
            title,
            body,
            hashed_author,
            score,
            is_vip,
            tier,
            intent_type,
        ) in candidates:
            permalink = raw_post.get("permalink", "")
            full_url = (
                f"https://www.reddit.com{permalink}" if permalink.startswith("/") else permalink
            )

            telemetry_packet = {
                "id": full_id,
                "source": "reddit_partner_hunter",
                "author": hashed_author,
                "text": f"{title}\n\n{body}",
                "url": full_url,
                "ai_process_info": json.dumps(
                    {
                        "intent": intent_type,
                        "tier": tier,
                        "authority_score": score,
                        "urgency_triangulation": True,
                        "is_vip": is_vip,
                        "subreddit": raw_post.get("subreddit", {}).get("name", ""),
                        "context": "Identified by Global Hunter V11.1",
                    }
                ),
                "created_at": time.time(),
            }

            inserted = self.db.insert_telemetry_signal(telemetry_packet)
            if inserted:
                new_count += 1
                self.local_cache[full_id] = datetime.utcnow()

        if new_count:
            self.daily_leads_found += new_count
            logger.info(
                f"⚡ {new_count} Leads Identifiés (Sub: {current_subreddit}, Type: {candidates[0][8]})."
            )


if __name__ == "__main__":
    try:
        hunter = PartnerHunter()
        asyncio.run(hunter.run())
    except KeyboardInterrupt:
        pass
