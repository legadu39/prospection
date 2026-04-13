### prospection/channels/tiktok/sender.py
# channels/tiktok/sender.py - TELEMETRY INJECTOR V41.2 (VALUE-FIRST EDITION)
# -*- coding: utf-8 -*-

"""
TIKTOK TELEMETRY INJECTOR V41.2 - COMPLIANCE & SAFETY
----------------------------------------------------
1. Telemetry Injection: Delivers informational payloads to qualified nodes.
2. Compliance Check: Adds mandatory legal mentions [Ad] to all payloads.
3. Network Context Awareness: Detects hostility (firewalls) vs noise.
4. Auto-Healing: DOM Shake strategy for robust element interaction.
5. Flow State Momentum: Dynamic adjustment of transmission rate.
6. Packet Sanitization: Payload cleanup before injection.
7. Intent Mapping: Selection of protocol signature based on node topology.

V3.2 UPDATE:
- "Value First" Pivot: Payloads now focus on Education (Guide) over Sales.
- Enhanced stealth for high-velocity environments.
- GEO-ALIGNMENT: FR vectors now strictly target SaaS/Crypto (No Prop Firm).
"""

import asyncio
import random
import time
import sys
import json
import logging
import signal
import re
from pathlib import Path
from typing import Optional, Union
from playwright.async_api import async_playwright, Page

# AUDIT FIX: Robust Imports
try:
    from core.database import NexusDB
    from core.browser_engine import SandboxCDPProfile, StealthInjector
    from core.time_manager import CircadianCycle
    from core.humanizer import PhysicsHumanizer
    from core.settings import settings
except ImportError:
    sys.path.append(str(Path(__file__).resolve().parents[2]))
    from core.database import NexusDB
    from core.browser_engine import SandboxCDPProfile, StealthInjector
    from core.time_manager import CircadianCycle
    from core.humanizer import PhysicsHumanizer
    from core.settings import settings

# --- CONFIGURATIONS ---
BASE_PAUSE_INTRA_BURST = (30, 180)   
BASE_PAUSE_INTER_BURST = (2700, 7200) 

# Updated to catch English and French hostility (Global Safety)
HARD_BANS = ["arnaque", "scam", "voleur", "thief", "fraud", "fake", "ponzi"]
SOFT_WARNINGS = ["fake", "bot", "faux", "signalé", "report", "police", "scammer", "spam"]

RESTRICTION_INDICATORS = [
    "Comments are turned off", 
    "Les commentaires sont désactivés",
    "Only friends can comment",
    "Seuls les amis peuvent commenter",
    "Creator turned off commenting",
    "L'auteur a désactivé les commentaires",
    "You cannot comment"
]

SELECTORS = {
    "likes": '[data-e2e="like-count"]',
    "comment_icon": [
        '[data-e2e="comment-icon"]',
        'span:has-text("Comment")',
        'div[class*="DivActionItemContainer"]'
    ],
    "input_area": '[data-e2e="comment-input"]',
    "input_fallback": 'div[contenteditable="true"]:visible',
    "send_button": [
        '[data-e2e="comment-post"]',
        'div[data-e2e="comment-post"]',
        'text="Post"',
        'text="Publier"'
    ],
    "send_fallback": 'div[class*="DivPostButton"]',
    "login_modal": ['[data-e2e="login-modal"]', 'text="Log in"', 'text="Connexion"']
}

# --- LEGAL & COPYWRITING UPDATE V3.2 (VALUE FIRST) ---
# EN: Educational Hook (Guide/PDF) -> Prop Firms Allowed
# FR: Educational Hook (Guide/Tuto) -> SaaS/Crypto ONLY (Compliance)

PAYLOAD_VECTORS_EN = {
    "PROP_FIRM_PROTOCOL": [
        "I wrote a free guide on how to pass the Prop Firm challenge without risking your own capital. Link in bio 📘 [Ad]",
        "Stop paying for resets. My strategy PDF explains how to manage the drawdown. It's in my profile 🧠 [Ad]",
        "Funding is easy, keeping it is hard. Grab my risk management template in bio (Free) 📉 [Sponsor]",
        "Don't trade with your savings. Check the 'Funded Trader Blueprint' in my bio 🚀 [Ad]"
    ],
    "SAAS_TOOLS_SIGNAL": [
        "My full charting setup and indicators are detailed in the guide (Link in bio) 📊 [Ad]",
        "Stop guessing. I shared my technical analysis checklist on my profile 🔗 [Info]",
        "To optimize your entries, check the tools I use. Full list in bio 👇 [Partner]",
        "Data is key. I listed the best professional feeds in my bio link ✅ [Ad]"
    ],
    "SECURITY_PROTOCOL": [
        "Not your keys, not your coins. The guide on Self-Custody is in my bio 🛡️ [Ad]",
        "Protect your assets. I made a tutorial on how to use hardware wallets properly. Link in bio ✅ [Advice]",
        "Sleep safe. Secure your crypto with the method in my bio 👇 [Partner]"
    ],
    "DEFAULT_BEACON": [
        "I compiled all the best trading resources in one list. Check bio 👇 [Ad]",
        "Free educational resources on my profile for serious traders 🔗 [Info]",
        "Gear up with the right tools. I shared my stack in the bio 👇 [Partner]"
    ],
    "LOW_LATENCY_PING": [
        "Free Guide in bio! 🔗 [Ad]",
        "Resources in bio 👇 [Info]",
        "Strategy PDF in bio ✅ [Partner]",
        "Check profile 👆 [Ad]"
    ]
}

# AUDIT CORRECTION: Suppression des offres Prop Firm pour le marché FR (Focus SaaS/Crypto)
PAYLOAD_VECTORS_FR = {
    "PROP_FIRM_PROTOCOL": [
        # Redirection vers Outils/Formation pour éviter la dissonance stratégique et risque AMF
        "Pour réussir en trading, il faut les bons outils. J'ai listé mon setup pro (TradingView) en bio 📊 [Partenaire]",
        "La gestion du risque est la clé. Télécharge mon guide de sécurisation crypto sur mon profil 🛡️ [Conseil]",
        "Arrête de trader au hasard. Utilise des outils d'analyse pro. Lien en bio 👇 [Outils]"
    ],
    "SAAS_TOOLS_SIGNAL": [
        "Mon setup graphique complet est détaillé dans le guide (Lien en bio) 📊 [Pub]",
        "Arrête de deviner. J'ai partagé ma checklist d'analyse technique sur mon profil 🔗 [Info]",
        "Pour optimiser tes entrées, regarde les outils que j'utilise. Liste en bio 👇 [Partenaire]"
    ],
    "SECURITY_PROTOCOL": [
        "Pas vos clés, pas vos coins. Le tuto Sécurité Crypto est en bio 🛡️ [Pub]",
        "Protégez vos actifs. J'ai fait un guide sur les wallets froids (Ledger). Lien en bio ✅ [Conseil]",
        "Dormez tranquille. Sécurisez tout avec la méthode en bio 👇 [Partenaire]"
    ],
    "DEFAULT_BEACON": [
        "J'ai compilé toutes les meilleures ressources trading. Voir bio 👇 [Pub]",
        "Ressources éducatives gratuites sur mon profil 🔗 [Info]",
        "Équipez-vous avec les bons outils. J'ai tout listé en bio 👇 [Partenaire]"
    ],
    "LOW_LATENCY_PING": [
        "Guide Gratuit en bio ! 🔗 [Pub]",
        "Ressources en bio 👇 [Info]",
        "PDF Stratégie en bio ✅ [Partenaire]"
    ]
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s | [TELEMETRY] %(message)s")
logger = logging.getLogger("TelemetryInjector")

class TelemetryInjector:
    def __init__(self):
        self.db = NexusDB()
        self.cdp = SandboxCDPProfile(
            cdp_port=settings.CDP_TIKTOK_PORT, 
            user_id="tiktok_sender",
            profile_dir=str(settings.CHROME_PROFILES_DIR / "tiktok_sender")
        )
        self.life_cycle = CircadianCycle(start_hour=9, end_hour=22, work_weekends=True)
        self.running = True

        self.momentum_score = 1.0 
        self.consecutive_success = 0
        
        self.actions_in_current_burst = 0
        self.target_burst_size = random.randint(3, 6)

        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, *args):
        logger.info("🛑 Arrêt demandé (Graceful Shutdown)...")
        self.running = False

    def update_momentum(self, status: str):
        if status == "SUCCESS":
            self.consecutive_success += 1
            if self.consecutive_success > 3:
                self.momentum_score = max(0.5, self.momentum_score * 0.9)
                logger.info(f"🚀 Flow State: Accélération du débit (Score: {self.momentum_score:.2f})")
        
        elif status == "SKIPPED_RESTRICTED":
            logger.info("⏩ Skipped (ACL Restriction): Momentum maintenu.")
            pass

        elif status in ["ERROR", "DOM_SHAKE_NEEDED", "POST_FAILED", "LOGIN_REQUIRED"]:
            self.consecutive_success = 0
            self.momentum_score = min(3.0, self.momentum_score * 1.5)
            logger.info(f"🐢 Résistance détectée ({status}): Ralentissement (Score: {self.momentum_score:.2f})")

    def _calculate_dynamic_pause(self) -> int:
        self.actions_in_current_burst += 1
        
        if self.actions_in_current_burst < self.target_burst_size:
            base_min, base_max = BASE_PAUSE_INTRA_BURST
            current_pause = random.randint(base_min, base_max) * self.momentum_score
            
            if self.consecutive_success > 5 and self.momentum_score < 0.8:
                 current_pause *= 0.7 
                 logger.info("🔥 HOT STREAK: Mode Turbo activé.")

            final_pause = max(20, min(int(current_pause), 300))
            logger.info(f"⚡ Mode Rafale ({self.actions_in_current_burst}/{self.target_burst_size}) : Pause de {final_pause}s")
            return final_pause
        
        else:
            base_min, base_max = BASE_PAUSE_INTER_BURST
            long_pause = random.randint(base_min, base_max)
            
            self.actions_in_current_burst = 0
            self.target_burst_size = random.randint(3, 7)
            
            logger.info(f"💤 Fin de session (Burst complet). Pause de {int(long_pause/60)} min.")
            return long_pause

    def _sanitize_payload(self, text: str) -> str:
        if not text: return ""
        # Ensures legal tags are not stripped
        clean = re.sub(r'[^\w\s.,!?:;@\'"()\[\]-]', '', text) 
        return clean.strip()
    
    def _detect_language_context(self, context_text: str, tags: list = []) -> str:
        """
        Smart heuristic to switch between FR and EN vectors.
        Uses Text Body AND Hashtags.
        """
        fr_markers = ["le", "la", "et", "est", "pas", "pour", "une", "dans", "avec", "bonjour", "merci"]
        fr_tags = ["france", "fr", "paris", "quebec", "belgique", "bourse", "financefr"]
        
        words = context_text.lower().split()
        score_fr = sum(1 for w in words if w in fr_markers)
        
        # Check tags
        tag_hit = any(t in str(tags).lower() for t in fr_tags)
        
        # If > 10% of words are FR common words OR explicit tag found
        if tag_hit or (len(words) > 5 and (score_fr / len(words)) > 0.1):
            return "FR"
        return "EN"

    def _get_protocol_template(self, intent_category: str, lang: str = "EN", is_viral: bool = False) -> str:
        vector_db = PAYLOAD_VECTORS_FR if lang == "FR" else PAYLOAD_VECTORS_EN
        
        if is_viral:
            return random.choice(vector_db["LOW_LATENCY_PING"])

        # V3 Intent Mapping
        intent_map = {
            "prop_firm": "PROP_FIRM_PROTOCOL",
            "tradingview": "SAAS_TOOLS_SIGNAL",
            "ledger": "SECURITY_PROTOCOL",
            "meria": "SECURITY_PROTOCOL",
            "binance": "SECURITY_PROTOCOL"
        }
        
        cat = intent_map.get(intent_category, "DEFAULT_BEACON")
        options = vector_db.get(cat, vector_db["DEFAULT_BEACON"])
        return random.choice(options)

    async def _get_isolated_page(self) -> Page:
        if not self.cdp.context:
            raise RuntimeError("Contexte navigateur non disponible")
        
        page = await self.cdp.context.new_page()
        
        if hasattr(self.cdp, 'identity'):
            await StealthInjector.inject(page, self.cdp.identity)
        else:
            logger.warning("⚠️ Identité numérique introuvable, injection Stealth par défaut.")
        
        return page

    async def check_uplink_health(self, page: Page) -> bool:
        username = getattr(settings, "TIKTOK_USERNAME", None)
        if not username:
            return True 

        logger.info(f"🩺 UPLINK CHECK: Vérification du lien pour @{username}...")
        try:
            await page.goto(f"https://www.tiktok.com/@{username}", timeout=30000)
            await asyncio.sleep(random.uniform(2, 4))
            
            link_sel = '[data-e2e="user-link"]'
            
            if await page.is_visible(link_sel):
                href = await page.get_attribute(link_sel, "href")
                if href and len(href) > 5:
                    logger.info(f"✅ UPLINK SAIN : Lien détecté ({href[:30]}...)")
                    return True
                else:
                    logger.error("❌ UPLINK CORROMPU : Élément présent mais href vide.")
                    return False
            else:
                logger.critical("🚨 ALERTE CRITIQUE : Aucun lien bio visible ! (Shadowban ?)")
                return False

        except Exception as e:
            logger.error(f"⚠️ Erreur lors du Check Uplink: {e}")
            return True 

    async def analyze_environment(self, page: Page, outgoing_payload: str) -> bool:
        logger.info("🕵️ Environmental Scan & Firewall Check...")
        
        try:
            content_buffer = ""
            targets = ['[data-e2e="video-desc"]', '[data-e2e="comment-level-1"]']
            
            for selector in targets:
                try:
                    elements = await page.locator(selector).all_inner_texts()
                    content_buffer += " ".join(elements[:8]).lower() + " "
                except: pass

            full_context = content_buffer + " " + outgoing_payload.lower()
            
            if any(bad in full_context for bad in HARD_BANS):
                logger.warning("☢️ TOXICITY CONFIRMED (Hard Ban). Abort.")
                return False

            for soft in SOFT_WARNINGS:
                if soft in full_context:
                    accusations = [
                        rf"{soft}\s+(ce|le|un|is|a)?\s*(bot|compte|profil|account)",
                        rf"(signalez|report)\s+{soft}",
                        rf"(it's|c'est)\s+(a|du)\s+{soft}"
                    ]
                    
                    for pattern in accusations:
                        if re.search(pattern, full_context):
                            logger.warning(f"⚠️ Contextual Toxicity Detected ({pattern}). Abort.")
                            return False

            return True

        except Exception as e:
            return True

    async def _shake_dom(self, page: Page):
        logger.info("🌪️ SHAKE STRATEGY ACTIVATED (Attempting DOM Wake-up)...")
        try:
            self.update_momentum("DOM_SHAKE_NEEDED")
            await page.mouse.wheel(0, 150)
            await asyncio.sleep(0.3)
            await page.mouse.wheel(0, -150)
            
            vp = page.viewport_size
            if vp:
                await page.set_viewport_size({"width": vp["width"], "height": vp["height"] + 1})
                await asyncio.sleep(0.1)
                await page.set_viewport_size({"width": vp["width"], "height": vp["height"]})
            
            await page.evaluate("document.body.focus()")
            await asyncio.sleep(0.5)
            await page.mouse.move(random.randint(10, 50), random.randint(10, 50))
        except Exception as e:
            logger.warning(f"⚠️ Shake failed: {e}")

    async def inject_packet(self, node_data: dict) -> Union[bool, str]:
        page = None
        try:
            page = await self._get_isolated_page()
            
            logger.info("🏠 Initialisation Session (Home)...")
            try:
                await page.goto("https://www.tiktok.com/", timeout=30000)
                await asyncio.sleep(random.uniform(2, 5))
            except Exception: pass

            logger.info(f"👻 Infiltration Node: {node_data['url']}")
            
            response = await page.goto(node_data['url'], timeout=45000)
            if not response or not response.ok:
                logger.warning(f"❌ Node unreachable (Status {response.status if response else 'Null'})")
                return False

            await asyncio.sleep(random.uniform(5, 8))

            is_viral = False
            try:
                raw_likes = await page.get_attribute(SELECTORS["likes"], "text")
                if raw_likes:
                    if 'K' in raw_likes.upper() or 'M' in raw_likes.upper():
                         is_viral = True
                         logger.info(f"🔥 HIGH DENSITY NODE detected. Punchy Mode activated.")
            except: pass 

            # Resolve Intent
            intent = "DEFAULT"
            try:
                if node_data.get('ai_process_info'):
                    info = json.loads(node_data['ai_process_info'])
                    intent = info.get('suggested_program') or info.get('intent') or 'DEFAULT'
                elif node_data.get('intent'):
                    intent = node_data['intent']
            except: pass
            
            # --- LANGUAGE DETECTION START ---
            page_content = ""
            try:
                page_content += await page.inner_text('[data-e2e="video-desc"]')
                comments = await page.locator('[data-e2e="comment-level-1"]').all_inner_texts()
                page_content += " ".join(comments[:5])
            except: pass
            
            # Use Hashtags from DB if available
            tags = node_data.get('tags', [])
            
            lang_context = self._detect_language_context(page_content, tags)
            logger.info(f"🌍 Detected Context Language: {lang_context}")
            # --- LANGUAGE DETECTION END ---

            raw_payload = ""
            if node_data.get('ai_draft') and len(node_data['ai_draft']) > 10:
                raw_payload = node_data['ai_draft']
                # Safety append if AI forgot legal tags
                legal_tags = ["[Ad]", "[Partner]", "[Pub]", "[Sponsor]"]
                if not any(tag in raw_payload for tag in legal_tags):
                     raw_payload = ("[Ad] " if lang_context == "EN" else "[Pub] ") + raw_payload
                logger.info(f"🤖 Utilisation du brouillon GEMINI unique.")
            else:
                raw_payload = self._get_protocol_template(intent, lang=lang_context, is_viral=is_viral)
            
            payload = self._sanitize_payload(raw_payload)
            if not payload:
                return False

            if not await self.analyze_environment(page, payload):
                return False
            
            physics = PhysicsHumanizer(page) 

            for sel in SELECTORS["comment_icon"]:
                if await page.is_visible(sel):
                    await physics.move_mouse_organic(sel)
                    await page.click(sel)
                    await asyncio.sleep(2)
                    break
            
            input_sel = SELECTORS["input_area"]
            if not await page.is_visible(input_sel):
                input_sel = SELECTORS["input_fallback"]
            
            if await page.is_visible(input_sel):
                logger.info("✍️ Preparing Packet Injection...")
                
                await page.click(input_sel)
                await asyncio.sleep(random.uniform(0.5, 1.5))
                
                await physics.human_type(input_sel, payload)
                await asyncio.sleep(random.uniform(2, 4))

                send_btn = None
                await page.mouse.move(random.randint(100, 500), random.randint(100, 500))
                
                for sel in SELECTORS["send_button"]:
                    if await page.is_visible(sel):
                        send_btn = sel
                        break
                
                if not send_btn:
                    logger.warning("⚠️ Send Button invisible. Launching Shake Strategy...")
                    await self._shake_dom(page)
                    await asyncio.sleep(1.5) 
                    
                    for sel in SELECTORS["send_button"]:
                        if await page.is_visible(sel):
                            send_btn = sel
                            break
                    
                    if not send_btn:
                         if await page.is_visible(SELECTORS["send_fallback"]):
                             send_btn = SELECTORS["send_fallback"]

                if send_btn:
                    await physics.move_mouse_organic(send_btn)
                    await page.click(send_btn)
                    logger.info("🚀 PACKET INJECTED (Click executed).")
                    
                    sent_snippet = payload[:15]
                    try:
                        await page.get_by_text(sent_snippet).first.wait_for(state="visible", timeout=10000)
                        logger.info("✅ Visual Confirmation: Packet published and visible.")
                        return True
                    except Exception:
                        logger.warning("⚠️ Packet not detected after 10s. Assuming SUCCESS.")
                        return True
                else:
                    return False
            else:
                content = await page.content()
                is_restricted = False
                for pattern in RESTRICTION_INDICATORS:
                    if pattern in content:
                        is_restricted = True
                        break
                
                if is_restricted:
                    logger.info("🚫 Logical Block: Comments restricted by author (ACL).")
                    return "SKIPPED_RESTRICTED"

                for ind in SELECTORS["login_modal"]:
                    if await page.is_visible(ind):
                        logger.critical("🛑 SESSION LOST (Login Modal).")
                        raise RuntimeError("LOGIN_REQUIRED")
                
                return False

        except RuntimeError as rt_err:
            raise rt_err
        except Exception as e:
            logger.error(f"❌ Technical Crash inject_packet: {e}")
            return False
        finally:
            if page:
                try: await page.close()
                except: pass

    async def run(self):
        logger.info("🌑 TELEMETRY INJECTOR STARTED (Isolated, Stealth & Compliance Mode).")
        
        async with async_playwright() as p:
            if not await self.cdp.connect(p): 
                logger.critical("❌ Impossible to join browser (Port closed?).")
                return
            
            try:
                startup_page = await self._get_isolated_page()
                is_healthy = await self.check_uplink_health(startup_page)
                await startup_page.close()
                
                if not is_healthy:
                    logger.critical("🚨 STOP: Uplink (Bio) is invalid. Check Account Status.")
                    return
            except Exception as e:
                logger.warning(f"⚠️ Startup Check Failed: {e}")

            loop = asyncio.get_running_loop()
            
            while self.running:
                try:
                    await self.life_cycle.check_schedule_and_sleep_if_needed()
                    await self.life_cycle.take_coffee_break()

                    if not await self.cdp.ensure_connected(p):
                        logger.warning("⚠️ Connection lost. Reconnecting...")
                        await asyncio.sleep(10)
                        continue

                    lead = await loop.run_in_executor(
                        None, 
                        self.db.get_next_lead_to_send_by_source, 
                        "tiktok"
                    )
                    
                    if lead:
                        logger.info(f"🎯 Target Acquired: {lead['id']}")
                        try:
                            result = await self.inject_packet(lead)
                            
                            if result == True:
                                await loop.run_in_executor(None, self.db.mark_lead_sent, lead['id'])
                                self.update_momentum("SUCCESS")
                                
                                pause = self._calculate_dynamic_pause()
                                
                                end_pause = time.time() + pause
                                while time.time() < end_pause and self.running:
                                    await asyncio.sleep(5)

                            elif result == "SKIPPED_RESTRICTED":
                                await loop.run_in_executor(None, self.db.fail_lead, lead['id'], "RESTRICTED")
                                self.update_momentum("SKIPPED_RESTRICTED")
                                await asyncio.sleep(random.randint(5, 15))

                            else:
                                self.update_momentum("POST_FAILED")
                                await loop.run_in_executor(None, self.db.fail_lead, lead['id'], "POST_FAILED")
                                await asyncio.sleep(random.randint(60, 180))

                        except RuntimeError as fatal_error:
                            if "LOGIN_REQUIRED" in str(fatal_error):
                                logger.critical("🛑 EMERGENCY STOP: LOGIN REQUIRED.")
                                await loop.run_in_executor(None, self.db.fail_lead, lead['id'], "LOGIN_REQUIRED")
                                self.running = False
                                break
                            else:
                                logger.error(f"Fatal Error: {fatal_error}")
                                self.update_momentum("ERROR")
                                await asyncio.sleep(60)

                    else:
                        logger.debug("💤 No active orders. Standby 30s.")
                        await asyncio.sleep(30)
                
                except Exception as e:
                    logger.error(f"🔥 Main Loop Error: {e}")
                    self.update_momentum("ERROR")
                    await asyncio.sleep(30)

if __name__ == "__main__":
    injector = TelemetryInjector()
    try:
        asyncio.run(injector.run())
    except KeyboardInterrupt: pass