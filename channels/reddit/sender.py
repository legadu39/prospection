### prospection/channels/reddit/sender.py
# channels/reddit/sender.py - REDDIT COMMUNITY GATEWAY V28.0 (PERSISTENT MEMORY)
# -*- coding: utf-8 -*-

import asyncio
import random
import sys
import logging
import json
import re
import time
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright, Page

# Imports Config & Core
try:
    sys.path.append(str(Path(__file__).resolve().parents[2]))
    from core.settings import settings
    from core.database import NexusDB
    from core.browser_engine import SandboxCDPProfile, StealthInjector
    from core.time_manager import CircadianCycle
    from core.vision_guardian import VisionGuardian
    from core.humanizer import PhysicsHumanizer
except ImportError:
    sys.exit(1)

# --- CONFIGURATION ---
PAUSE_BETWEEN_ACTIONS = (300, 900)  # 5 à 15 min

# --- COMPLIANCE CONSTANTS (Loi Influenceurs & AMF) ---
# Mention obligatoire pour toute promotion financière.
LEGAL_DISCLAIMER_EN = "\n\n(Ad. Not financial advice. Trading involves risk.)"
LEGAL_DISCLAIMER_FR = "\n\n(Publicité. Ce site ne fournit pas de conseil en investissement financier. Les crypto-actifs sont risqués.)"

# --- ZONES GÉOGRAPHIQUES CIBLES ---
FR_SUBREDDITS_TARGETS = [
    "vosfinances",
    "francefire",
    "cryptomonnaie",
    "finance",
    "investir",
    "immobilier",
    "bouse",
    "francedigitale",
]


# --- DCO ENGINE (Dynamic Creative Optimization) ---
class DCOEngine:
    """
    Moteur d'Optimisation Dynamique de Créa (DCO).
    Génère des variations sémantiques pour maximiser l'engagement et l'unicité du message.
    Supporte les structures A/B testing imbriquées : {OptionA|{SubOptionB|SubOptionC}|OptionD}
    """

    @staticmethod
    def optimize_creative(text: str) -> str:
        if not text:
            return ""
        # Regex pour trouver le motif {a|b|c} le plus profond
        pattern = re.compile(r"\{([^{}]+)\}")
        while True:
            match = pattern.search(text)
            if not match:
                break
            # Sélection algorithmique de la variante (simulée par random pour l'instant)
            options = match.group(1).split("|")
            chosen = random.choice(options)
            # Remplacement dans le texte
            text = text[: match.start()] + chosen + text[match.end() :]
        return text


# Templates Contextuels Reddit (US/GLOBAL - STRATÉGIE V3.0 PROP FIRM)
REDDIT_CAMPAIGNS_US = {
    "NEED": [
        "{Hey|Hi}, if you're looking for {funding|capital} to trade, {I wrote a guide|I have a setup} on how to pass the eval without blowing up. {Check my profile|Link in bio} if interested 📉",
        "{Stop risking|Don't risk} your own money. I {documented|explained} how I got {funded|allocated} with a Prop Firm on my {profile|pinned post}. {✅|🚀}",
        "For those {seeking|looking for} capital, {check|see} my profile. I shared my {strategy|method} to pass the challenge {safely|consistently}.",
    ],
    "QUESTION": [
        "I {made|wrote} a {breakdown|post} on the risk management needed for this, {it's pinned on my profile|check my bio}. Hope it helps! {🧠|💡}",
        "It's mainly about {drawdown|risk} control. I {detailed|shared} the rules I use to keep the account on my profile {pinned post|bio}.",
        "{Check|See} the guide on my profile, I explain how to {handle|manage} the trailing drawdown {effectively|properly}.",
    ],
    "OFFER": [
        "If you need a {discount|promo} for the eval, {I pinned one|check the one} on my profile. Helps with the {initial cost|reset fees}! {📉|💸}",
        "{Valid|Working} reduction code for the challenge is on my {bio|profile} if anyone needs it to {start|save money}.",
    ],
    "TRUST": [
        "It's {legit|reliable}, I've been {funded|trading} with them for months. {Payout proofs|Withdrawals} are on my {profile|bio} if you want to see. {✅|💯}",
        "{Works fine|No issues}, just follow the {rules|consistency rules}. I posted my {experience|journey} on my profile.",
        "{Totally valid|Good firm}. If you want to see how the {payout|profit split} works, check my pinned post.",
    ],
    "DEFAULT": [
        "{Check|Visit} my profile for the {strategy guide|funding tips}, it might {help|be useful}! {✅|🔥}",
        "{Sent|Dropped} you a {message|DM} with the {details|strategy}!",
        "{Info|Guide} is on my {pinned profile post|bio}.",
    ],
}

# Templates Contextuels Reddit (FRANCE/EURO - STRATÉGIE V3.0 SAAS & CRYPTO)
# INTERDICTION FORMELLE DE VENDRE DU PROP FIRM ICI.
REDDIT_CAMPAIGNS_FR = {
    "NEED": [
        "{Salut|Hello}, pour ceux qui cherchent des {outils|charts} pro, j'ai compilé un comparatif {SaaS|TradingView} sur mon profil. {Utile|Pratique} pour éviter les frais inutiles. 📉",
        "Ne laissez pas vos {assets|crypto} sur les exchanges. J'ai fait un tuto sur la sécurisation {Ledger|Cold Storage} épinglé sur mon profil. {✅|🛡️}",
        "Pour l'analyse technique, regardez le {setup|guide} que j'ai mis en bio. C'est basé sur des indicateurs {SaaS|fiables}.",
    ],
    "QUESTION": [
        "J'ai rédigé une {analyse|note} sur la gestion des risques crypto, {c'est épinglé|voir ma bio}. En espérant que ça aide ! {🧠|💡}",
        "Tout est dans la sécurisation des clés. J'ai {détaillé|partagé} les bonnes pratiques PSAN sur mon profil {épinglé|bio}.",
        "{Allez voir|Checkez} le guide sur mon profil, j'explique comment optimiser ses {frais|charts} de trading.",
    ],
    "OFFER": [
        "Si vous cherchez une réduction sur {TradingView|Ledger}, j'en ai mis une sur mon profil. Toujours ça de pris ! {📉|💸}",
        "{Code|Plan} dispo dans ma {bio|description} pour ceux qui veulent s'équiper en outils pro.",
    ],
    "TRUST": [
        "C'est {fiable|régulé}, je l'utilise pour ma sécurisation. Les détails sont sur mon {profil|bio}. {✅|💯}",
        "{Fonctionne bien|Validé}, surtout pour la partie conformité PSAN. J'ai posté mon {retour|avis} sur mon profil.",
        "Si vous voulez voir comment fonctionne l'interface {SaaS|Pro}, checkez mon post épinglé.",
    ],
    "DEFAULT": [
        "{Allez voir|Visitez} mon profil pour le {guide outils|comparatif}, ça peut {aider|servir} ! {✅|🔥}",
        "{Info|Guide} dispo sur mon {post épinglé|bio}.",
    ],
}

# --- LOGGING ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)-8s | REDDIT_GATEWAY | %(message)s"
)
logger = logging.getLogger("RedditGateway")


# --- INTELLIGENCE N°2 : STATE MANAGER PERSISTANT ---
class RedditMemory:
    """Gère la mémoire à long terme des stratégies par Subreddit."""

    FILE_PATH = settings.BASE_DIR / "config" / "reddit_strategy_state.json"

    @staticmethod
    def load():
        if not RedditMemory.FILE_PATH.exists():
            return {}
        try:
            with open(RedditMemory.FILE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    @staticmethod
    def save(data):
        try:
            temp = RedditMemory.FILE_PATH.with_suffix(".tmp")
            with open(temp, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            temp.replace(RedditMemory.FILE_PATH)
        except Exception as e:
            logger.error(f"⚠️ Failed to save strategy state: {e}")


class RedditCommunityGateway:
    def __init__(self):
        self.db = NexusDB()

        self.port = settings.CDP_REDDIT_PORT
        # AUDIT FIX: Utilisation du port failover depuis settings
        self.failover = getattr(settings, "CDP_REDDIT_FAILOVER", 9224)

        self.cdp = SandboxCDPProfile(
            user_id="reddit_gateway", cdp_port=self.port, cdp_secondary_port=self.failover
        )

        self.guardian = VisionGuardian() if "VisionGuardian" in globals() else None
        self.life_cycle = CircadianCycle(start_hour=10, end_hour=2, work_weekends=True)

        # Momentum State (Flow Control)
        self.momentum_score = 1.0
        self.consecutive_success = 0

        # Intelligence : Cache des sélecteurs d'input par Subreddit
        self.selector_cache = {}

        # Intelligence : Persistance de la stratégie (Chargement JSON)
        self.strategy_memory = RedditMemory.load()

    def update_momentum(self, status: str):
        """Ajuste le rythme d'engagement en fonction du succès."""
        if status == "SUCCESS":
            self.consecutive_success += 1
            if self.consecutive_success > 3:
                self.momentum_score = max(0.5, self.momentum_score * 0.9)
                logger.info(
                    f"🚀 Flow State: Optimisation Vélocité (Score: {self.momentum_score:.2f})"
                )
        else:
            self.consecutive_success = 0
            self.momentum_score = min(3.0, self.momentum_score * 1.5)
            logger.info(
                f"🐢 Résistance détectée: Modération Vélocité (Score: {self.momentum_score:.2f})"
            )

    def _calculate_pause(self) -> int:
        """
        Calcule une pause intelligente basée sur l'heure (Circadien) et le Momentum.
        """
        # Heuristique de Surchauffe (Safety Cooling)
        if self.consecutive_success > 5:
            logger.info("🔥 HIGH ENGAGEMENT: Pause de refroidissement préventive (20 min).")
            self.consecutive_success = 0  # Reset streak pour éviter boucle
            return 1200  # 20 minutes

        base_min, base_max = PAUSE_BETWEEN_ACTIONS

        hour = datetime.now().hour
        time_factor = 1.0

        if 18 <= hour <= 23:  # Prime Time (USA/EU)
            time_factor = 0.6
        elif 2 <= hour <= 6:  # Nuit Profonde
            time_factor = 3.0

        current_pause = random.randint(base_min, base_max) * self.momentum_score * time_factor
        return max(120, min(int(current_pause), 3600))

    def _get_campaign_creative(self, intent: str, subreddit: str) -> str:
        """
        Sélectionne une créa en fonction de la langue du Subreddit (Geo-Routing V3)
        et évite la répétition sémantique grâce à la mémoire persistante.
        """
        # 1. DÉTECTION GÉOGRAPHIQUE / STRATÉGIQUE
        is_french_target = any(sub in subreddit.lower() for sub in FR_SUBREDDITS_TARGETS)

        if is_french_target:
            campaign_source = REDDIT_CAMPAIGNS_FR
            disclaimer = LEGAL_DISCLAIMER_FR
            logger.info(f"🇫🇷 Cible FR détectée ({subreddit}). Stratégie: SaaS & Crypto.")
        else:
            campaign_source = REDDIT_CAMPAIGNS_US
            disclaimer = LEGAL_DISCLAIMER_EN
            logger.info(f"🇺🇸 Cible Global détectée ({subreddit}). Stratégie: Prop Firm.")

        category = intent if intent in campaign_source else "DEFAULT"

        # 2. INTELLIGENCE DE PERSISTANCE (FSM)
        # Récupère l'historique des stratégies utilisées sur ce subreddit depuis le fichier persistant
        subreddit_history = self.strategy_memory.get(subreddit, [])
        # On ne garde que les 3 dernières
        recent_categories = subreddit_history[-3:] if subreddit_history else []

        # Rotation
        if category in recent_categories and len(recent_categories) > 0:
            logger.info(
                f"🔄 Rotation Contextuelle: {category} déjà utilisé sur {subreddit}. Recherche variation..."
            )
            alternatives = [
                k for k in campaign_source.keys() if k != category and k not in recent_categories
            ]

            if alternatives:
                category = random.choice(alternatives)
            elif category == "DEFAULT":
                category = "TRUST"

        # Mise à jour mémoire et sauvegarde disque
        if subreddit not in self.strategy_memory:
            self.strategy_memory[subreddit] = []
        self.strategy_memory[subreddit].append(category)
        if len(self.strategy_memory[subreddit]) > 10:  # Keep file size managed
            self.strategy_memory[subreddit] = self.strategy_memory[subreddit][-10:]

        RedditMemory.save(self.strategy_memory)

        raw_creative = random.choice(campaign_source[category])

        # 3. DCO & FINALIZATION
        optimized_text = DCOEngine.optimize_creative(raw_creative)
        return optimized_text + disclaimer

    def _get_subreddit_name(self, url: str) -> str:
        try:
            match = re.search(r"reddit\.com/r/([^/]+)", url)
            if match:
                return f"r/{match.group(1).lower()}"
        except Exception:
            pass
        return "unknown"

    async def _get_isolated_page(self) -> Page:
        if not self.cdp.context:
            raise Exception("Contexte navigateur non disponible")

        page = await self.cdp.context.new_page()

        if hasattr(self.cdp, "identity"):
            await StealthInjector.inject(page, self.cdp.identity)
        else:
            logger.warning("⚠️ Identité numérique introuvable, injection Stealth par défaut.")

        return page

    async def analyze_context(self, page, subreddit: str) -> bool:
        logger.info(f"🕵️ Analyse du contexte {subreddit}...")
        try:
            hostile_selectors = [
                "shreddit-post-locked-indicator",
                "div[data-testid='archived-post-banner']",
                "div:has-text('You have been banned')",
            ]

            for sel in hostile_selectors:
                if await page.locator(sel).count() > 0:
                    logger.warning(f"🛑 Contexte non-éligible détecté : {sel}")
                    return False

            return True

        except Exception as e:
            logger.warning(f"⚠️ Erreur analyse contexte: {e}")
            return True

    # --- INTELLIGENCE N°3 : REPUTATION GATE ---
    async def check_author_reputation(self, page: Page, author_name: str) -> bool:
        """
        Analyse silencieusement le profil de l'auteur pour détecter les risques (Honeypot, Bot, Mod).
        Retourne True si l'audience est "Safe" (cible valide).
        """
        if not author_name or author_name == "unknown" or "AutoModerator" in author_name:
            logger.warning("🚫 Audience invalide (Inconnu ou AutoMod).")
            return False

        logger.info(f"🛡️ Audit de réputation pour u/{author_name}...")

        try:
            # Technique "Zero-Click" : Fetch API JSON public
            api_url = f"https://www.reddit.com/user/{author_name}/about.json"

            response = await page.request.get(api_url)

            if response.status != 200:
                logger.warning(
                    f"⚠️ Impossible de récupérer le profil ({response.status}). Prudence."
                )
                return response.status != 404

            data = await response.json()
            user_data = data.get("data", {})

            # 1. Vérification Age du compte
            created_utc = user_data.get("created_utc", time.time())
            account_age_hours = (time.time() - created_utc) / 3600

            if account_age_hours < 24:
                logger.warning(
                    f"🚫 REJET: Compte trop jeune ({account_age_hours:.1f}h). Risque de ban par association."
                )
                return False

            # 2. Vérification Karma (Honeypot vs PowerUser)
            total_karma = user_data.get("total_karma", 0)

            if total_karma < 0:
                logger.warning(f"🚫 REJET: Karma négatif ({total_karma}). Troll potentiel.")
                return False

            if total_karma > 100000:
                logger.warning(
                    f"🚫 REJET: Karma excessif ({total_karma}). Power User/Mod potentiel."
                )
                return False

            # 3. Vérification Statut Modérateur
            if user_data.get("is_mod", False):
                logger.warning("🚫 REJET: L'utilisateur est modérateur.")
                return False

            logger.info(
                f"✅ Audience Validée: u/{author_name} (Age: {int(account_age_hours/24)}j, Karma: {total_karma})"
            )
            return True

        except Exception as e:
            logger.error(f"⚠️ Erreur Audit Réputation: {e}")
            return True  # Fail open

    async def analyze_intent_from_page(self, page) -> str:
        try:
            text_content = ""
            try:
                title_el = page.locator("h1").first
                if await title_el.is_visible():
                    text_content += await title_el.inner_text() + " "
            except Exception:
                pass

            text_lower = text_content.lower()

            if any(
                w in text_lower
                for w in [
                    "code",
                    "link",
                    "referral",
                    "invitation",
                    "sign up",
                    "promo",
                    "lien",
                    "parrainage",
                ]
            ):
                return "NEED"
            if any(
                w in text_lower
                for w in [
                    "how",
                    "work",
                    "guide",
                    "help",
                    "step",
                    "explain",
                    "pass",
                    "fail",
                    "comment",
                    "aide",
                    "tuto",
                ]
            ):
                return "QUESTION"
            if any(
                w in text_lower
                for w in [
                    "scam",
                    "fake",
                    "legit",
                    "safe",
                    "danger",
                    "review",
                    "payout",
                    "arnaque",
                    "avis",
                    "retrait",
                ]
            ):
                return "TRUST"

        except Exception as e:
            logger.debug(f"JIT Intent Analysis failed: {e}")

        return "DEFAULT"

    async def find_semantic_input(self, page, subreddit: str) -> str:
        if subreddit in self.selector_cache:
            cached_sel = self.selector_cache[subreddit]
            try:
                if await page.is_visible(cached_sel):
                    return cached_sel
            except Exception:
                del self.selector_cache[subreddit]

        css_candidates = [
            "shreddit-comment-composer >> textarea",
            "shreddit-comment-composer >> div[contenteditable='true']",
            "div.public-DraftEditor-content",
        ]
        for sel in css_candidates:
            if await page.is_visible(sel):
                self.selector_cache[subreddit] = sel
                return sel

        semantic_candidates = [
            "div[role='textbox']",
            "div[contenteditable='true']",
            "textarea[name='text']",
        ]
        for sel in semantic_candidates:
            if await page.locator(sel).count() > 0:
                el = page.locator(sel).first
                if await el.is_visible() and await el.is_editable():
                    self.selector_cache[subreddit] = sel
                    return sel

        try:
            md_btn = page.locator("button[data-test-id='comment-submission-form-markdown-mode']")
            if await md_btn.is_visible():
                await md_btn.click()
                await asyncio.sleep(1)
                if await page.is_visible("textarea"):
                    self.selector_cache[subreddit] = "textarea"
                    return "textarea"
        except Exception:
            pass

        return None

    async def post_reply(self, lead: dict) -> bool:
        logger.info(f"👽 Traitement lead {lead['id']}...")
        page = None
        subreddit = self._get_subreddit_name(lead.get("url", ""))

        try:
            page = await self._get_isolated_page()
            physics = PhysicsHumanizer(page)

            await page.goto(lead["url"], wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(random.uniform(4, 7))

            if not await self.analyze_context(page, subreddit):
                self.db.update_subreddit_stats(subreddit, success=False)
                return False

            # --- INTELLIGENCE N°3 : AUDIT DE L'AUDIENCE ---
            author_name = lead.get("author")
            is_safe = await self.check_author_reputation(page, author_name)
            if not is_safe:
                return False
            # --------------------------------------------

            intent = "DEFAULT"
            try:
                if lead.get("ai_process_info"):
                    info = json.loads(lead["ai_process_info"])
                    intent = info.get("intent_category", "DEFAULT")
                elif lead.get("intent"):
                    intent = lead.get("intent")
            except Exception:
                pass

            if intent == "DEFAULT":
                intent = await self.analyze_intent_from_page(page)
                logger.info(f"🧠 JIT Intent déduit: {intent}")

            # AUDIT FIX: Appel de la méthode mise à jour avec contexte géographique
            creative_text = self._get_campaign_creative(intent, subreddit)
            logger.info(f"🖋️ Génération DCO (Geo-Aligned): {creative_text[:50]}...")

            target_sel = await self.find_semantic_input(page, subreddit)

            if not target_sel:
                logger.warning(f"❌ Editeur introuvable sur {subreddit}.")
                self.db.update_subreddit_stats(subreddit, success=False)
                return False

            await physics.human_type(target_sel, creative_text)
            await asyncio.sleep(random.uniform(2, 4))

            submit_btn = None

            try:
                btn_candidate = page.locator("shreddit-comment-composer").get_by_role(
                    "button", name="Comment"
                )
                if await btn_candidate.count() > 0 and await btn_candidate.is_visible():
                    submit_btn = btn_candidate
            except Exception:
                pass

            if not submit_btn:
                generic_selectors = [
                    "button[type='submit']",
                    "button:has-text('Comment')",
                    "div[role='button']:has-text('Comment')",
                    "button:has-text('Reply')",
                ]
                for sel in generic_selectors:
                    try:
                        btn_candidate = page.locator(sel).last
                        if await btn_candidate.is_visible():
                            submit_btn = btn_candidate
                            break
                    except Exception:
                        continue

            if submit_btn and await submit_btn.is_visible():
                logger.info("🚀 Envoi de l'engagement...")
                await physics.move_mouse_organic(str(submit_btn))

                # --- DÉTECTION FIABLE PAR LISTENER RÉSEAU ---
                # On enregistre le listener AVANT le clic pour ne rater aucune réponse.
                # Patterns couvrant l'API commentaire Reddit (old + new shreddit + GraphQL).
                def _is_comment_api_response(response) -> bool:
                    url = response.url
                    return any(
                        pat in url
                        for pat in [
                            "/api/comment",
                            "/svc/shreddit/graphql",
                            "gateway.reddit.com",
                        ]
                    )

                success = False
                try:
                    async with page.expect_response(
                        _is_comment_api_response,
                        timeout=10_000,
                    ) as response_info:
                        await submit_btn.click()

                    response = await response_info.value
                    success = response.status == 200
                    logger.info(
                        f"{'✅' if success else '❌'} Confirmation réseau : "
                        f"HTTP {response.status} ({response.url[:80]})"
                    )
                except Exception as net_err:
                    # Fallback : le listener a timeout ou Reddit n'a pas matché —
                    # on revient à la vérification du champ texte, moins fiable
                    # mais meilleure que rien.
                    logger.warning(
                        f"⚠️ Listener réseau indisponible ({net_err}). "
                        "Fallback sur vérification du champ texte."
                    )
                    await asyncio.sleep(5)
                    try:
                        input_loc = page.locator(target_sel)
                        field_visible = await input_loc.is_visible()
                        field_empty = (
                            (await input_loc.input_value()) == "" if field_visible else True
                        )
                        success = not field_visible or field_empty
                    except Exception:
                        success = False

                self.db.update_subreddit_stats(subreddit, success=success)
                return True
            else:
                logger.error("❌ Bouton d'envoi introuvable.")
                self.db.update_subreddit_stats(subreddit, success=False)
                return False

        except Exception as e:
            logger.error(f"❌ Crash Lead {lead['id']}: {e}")
            if self.guardian and page:
                await self.guardian.handle_crash(
                    page, context="RedditGatewayError", error_msg=str(e)
                )
            self.db.update_subreddit_stats(subreddit, success=False)
            return False
        finally:
            if page:
                try:
                    await page.close()
                except Exception:
                    pass

    async def run(self):
        logger.info("🚀 REDDIT COMMUNITY GATEWAY INITIALIZED (GEO-ROUTING ENABLED).")
        loop = asyncio.get_running_loop()

        async with async_playwright() as p:
            if not await self.cdp.connect(p):
                logger.critical("❌ Impossible de connecter le CDP Reddit.")
                return

            while True:
                if not await self.cdp.ensure_connected(p):
                    logger.warning("⚠️ Connexion browser perdue. Tentative de reconnexion...")
                    await asyncio.sleep(10)
                    continue

                await self.life_cycle.check_schedule_and_sleep_if_needed()

                lead = await loop.run_in_executor(
                    None, self.db.get_next_lead_to_send_by_source, "reddit"
                )

                if lead:
                    subreddit = self._get_subreddit_name(lead.get("url", ""))
                    stats = self.db.get_subreddit_stats(subreddit)

                    total_attempts = stats["success_count"] + stats["fail_count"]
                    fail_rate = stats["fail_count"] / total_attempts if total_attempts > 0 else 0.0

                    if total_attempts > 5 and fail_rate > 0.4:
                        logger.warning(
                            f"🚫 SKIP {subreddit} (Taux d'échec {fail_rate:.1%}). Zone à faible réceptivité."
                        )
                        await loop.run_in_executor(
                            None, self.db.fail_lead, lead["id"], "SKIPPED_HOSTILE"
                        )
                        continue

                    success = await self.post_reply(lead)

                    if success:
                        self.update_momentum("SUCCESS")
                        await loop.run_in_executor(None, self.db.mark_lead_sent, lead["id"])

                        pause = self._calculate_pause()
                        logger.info(f"✅ Engagement réussi. Pause intelligente {int(pause/60)} min.")
                        await asyncio.sleep(pause)
                    else:
                        self.update_momentum("ERROR")
                        await loop.run_in_executor(
                            None, self.db.fail_lead, lead["id"], "SEND_ABORTED"
                        )
                        await asyncio.sleep(60)
                else:
                    await asyncio.sleep(30)


if __name__ == "__main__":
    try:
        asyncio.run(RedditCommunityGateway().run())
    except KeyboardInterrupt:
        logger.info("🛑 Arrêt Gateway.")
