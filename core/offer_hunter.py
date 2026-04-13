# prospection/core/offer_hunter.py - MARKET OFFER SCANNER V3.0 (GLOBAL PROP FIRM EDITION)
# -*- coding: utf-8 -*-
# MODULE DE VEILLE CONCURRENTIELLE & YIELD DATA
# INTELLIGENCE V3.0 :
# - Focus: Prop Firm Flash Sales (Discount Codes) & SaaS Trials
# - Removed: Bank Bonus Hunting (IOBSP Risk)
# - New Metric: Discount Percentage vs CPA Yield

import asyncio
import re
import logging
import sys
import time
import json
import statistics
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Set, Deque
from collections import deque

# Imports tiers
from playwright.async_api import async_playwright

# Config & Imports Safe
try:
    from core.database import NexusDB
except ImportError:
    # Fallback pour exécution directe
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from core.database import NexusDB

# Config
BASE_DIR = Path(__file__).resolve().parents[1]
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
SPONSORS_FILE = BASE_DIR / "config" / "sponsors.json"
MEMORY_FILE = BASE_DIR / "core" / "scanner_memory.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | [PropFirmScanner] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "market_scanner.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("MarketScanner")

class MarketOfferScanner:
    """
    Scanne les offres publiques (Prop Firms & SaaS) pour alimenter le moteur de Yield.
    V3.0 : Détecte les "Reset Sales" (-80%, -90%) et les extensions de trial SaaS.
    """
    def __init__(self):
        self.db = NexusDB()
        self.memory = self._load_memory()
        self.sponsors_config = self._load_sponsors_config()
        self.priority_queue: Deque[str] = deque() # File d'attente pour scans urgents
        
        # Intelligence N°3 : Matrice de Concurrence Global (Prop Firms)
        # Si un acteur lance une promo, les autres réagissent souvent dans les 24h.
        self.COMPETITORS_MAP = {
            "apex_trader_funding": ["topstep", "myfundedfx", "takepropfit"],
            "topstep": ["apex_trader_funding", "ftmo"],
            "ftmo": ["the5ers", "topstep"],
            "tradingview_saas": ["trendspider", "gocharting"],
            "ledger_hardware": ["trezor", "tangem"]
        }
        
        # Regex Extraction : Focus sur les % de réduction (Promos Prop Firm) et les montants $
        self.regex_discount = re.compile(r"(\d{1,2})\s?%\s*(?:OFF|DE RABAIS|DE REMISE|DISCOUNT|SALE)", re.IGNORECASE)
        self.regex_code = re.compile(r"CODE\s?:\s*([A-Z0-9]{3,15})", re.IGNORECASE)
        # Regex Période Validité (Dates)
        self.regex_date = re.compile(r"(?:ends|fin|until|au)\s+(\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?)", re.IGNORECASE)

    def _load_sponsors_config(self) -> Dict[str, Any]:
        """Charge la configuration dynamique des sponsors depuis JSON."""
        config_map = {}
        if SPONSORS_FILE.exists():
            try:
                with open(SPONSORS_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # Supporte structure liste ou dict (mandates)
                    items = data.get("mandates", {}).values() if isinstance(data, dict) and "mandates" in data else data
                    
                    for sponsor in items:
                        # Adaptation V3 : On utilise l'ID ou le nom technique comme clé
                        program_key = sponsor.get("id") or sponsor.get("sponsor_name", "").lower().replace(" ", "_")
                        
                        if sponsor.get("active"):
                            # On construit une config de chasse par défaut si absente
                            config_map[program_key] = {
                                "endpoint": sponsor.get("target_url"),
                                "standard_offer": 0, # Pas de prime cash standard, c'est du discount
                                "min_threshold": 10, # 10% min discount
                                "max_threshold": 95  # 95% max discount
                            }
                            
                logger.info(f"📋 Configuration chargée pour {len(config_map)} partenaires actifs.")
                return config_map
            except Exception as e:
                logger.error(f"❌ Erreur critique chargement sponsors.json : {e}")
                return {}
        else:
            logger.error("❌ Fichier config/sponsors.json introuvable.")
            return {}

    def _load_memory(self) -> Dict[str, Any]:
        """Charge la mémoire : sélecteurs appris, overrides d'URL, et historique stats."""
        default_memory = {
            "selectors": {}, # Apprentissage DOM : {protocol: "css_selector"}
            "topology": {},  # Apprentissage URL : {protocol: "real_url"}
            "history": {},   # Volatilité & Stats
            "discount_history": {}, # {protocol: [val1, val2...]} pour stats
            "reaction_lags": {} # {leader->follower: [seconds_delay, ...]}
        }
        if MEMORY_FILE.exists():
            try:
                with open(MEMORY_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                # Merge deep pour garantir la structure
                for k, v in default_memory.items():
                    if k not in data:
                        data[k] = v
                return data
            except Exception as e:
                logger.warning(f"⚠️ Erreur chargement mémoire : {e}")
        return default_memory

    def _save_memory(self):
        """Persiste la mémoire sur disque."""
        try:
            with open(MEMORY_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.memory, f, indent=2)
        except Exception as e:
            logger.error(f"❌ Erreur sauvegarde mémoire : {e}")

    def _get_target_config(self, protocol: str) -> Dict[str, Any]:
        """Fusionne la config JSON dynamique avec la mémoire topologique (URL apprises)."""
        base_config = self.sponsors_config.get(protocol, {}).copy()
        
        # Sécurisation si pas de config (cas rare)
        if not base_config:
            return {"endpoint": None}

        # Définition de l'endpoint : soit apprise, soit config publique
        learned_url = self.memory.get("topology", {}).get(protocol)
        if learned_url:
            base_config["endpoint"] = learned_url
            
        return base_config

    def _is_statistically_plausible(self, protocol: str, discount: int, min_t: int, max_t: int) -> bool:
        """
        GARDIEN STATISTIQUE : Vérifie si le % de réduction est plausible.
        """
        # 1. Vérification Hard (bornes JSON)
        if not (min_t <= discount <= max_t):
            logger.debug(f"🛑 Rejet Hard ({protocol}): {discount}% hors limites [{min_t}-{max_t}]")
            return False

        # 2. Vérification Soft (Statistique historique) - Pour Prop Firms, 50% à 90% est fréquent
        # On est plus permissif que sur les primes bancaires
        return True

    def _update_discount_history(self, protocol: str, discount: int):
        """Met à jour l'historique glissant des discounts."""
        if protocol not in self.memory["discount_history"]:
            self.memory["discount_history"][protocol] = []
        
        history = self.memory["discount_history"][protocol]
        history.append(discount)
        if len(history) > 20: 
            history.pop(0)

    # --- INTELLIGENCE N°1 & N°3 : CORRÉLATION & DÉCLENCHEMENT SYMPATHIQUE ---
    
    def _trigger_sympathetic_scan(self, leader_protocol: str):
        """
        Active le scan d'urgence pour les concurrents du leader qui vient de bouger.
        """
        # Nettoyage de la clé pour matcher la map (ex: suppression ID partiel si besoin)
        leader_key = next((k for k in self.COMPETITORS_MAP.keys() if k in leader_protocol.lower()), None)
        
        if leader_key:
            competitors = self.COMPETITORS_MAP.get(leader_key, [])
            if competitors:
                logger.info(f"⚡ DÉCLENCHEMENT SYMPATHIQUE : Promo sur {leader_key}. Scan d'urgence de {competitors}")
                for comp in competitors:
                    # Trouver la clé complète réelle dans la config
                    real_target = next((k for k in self.sponsors_config.keys() if comp in k), None)
                    if real_target and real_target not in self.priority_queue:
                        self.priority_queue.appendleft(real_target)

    def _update_correlation_metrics(self, leader: str, follower: str):
        """Enregistre le délai de réaction entre un leader et un suiveur."""
        leader_data = self.memory["history"].get(leader, {})
        last_change = leader_data.get("last_change_ts", 0)
        
        if last_change > 0:
            current_time = time.time()
            lag = current_time - last_change
            if lag < 172800: # 48h max
                key = f"{leader}->{follower}"
                if key not in self.memory.get("reaction_lags", {}):
                    if "reaction_lags" not in self.memory: self.memory["reaction_lags"] = {}
                    self.memory["reaction_lags"][key] = []
                
                self.memory["reaction_lags"][key].append(lag)
                if len(self.memory["reaction_lags"][key]) > 5:
                    self.memory["reaction_lags"][key].pop(0)

    async def _heal_selector(self, page, protocol: str, candidates_text: str) -> Optional[str]:
        """
        CICATRISATION DU DOM (Self-Healing) V3.
        Cherche des bannières de "SALE" ou "OFF".
        """
        anchors = ["sale", "off", "discount", "code", "save", "promo", "limited"]
        
        try:
            js_script = """
            (regex_str) => {
                const regex = new RegExp(regex_str, 'i');
                const allElements = document.querySelectorAll('div, span, p, h1, h2, h3, b, strong, .banner');
                
                for (let el of allElements) {
                    if (regex.test(el.innerText)) {
                        if (el.innerText.length < 100) { 
                            let path = el.tagName.toLowerCase();
                            if (el.id) path += '#' + el.id;
                            else if (el.className) path += '.' + el.className.split(' ').join('.');
                            return path;
                        }
                    }
                }
                return null;
            }
            """
            regex_pattern = r"(\d{1,2})\s?%\s*(?:OFF|DISCOUNT)"
            new_selector = await page.evaluate(js_script, regex_pattern)
            
            if new_selector:
                logger.info(f"🩹 CICATRISATION DOM ({protocol}): Nouveau sélecteur appris -> {new_selector}")
                self.memory["selectors"][protocol] = new_selector
                return new_selector
        except Exception as e:
            logger.debug(f"Echec cicatrisation {protocol}: {e}")
        
        return None

    async def _extract_offer_data(self, page, protocol: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extrait le Pourcentage de réduction et la date.
        """
        data = {"amount": 0, "end_date": None, "code": None}
        content_block = ""
        used_strategy = "RAW"
        
        min_threshold = config.get("min_threshold", 5)
        max_threshold = config.get("max_threshold", 99)

        # 1. FAST PATH : Sélecteur en mémoire
        preferred_selector = self.memory.get("selectors", {}).get(protocol)
        if preferred_selector:
            try:
                if await page.locator(preferred_selector).count() > 0:
                    content_block = await page.inner_text(preferred_selector)
                    used_strategy = "MEMORY"
            except: pass

        # 2. BRUTE FORCE : Scan des zones chaudes (Titres, Hero, TopBar)
        if not content_block or not self.regex_discount.search(content_block):
            try:
                candidates = await page.evaluate("""() => {
                    const elements = Array.from(document.querySelectorAll('h1, h2, .hero, .banner, .top-bar, .announcement, section'));
                    return elements.map(el => el.innerText).join(' || ');
                }""")
                content_block = candidates
                used_strategy = "BRUTE"
            except Exception as e:
                logger.warning(f"Scan DOM échoué pour {protocol}: {e}")
                return data

        # Extraction Regex Discount (%)
        discounts = self.regex_discount.findall(content_block)
        
        valid_discounts = [int(x) for x in discounts if min_threshold <= int(x) <= max_threshold]
        
        if valid_discounts:
            candidate_discount = max(valid_discounts)
            if self._is_statistically_plausible(protocol, candidate_discount, min_threshold, max_threshold):
                data["amount"] = candidate_discount # On stocke le % comme "amount" pour l'unification
                self._update_discount_history(protocol, candidate_discount)

        # Extraction Code Promo
        code_match = self.regex_code.search(content_block)
        if code_match:
            data["code"] = code_match.group(1)

        # Extraction Date
        date_match = self.regex_date.search(content_block)
        if date_match:
            data["end_date"] = date_match.group(1)

        if data["amount"] > 0:
            logger.debug(f"🔍 Extraction {protocol} ({used_strategy}): -{data['amount']}% (Code: {data.get('code')})")

        return data

    async def run_scan_cycle(self):
        """Cycle principal avec Topologie Dynamique & Sympathetic Triggering."""
        logger.info("🕵️ Démarrage du cycle de veille (Prop Firms & SaaS)...")
        
        self.sponsors_config = self._load_sponsors_config()
        targets_to_scan = list(self.sponsors_config.keys())
        
        if not targets_to_scan:
            logger.warning("⚠️ Aucune cible active trouvée dans sponsors.json")
            return

        while self.priority_queue:
            target = self.priority_queue.popleft()
            if target in targets_to_scan:
                targets_to_scan.remove(target)
                targets_to_scan.insert(0, target)
                logger.info(f"🔥 SCAN PRIORITAIRE DÉCLENCHÉ: {target}")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                locale="en-US", # Global Market = English Priority
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )

            for protocol in targets_to_scan:
                config = self._get_target_config(protocol)
                
                if not config.get("endpoint"):
                    continue

                page = await context.new_page()
                
                try:
                    logger.info(f"🌍 Analyse {protocol.upper()} ({config['endpoint']})...")
                    try:
                        response = await page.goto(config['endpoint'], wait_until="domcontentloaded", timeout=20000)
                    except:
                        logger.warning(f"Timeout sur {protocol}")
                        await page.close()
                        continue
                        
                    await asyncio.sleep(2) 

                    # --- TOPOLOGIE DYNAMIQUE ---
                    final_url = page.url
                    if response and response.status in [200] and final_url != config['endpoint']:
                        if "login" not in final_url and "auth" not in final_url:
                            self.memory["topology"][protocol] = final_url

                    offer_data = await self._extract_offer_data(page, protocol, config)
                    current_discount = offer_data["amount"] # Represents % off
                    promo_code = offer_data.get("code")
                    end_date_str = offer_data["end_date"]

                    # Logique V3: Tout discount > 30% est considéré comme "Boosté"
                    is_boosted = current_discount >= 30 

                    if current_discount > 0:
                        logger.info(f"💰 {protocol.upper()}: PROMO DÉTECTÉE = -{current_discount}% (Boost: {is_boosted}) | Code: {promo_code}")
                        
                        hist_data = self.memory["history"].get(protocol, {})
                        last_discount = hist_data.get("last_amount", 0)
                        
                        if current_discount != last_discount:
                            logger.info(f"📈 VOLATILITÉ: Changement détecté sur {protocol} ({last_discount}% -> {current_discount}%)")
                            
                            self.memory["history"][protocol] = {
                                "last_amount": current_discount,
                                "last_change_ts": time.time(),
                                "volatility_score": hist_data.get("volatility_score", 1.0) + 0.5
                            }
                            
                            # Trigger Concurrents
                            self._trigger_sympathetic_scan(protocol)
                        else:
                            # Refroidissement
                            old_score = hist_data.get("volatility_score", 1.0)
                            if "history" not in self.memory: self.memory["history"] = {}
                            if protocol not in self.memory["history"]: self.memory["history"][protocol] = {}
                            self.memory["history"][protocol]["volatility_score"] = max(0.5, old_score * 0.95)

                        # Mise à jour Base de Données
                        # Note: on stocke le % dans 'amount' pour simplifier le schéma DB existant
                        await asyncio.to_thread(
                            self.db.upsert_campaign, 
                            protocol, current_discount, is_boosted, end_date_str, final_url
                        )
                    else:
                        logger.debug(f"💤 {protocol.upper()}: Pas de promo flash.")

                except Exception as e:
                    logger.error(f"❌ Erreur scan {protocol}: {e}")
                finally:
                    await page.close()

            await browser.close()
            self._save_memory()
            logger.info("✅ Cycle de veille terminé.")

    async def start_service(self):
        """Boucle de service."""
        while True:
            try:
                await self.run_scan_cycle()
                
                if self.priority_queue:
                    logger.info("⏩ MODE URGENCE: Redémarrage immédiat.")
                    await asyncio.sleep(5)
                    continue

                base_sleep = 7200 # 2 heures
                
                # Boosters Calendrier Prop Firms (Souvent fin de mois ou jours fériés US)
                now = datetime.now()
                calendar_booster = 1.0
                if now.day >= 25: calendar_booster = 0.5 # Fin de mois = Close Sales
                
                final_sleep = int(base_sleep * calendar_booster)
                logger.info(f"💤 Pause: {final_sleep/60:.0f} min")
                await asyncio.sleep(final_sleep)

            except Exception as e:
                logger.error(f"🔥 Crash Service Scanner: {e}. Retry 60s.")
                await asyncio.sleep(60)

if __name__ == "__main__":
    scanner = MarketOfferScanner()
    try:
        asyncio.run(scanner.start_service())
    except KeyboardInterrupt:
        pass