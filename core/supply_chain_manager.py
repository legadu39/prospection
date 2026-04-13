### prospection/core/supply_chain_manager.py
# core/supply_chain_manager.py - YIELD OPTIMIZER V5.4 (SIGNAL EXCHANGE)
# -*- coding: utf-8 -*-

"""
YIELD OPTIMIZER V5.4
--------------------
Moteur de décision stratégique pour le routing des leads.
Intègre la logique "Waterfall", "Yield" et "Signal Exchange".
Analyse les tendances Cross-Canal pour ajuster les priorités de chasse.
"""

import logging
import random
import json
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from pathlib import Path
from decimal import Decimal

# Integration Database pour vérifier les quotas réels
try:
    from core.database import NexusDB
    from core.settings import settings
except ImportError:
    import sys
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from core.database import NexusDB
    from core.settings import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s | [YIELD] %(message)s")
logger = logging.getLogger("YieldOptimizer")

@dataclass
class OfferConfig:
    key: str
    name: str
    payout: float
    conversion_rate: float
    is_boosted: bool = False
    quota_current: int = 0
    quota_max: int = 9999
    priority: int = 1
    active: bool = True
    program_type: str = "CPA" # REVENUE_SHARE ou CPA

class FleetManager:
    """
    Gestionnaire de la Supply Chain (Offres & Partenaires).
    Intègre le "Smart Pacing" et le "Signal Exchange" (Cross-Channel).
    """
    def __init__(self, db: Optional[NexusDB] = None):
        self.db = db if db else NexusDB()
        self.offers: Dict[str, OfferConfig] = {}
        self.sponsors_file = settings.BASE_DIR / "config" / "sponsors.json"
        self.hunting_orders_file = settings.BASE_DIR / "config" / "hunting_orders.json"
        self._last_refresh = 0
        self._refresh_interval = 300  # 5 minutes
        
        # Chargement initial
        self.refresh_market_data()

    def run_monitor_loop(self):
        """Thread de fond pour la mise à jour des stats (compatible Launcher)."""
        logger.info("📡 Yield Monitor started.")
        while True:
            try:
                self.refresh_market_data()
                self._analyze_global_signals() # Intelligence N°2
                time.sleep(self._refresh_interval)
            except Exception as e:
                logger.error(f"⚠️ Yield Monitor Error: {e}")
                time.sleep(60)

    def refresh_market_data(self):
        """Recharge la configuration et les quotas depuis la DB/JSON."""
        try:
            # 1. Chargement Config Statique
            if self.sponsors_file.exists():
                with open(self.sponsors_file, 'r', encoding='utf-8') as f:
                    raw_sponsors = json.load(f)
            else:
                raw_sponsors = []
                logger.warning("⚠️ sponsors.json introuvable.")

            # 2. Enrichissement Dynamique (Quotas & Boosts)
            new_offers = {}
            for s in raw_sponsors:
                if not s.get('active', True):
                    continue

                program_key = s.get('program', 'unknown').lower()
                sponsor_id = s.get('id')
                
                # Récupération Stats Réelles via DB
                quota_used = 0
                if self.db:
                    stats = self.db.get_sponsor_stats(sponsor_id)
                    quota_used = stats.get('verified_count_month', 0) + stats.get('pending_leads_count', 0)

                # Detection Boost (Offer Hunter)
                is_boosted = False
                boost_payout = s['financial_terms']['target_cpa']
                
                campaign_info = self.db.get_campaign_info(program_key)
                if campaign_info and campaign_info.get('is_boosted'):
                    is_boosted = True
                    if campaign_info.get('amount'):
                         boost_payout = float(campaign_info['amount'])

                config = OfferConfig(
                    key=program_key,
                    name=s.get('label', program_key),
                    payout=float(boost_payout),
                    conversion_rate=float(s.get('conversion_rate_estimate', 0.1)),
                    is_boosted=is_boosted,
                    quota_current=quota_used,
                    quota_max=int(s.get('quota_max', 20)),
                    priority=int(s.get('priority', 1)),
                    active=True,
                    program_type=s['financial_terms']['model']
                )
                
                unique_key = f"{program_key}_{sponsor_id}" 
                new_offers[unique_key] = config

            self.offers = new_offers
            logger.info(f"🔄 Market Data Refreshed: {len(self.offers)} active routes loaded.")

        except Exception as e:
            logger.error(f"❌ Market Refresh Failed: {e}")

    def _analyze_global_signals(self):
        """
        INTELLIGENCE N°2 (Signal Exchange) : 
        Analyse les taux de conversion globaux pour détecter des tendances (ex: 'Crypto' explose).
        Génère un fichier de priorité 'hunting_orders.json' pour les Snipers.
        """
        try:
            # Simulation d'analyse simple (à connecter à une table 'conversion_logs' réelle)
            # Ici on regarde si un programme a un 'is_boosted' activé
            priorities = []
            
            for key, offer in self.offers.items():
                if offer.is_boosted:
                    # Signal fort : On ordonne à TOUS les snipers de chasser ça
                    priorities.append({
                        "keyword": offer.key, # ex: "prop_firm"
                        "intensity": "AGGRESSIVE",
                        "reason": "YIELD_BOOST",
                        "expires_at": time.time() + 3600
                    })
            
            # Sauvegarde pour les bots externes (Snipers)
            if priorities:
                with open(self.hunting_orders_file, 'w', encoding='utf-8') as f:
                    json.dump({"orders": priorities, "updated_at": time.time()}, f)
                logger.info(f"📢 Signal Exchange: {len(priorities)} ordres de chasse émis.")

        except Exception as e:
            logger.warning(f"⚠️ Signal Exchange Error: {e}")

    def _calculate_pacing_factor(self, offer: OfferConfig) -> float:
        """
        Anticipation & Prédiction: PID Regulator pour consommation fluide.
        """
        if offer.quota_max >= 9000:
            return 1.0

        days_in_month = 30
        current_day = time.localtime().tm_mday
        
        quota_target_today = (offer.quota_max / days_in_month) * current_day
        error = offer.quota_current - quota_target_today
        
        if error > 0:
            overburn_ratio = error / max(1, quota_target_today)
            if overburn_ratio > 0.5: return 0.1 
            elif overburn_ratio > 0.2: return 0.5 
            else: return 0.8
        elif error < 0:
            return 1.0
            
        return 1.0

    def _calculate_epc(self, offer: OfferConfig, context: str) -> float:
        """
        Calcule l'Earn Per Click (Espérance de gain) avec logique Waterfall & Pacing.
        """
        # 1. WATERFALL CHECK (Quota Hard)
        if offer.quota_current >= offer.quota_max:
            return 0.0 

        # 2. Base EPC
        base_epc = offer.payout * offer.conversion_rate
        
        # 3. Pacing
        pacing_factor = self._calculate_pacing_factor(offer)
        base_epc *= pacing_factor

        # 4. Boosts
        if offer.is_boosted:
            base_epc *= 1.3
            
        if offer.priority > 1:
            base_epc *= 0.8 

        # 5. Contexte Sémantique V3 (Capital & Crypto)
        if context == "crypto" or context == "invest":
            if "meria" in offer.key or "binance" in offer.key or "ledger" in offer.key:
                base_epc *= 2.0
        
        elif context == "capital" or context == "prop_firm" or context == "trading":
            if "apex" in offer.key or "ftmo" in offer.key or "topstep" in offer.key:
                base_epc *= 2.5 
            elif "trade" in offer.key or "etoro" in offer.key:
                base_epc *= 1.5

        return base_epc

    def get_best_route(self, lead_context: str = "general") -> Dict:
        """
        Détermine la meilleure offre (Sponsor ID) pour un lead donné.
        """
        scored_offers = []
        
        for key, offer in self.offers.items():
            epc = self._calculate_epc(offer, lead_context)
            if epc > 0:
                scored_offers.append({
                    "sponsor_id": key.split('_', 1)[1] if '_' in key else key,
                    "program": offer.key.split('_')[0],
                    "name": offer.name,
                    "score": epc,
                    "is_boosted": offer.is_boosted,
                    "target_url": f"/api/redirect/{key}" 
                })
            
        if not scored_offers:
            logger.warning(f"⚠️ Aucun sponsor disponible pour le contexte {lead_context} (Quotas pleins ?)")
            return None
            
        scored_offers.sort(key=lambda x: x["score"], reverse=True)
        best = scored_offers[0]
        
        return best

    def get_display_config(self) -> Dict:
        """
        Retourne la configuration pour l'UI Frontend (CapitalAllocationUI).
        """
        capital_best = self.get_best_route("capital") 
        crypto_best = self.get_best_route("crypto")
        
        if not capital_best:
             capital_best = {"name": "Allocation (Complet)", "score": 0}
        if not crypto_best:
             crypto_best = {"name": "Outils Pro", "score": 0}

        cap_offer = next((o for k,o in self.offers.items() if capital_best.get('sponsor_id') in k), None)
        cry_offer = next((o for k,o in self.offers.items() if crypto_best.get('sponsor_id') in k), None)

        cap_amount = f"{int(cap_offer.payout)}$" if cap_offer else "Fermé"
        cry_amount = f"{int(cry_offer.payout)}€" if cry_offer else "Accès"

        return {
            "finance": { 
                "program": capital_best.get('program', 'prop_firm'),
                "amount": cap_amount,
                "status": "FLASH SALE" if cap_offer and cap_offer.is_boosted else "DISPO",
                "color": "from-rose-500 to-pink-600" if cap_offer and cap_offer.is_boosted else "from-blue-600 to-indigo-600"
            },
            "crypto": {
                "program": crypto_best.get('program', 'crypto'),
                "amount": cry_amount,
                "status": "PROMO" if cry_offer and cry_offer.is_boosted else "SAAS",
                "color": "from-emerald-500 to-teal-600"
            }
        }

if __name__ == "__main__":
    mgr = FleetManager()
    print(mgr.get_display_config())