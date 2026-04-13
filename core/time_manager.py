### prospection/core/time_manager.py
# core/time_manager.py - CIRCADIAN CYCLE V5.0 (GLOBAL MARKET SYNC)
# -*- coding: utf-8 -*-

import time
import random
import asyncio
import logging
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional
from collections import deque

try:
    from core.settings import settings
except ImportError:
    import sys
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from core.settings import settings

logger = logging.getLogger("CircadianCycle")

# Décalage horaire cible par défaut (UTC offset).
# Pour NY (EST) c'est -5 ou -4 selon DST. On vise une synchro UTC pour simplifier.
MARKET_OPEN_LONDON_UTC = 8
MARKET_OPEN_NY_UTC = 14

class CircadianCycle:
    """
    Gestionnaire de temps biologique V5.0 (Global Market Sync).
    Intègre l'apprentissage par renforcement, la persistance des habitudes,
    et la synchronisation avec la volatilité des marchés financiers (Prop Firms).
    """
    def __init__(self, start_hour=8, end_hour=23, work_weekends=True, target_utc_offset=0):
        # On élargit les horaires pour couvrir l'overlap US/EU
        self.start_hour = start_hour
        self.end_hour = end_hour
        self.work_weekends = work_weekends
        self.target_offset = target_utc_offset # 0 = UTC (Base de ref pour Trading)
        
        self.stats_file = settings.BASE_DIR / "config" / "activity_heatmap.json"
        self.hourly_stats = self._load_stats()
        
        self.recent_successes = deque(maxlen=10)
        self.stress_level = 0.0 
        self.consecutive_fails = 0

    def _get_target_time(self) -> datetime:
        """Retourne l'heure ajustée au marché cible (UTC par défaut)."""
        return datetime.now(timezone.utc)

    def _load_stats(self) -> Dict[str, float]:
        if self.stats_file.exists():
            try:
                with open(self.stats_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"⚠️ Erreur chargement stats, reset: {e}")
        return {str(h): 1.0 for h in range(24)}

    def _save_stats(self):
        try:
            self.stats_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.stats_file, "w", encoding="utf-8") as f:
                json.dump(self.hourly_stats, f, indent=2)
        except Exception as e:
            logger.error(f"❌ Échec sauvegarde stats: {e}")

    def get_volatility_multiplier(self) -> float:
        """
        🧠 INTELLIGENCE V5.0: Volatility-Aware Scheduling.
        Retourne un multiplicateur de vitesse (Plus petit = Plus rapide).
        """
        now_utc = datetime.now(timezone.utc).hour
        
        # Boost lors des ouvertures de session (Liquidité Max pour Prop Firms)
        # London Open (08h-10h UTC) et NY Open (14h-16h UTC)
        if now_utc in [MARKET_OPEN_LONDON_UTC, MARKET_OPEN_LONDON_UTC+1]:
            return 0.6 # Turbo Mode (40% plus rapide)
        if now_utc in [MARKET_OPEN_NY_UTC, MARKET_OPEN_NY_UTC+1]:
            return 0.5 # Hyper Mode (50% plus rapide)
            
        return 1.0

    def should_work(self) -> bool:
        now = self._get_target_time() 
        
        # 1. Vérification Horaire (Basée sur UTC/Target, pas locale)
        if not (self.start_hour <= now.hour < self.end_hour):
            return False
            
        # 2. Week-end
        if not self.work_weekends and now.weekday() >= 5:
            return False
            
        # 3. Burnout Protection
        if self.stress_level > 0.9:
            logger.warning("🥵 BURNOUT DETECTED. Repos forcé.")
            return False
            
        return True

    async def wait_for_slot(self):
        """Bloque l'exécution jusqu'au prochain créneau valide."""
        while not self.should_work():
            next_start = self._next_start_time()
            now_local = datetime.now()
            wait_seconds = (next_start - self._get_target_time()).total_seconds()
            
            logger.info(f"🌙 Hors cycle marché. Reprise estimée dans {wait_seconds/3600:.1f}h")
            
            # On dort par tranches pour rester réactif
            if wait_seconds > 0:
                await self._interruptible_sleep(min(wait_seconds, 3600))
            else:
                await asyncio.sleep(60)

    async def organic_pause(self, base_seconds=60):
        """
        Pause 'biologique' optimisée pour la volatilité.
        """
        now = self._get_target_time()
        hour_key = str(now.hour)
        
        efficiency = self.hourly_stats.get(hour_key, 1.0)
        volatility_factor = self.get_volatility_multiplier() # Nouveauté V5.0
        
        momentum = 1.0
        if len(self.recent_successes) > 3:
            success_rate = sum(self.recent_successes) / len(self.recent_successes)
            if success_rate > 0.8: 
                momentum = 0.6 
            elif success_rate < 0.2:
                momentum = 1.5 

        random_flux = random.uniform(0.8, 1.2)
        
        # Formule : Base * Efficiency * Momentum * Volatility * Random
        # Si Volatility est haute (0.5), la pause est divisée par 2.
        final_duration = base_seconds * efficiency * momentum * volatility_factor * random_flux * (1 + self.stress_level)
        
        if final_duration > 300: 
            logger.info(f"☕ Pause Café ({int(final_duration/60)} min). Stress: {self.stress_level:.1f}")
            
        await asyncio.sleep(final_duration)

    def _interruptible_sleep(self, duration: float):
        return asyncio.sleep(duration) # Simplifié pour async native

    def _next_start_time(self) -> datetime:
        now = self._get_target_time()
        target = now.replace(hour=self.start_hour, minute=0, second=0, microsecond=0)
        if now.hour >= self.start_hour:
             target += timedelta(days=1)
        return target

    def record_outcome(self, success: bool):
        val = 1 if success else 0
        self.recent_successes.append(val)
        
        if not success:
            self.consecutive_fails += 1
            self.stress_level = min(1.0, self.stress_level + 0.05)
        else:
            self.consecutive_fails = 0
            self.stress_level = max(0.0, self.stress_level - 0.1)

        try:
            current_hour = str(self._get_target_time().hour)
            current_weight = self.hourly_stats.get(current_hour, 1.0)
            
            alpha = 0.05 
            
            if success:
                new_weight = max(0.5, current_weight - alpha)
            else:
                new_weight = min(2.0, current_weight + (alpha / 2))
                
            self.hourly_stats[current_hour] = new_weight
            
            if random.random() < 0.1: 
                self._save_stats()
                
        except Exception as e:
            logger.error(f"⚠️ Erreur Learning Loop: {e}")