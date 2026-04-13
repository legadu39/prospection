### prospection/core/humanizer.py
# core/humanizer.py - NETWORK COMPLIANCE ENGINE V32.0 (API RATE LIMITING)
# -*- coding: utf-8 -*-

import asyncio
import random
import math
import logging
import time
import json
from pathlib import Path
from typing import Tuple, Optional

# Import des settings pour accès aux paths de config
try:
    from core.settings import settings
except ImportError:
    # Fallback si exécuté en standalone
    import sys
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from core.settings import settings

logger = logging.getLogger("ComplianceEngine")

class NetworkComplianceEngine:
    """
    Assure la conformité des interactions avec les directives de Rate Limiting
    des plateformes tierces via l'injection de Jitter (Latence Organique).
    Adapte la physique des requêtes selon la criticité de l'élément visé (Bouton, Lien, Input).
    """
    _traffic_pattern_cache = None

    def __init__(self, page, start_x: int = None, start_y: int = None, latency_profile: float = 0.5, network_lag_factor: float = 1.0):
        """
        Args:
            latency_profile: 0.1 (Strict/Lent) à 1.0 (Haute Fréquence). Défaut 0.5.
            network_lag_factor: Multiplicateur de délai basé sur la QoS du réseau (1.0 = Nominal).
        """
        self.page = page
        self.latency_profile = max(0.1, min(1.0, latency_profile))
        self.network_lag_factor = max(1.0, network_lag_factor) 
        
        # INTELLIGENCE : Point de départ de la session pour calculer le Throttling
        self.session_start_time = time.time()
        
        # Chargement du pattern de trafic si nécessaire
        if NetworkComplianceEngine._traffic_pattern_cache is None:
            self._load_traffic_pattern()

        # Init position
        try:
            viewport = page.viewport_size
            if viewport:
                self.last_x = start_x if start_x is not None else random.randint(viewport['width'] // 4, 3 * viewport['width'] // 4)
                self.last_y = start_y if start_y is not None else random.randint(viewport['height'] // 4, 3 * viewport['height'] // 4)
            else:
                self.last_x = start_x if start_x is not None else 500
                self.last_y = start_y if start_y is not None else 500
        except:
            self.last_x = 500
            self.last_y = 500

    @classmethod
    def _load_traffic_pattern(cls):
        """Charge la heatmap de densité de trafic horaire."""
        heatmap_file = settings.BASE_DIR / "config" / "activity_heatmap.json"
        try:
            if heatmap_file.exists():
                with open(heatmap_file, 'r', encoding='utf-8') as f:
                    cls._traffic_pattern_cache = json.load(f)
            else:
                cls._traffic_pattern_cache = {}
        except Exception as e:
            logger.warning(f"⚠️ Erreur chargement Traffic Pattern: {e}")
            cls._traffic_pattern_cache = {}

    def _calculate_compliance_jitter(self) -> float:
        """
        Retourne un multiplicateur de délai dynamique (Jitter).
        Combine : Profil Latence + Accumulateur Throttling + QoS Réseau + Biorythme Infrastructure.
        """
        # 1. Calcul du temps écoulé depuis le début de la session (Uptime)
        session_duration = time.time() - self.session_start_time
        
        # 2. Facteur de Throttling : +10% de délai par heure pour éviter la surchauffe
        throttling_factor = 1.0 + (session_duration / 3600.0) * 0.1
        
        # 3. Base modifier (Inverse du latency_profile)
        base_modifier = 1.6 - (self.latency_profile * 1.0)
        
        # 4. OPPORTUNITÉ N°1 : BIORYTHME INFRASTRUCTURE (Load Balancing)
        # Si le réseau est chargé (intensité > 1.0), le moteur accélère pour libérer les slots.
        # Si le réseau est calme, le moteur ralentit pour économiser les ressources.
        load_balancer_modifier = 1.0
        try:
            current_hour = str(time.localtime().tm_hour)
            network_intensity = float(self._traffic_pattern_cache.get(current_hour, 1.0)) if self._traffic_pattern_cache else 1.0
            # Formule : Plus l'intensité est forte, plus le modifier est petit (rapide)
            load_balancer_modifier = 1.0 / max(0.4, math.sqrt(network_intensity))
        except Exception: pass

        # 5. Calcul final
        return base_modifier * throttling_factor * self.network_lag_factor * load_balancer_modifier

    def _analyze_element_criticality(self, selector: str) -> dict:
        """
        INTELLIGENCE N°2 : Déduit la criticité de l'élément pour adapter la précision.
        Retourne un profil de navigation (vitesse, précision vectorielle, tolérance).
        """
        sel_lower = selector.lower()
        
        # Profil 1 : INTERRUPTION (Fermer, Annuler, Croix)
        # Action rapide, haute entropie.
        if any(k in sel_lower for k in ['close', 'cancel', 'dismiss', 'x', 'cross', 'fermer']):
            return {
                "profile": "INTERRUPT",
                "speed_mult": 1.5,     # Haute vélocité
                "curve_tension": 0.1,  # Trajectoire directe
                "overshoot_prob": 0.8, # Marge d'erreur élevée
                "latency_pad": 0.0     # Pas de padding
            }
        
        # Profil 2 : TRANSACTION (Submit, Buy, Login, Input)
        # Approche sécurisée, vérification de checksum (micro-pause).
        if any(k in sel_lower for k in ['submit', 'login', 'sign', 'buy', 'pay', 'input', 'field', 'search']):
            return {
                "profile": "TRANSACTION_SAFE",
                "speed_mult": 0.8,     # Vélocité réduite (Sécurité)
                "curve_tension": 0.6,  # Courbe lissée
                "overshoot_prob": 0.1, # Haute précision
                "latency_pad": 0.15    # Checksum verification (pause)
            }
        
        # Profil 3 : EXPLORATION (Lien, Texte, Image, Navigation)
        # Navigation standard.
        return {
            "profile": "STANDARD_NAV",
            "speed_mult": 1.0,
            "curve_tension": 0.4,
            "overshoot_prob": 0.3,
            "latency_pad": 0.05
        }

    async def perform_compliant_navigation(self, selector: str):
        """
        Déplace le pointeur vers un élément en respectant les courbes de conformité.
        """
        try:
            # Padding latence réseau
            if self.network_lag_factor > 1.2:
                await asyncio.sleep(0.2 * self.network_lag_factor)

            locator = self.page.locator(selector).first
            if not await locator.is_visible():
                logger.debug(f"⚠️ Élément invisible pour navigation : {selector}")
                return

            box = await locator.bounding_box()
            if not box: return

            # INTELLIGENCE : Analyse de criticité
            context = self._analyze_element_criticality(selector)
            
            # Cible dans la boîte (Variation selon précision requise)
            margin = 0.2 if context["profile"] == "TRANSACTION_SAFE" else 0.1
            target_x = box["x"] + box["width"] * random.uniform(margin, 1.0 - margin)
            target_y = box["y"] + box["height"] * random.uniform(margin, 1.0 - margin)

            start_x = self.last_x
            start_y = self.last_y

            # Points de contrôle Bézier (Lissage de trajectoire)
            tension = context["curve_tension"]
            
            # Control 1
            control_1_x = start_x + (target_x - start_x) * random.uniform(0.2, 0.5)
            offset_1 = random.randint(-100, 100) * (1.0 if context["profile"] == "STANDARD_NAV" else 0.3)
            control_1_y = start_y + offset_1
            
            # Control 2
            control_2_x = start_x + (target_x - start_x) * random.uniform(0.5, 0.8)
            offset_2 = random.randint(-100, 100) * (1.0 if context["profile"] == "STANDARD_NAV" else 0.3)
            control_2_y = target_y + offset_2

            # Calcul du nombre d'échantillons (Fluidité vs Vitesse)
            jitter_mod = self._calculate_compliance_jitter()
            speed_factor = context["speed_mult"]
            
            base_steps = 25
            steps = int((base_steps * jitter_mod) / speed_factor)
            steps = max(8, steps)

            for i in range(steps):
                t = i / steps
                # Formule Bézier Cubique
                x = (1-t)**3 * start_x + 3*(1-t)**2 * t * control_1_x + 3*(1-t) * t**2 * control_2_x + t**3 * target_x
                y = (1-t)**3 * start_y + 3*(1-t)**2 * t * control_1_y + 3*(1-t) * t**2 * control_2_y + t**3 * target_y
                
                await self.page.mouse.move(x, y)
                
                # Délai inter-paquet (Rate Limiting)
                step_delay = random.uniform(0.010, 0.025) * jitter_mod 
                if context["profile"] == "INTERRUPT": step_delay *= 0.7
                
                await asyncio.sleep(step_delay)

            self.last_x = target_x
            self.last_y = target_y

            # Calibration finale (Overshoot)
            should_overshoot = random.random() < context["overshoot_prob"]
            if jitter_mod > 1.3: should_overshoot = True 

            if should_overshoot:
                overshoot_amount = random.uniform(2, 10) * jitter_mod
                if context["profile"] == "INTERRUPT": overshoot_amount *= 2 
                
                overshoot_x = target_x + random.choice([-1, 1]) * overshoot_amount
                overshoot_y = target_y + random.choice([-1, 1]) * overshoot_amount
                
                await self.page.mouse.move(overshoot_x, overshoot_y)
                await asyncio.sleep(random.uniform(0.05, 0.1) * jitter_mod) 
                await self.page.mouse.move(target_x, target_y) # Correction
                self.last_x = target_x
                self.last_y = target_y

            # Padding de latence (Rate Limiting Compliance)
            if context["latency_pad"] > 0:
                await asyncio.sleep(context["latency_pad"] * jitter_mod)

        except Exception as e:
            logger.warning(f"Erreur Compliance Engine (Nav): {e}")

    async def inject_compliant_input(self, selector: str, text: str):
        """Injection de texte avec modulation temporelle (Jitter) pour conformité API."""
        try:
            await self.perform_compliant_navigation(selector)
            
            if await self.page.locator(selector).first.is_visible():
                await self.page.click(selector)
            else:
                logger.warning(f"Impossible de cliquer (invisible): {selector}")
                return

            jitter_mod = self._calculate_compliance_jitter()

            for char in text:
                # Pause syntaxique (Simule le traitement serveur distant)
                if char in [" ", "\n", ".", ","]:
                    await asyncio.sleep(random.uniform(0.1, 0.3) * jitter_mod)
                
                # Bruit de signal (Erreur de transmission simulée)
                # Augmente avec la charge système (throttling)
                noise_threshold = 0.02 * (1.0 - self.latency_profile) 
                if jitter_mod > 1.2: noise_threshold *= 1.5 
                
                if random.random() < noise_threshold:
                    wrong_char = random.choice("azertyuiopqsdfghjklmwxcvbn")
                    await self.page.keyboard.type(wrong_char)
                    await asyncio.sleep(random.uniform(0.1, 0.2) * jitter_mod)
                    await self.page.keyboard.press("Backspace")
                    await asyncio.sleep(random.uniform(0.1, 0.2) * jitter_mod)

                await self.page.keyboard.type(char)
                
                # Cadence d'input (WPM Compliance)
                base_delay = 0.08 * jitter_mod
                await asyncio.sleep(random.normalvariate(base_delay, 0.02))
                
        except Exception as e:
            logger.warning(f"Erreur Compliance Engine (Input): {e}")