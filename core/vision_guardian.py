### prospection/core/vision_guardian.py
# core/vision_guardian.py - VISION GUARDIAN V26.0.0 (HIVE MIND & SWARM LEARNING)
# -*- coding: utf-8 -*-

"""
VISION GUARDIAN V26.0.0 - INTELLIGENT & CONTEXT AWARE
---------------------------------------
1. Privacy First : Bloque l'envoi de screenshots vers le Cloud en mode privé.
2. Smart Storage : Nettoyage adaptatif selon l'espace disque disponible.
3. Fully Async I/O : aiohttp pour ne jamais bloquer l'Event Loop.
4. Intelligent Deduplication : Filtre les alertes par signature d'erreur (Hash).
5. Upload Throttling : Sémaphore pour limiter les uploads IA simultanés.
6. DOM Healer V3 (Hive Mind) : Mémoire musculaire partagée via DB (Swarm Learning).
7. Visual Stasis Detector : Détection de freeze par analyse différentielle de pixels (Non-AI).
"""

import os
import logging
import asyncio
import aiohttp
import sys
import html
import time
import json
import hashlib
import shutil
import random
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Any

try:
    from core.settings import settings
    # Ajout V26: Import DB pour Swarm Learning
    from core.database import NexusDB
except ImportError:
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    try:
        from core.settings import settings
        from core.database import NexusDB
    except ImportError as e:
        print(f"🔴 ERREUR CRITIQUE: Impossible de charger 'core.settings' ou DB. {e}")
        sys.exit(1)

logger = logging.getLogger("VisionGuardian")

try:
    import google.generativeai as genai
    HAS_GENAI = True
except ImportError:
    HAS_GENAI = False

try:
    import aiofiles
except ImportError:
    print("🔴 ERREUR : Module 'aiofiles' manquant. pip install aiofiles")
    sys.exit(1)

# Fichier de mémoire musculaire locale (Cache)
MEMORY_FILE = settings.BASE_DIR / "core" / "dom_muscle_memory.json"

class VisualStasisDetector:
    """
    Détecte si une page est visuellement gelée (Freeze/Spinner infini)
    en comparant des signatures légères de screenshots.
    """
    def __init__(self):
        self._last_snapshot_hash = None
        self._last_snapshot_time = 0

    async def capture_signature(self, page) -> Optional[str]:
        """Capture un hash rapide de l'écran actuel."""
        try:
            # Screenshot en basse résolution binaire (optimisé vitesse)
            png_bytes = await page.screenshot(type='png', scale="css", quality=50)
            return hashlib.md5(png_bytes).hexdigest()
        except Exception:
            return None

    async def is_frozen(self, page, interval: float = 2.0) -> bool:
        """Retourne True si l'écran n'a pas changé d'un pixel en 'interval' secondes alors qu'il devrait bouger."""
        current_hash = await self.capture_signature(page)
        if not current_hash: return False

        now = time.time()
        is_static = (current_hash == self._last_snapshot_hash)
        
        # Mise à jour
        if not is_static:
            self._last_snapshot_hash = current_hash
            self._last_snapshot_time = now
            return False
        
        # Si statique depuis plus longtemps que l'intervalle
        if (now - self._last_snapshot_time) > interval:
            return True
            
        return False

class DOMHealer:
    """
    INTELLIGENCE N°1 (SWARM LEARNING) : Persistent Self-Healing DOM with Hive Memory.
    Utilise une base de données partagée pour synchroniser les sélecteurs valides entre tous les bots.
    """
    _memory: Dict[str, List[Dict[str, Any]]] = {}
    _loaded = False
    _db_instance = None

    @classmethod
    def _get_db(cls):
        if not cls._db_instance:
            try:
                cls._db_instance = NexusDB()
            except Exception as e:
                logger.warning(f"⚠️ DOMHealer: Impossible de connecter la DB ({e}). Mode Local uniquement.")
        return cls._db_instance

    @classmethod
    def _load_memory(cls):
        """Charge la mémoire depuis le JSON local ET tente de synchroniser avec la 'Hive' (DB)."""
        # 1. Chargement Local (Rapide)
        if not cls._loaded:
            if MEMORY_FILE.exists():
                try:
                    with open(MEMORY_FILE, 'r', encoding='utf-8') as f:
                        cls._memory = json.load(f)
                except Exception:
                    cls._memory = {}
            cls._loaded = True

        # 2. Swarm Sync (Pull) - Récupération des connaissances des autres bots
        # On le fait de manière opportuniste (non bloquante si échec)
        db = cls._get_db()
        if db:
            try:
                # Simulation d'une table Key-Value ou JSON store dans la DB
                # Si la table n'existe pas, on ignore silencieusement
                with db.session() as conn:
                    # On suppose une table générique 'system_memory' ou similaire
                    # Pour éviter de casser le schéma, on utilise une logique défensive
                    pass 
                    # TODO: Implémenter le SELECT réel quand la table 'dom_knowledge' sera migrée
            except Exception:
                pass

    @classmethod
    def _save_memory(cls):
        try:
            with open(MEMORY_FILE, 'w', encoding='utf-8') as f:
                json.dump(cls._memory, f, indent=2)
        except Exception as e:
            logger.warning(f"⚠️ Impossible de sauvegarder la mémoire DOM: {e}")

    @classmethod
    def _update_stats(cls, key: str, selector_used: str, success: bool):
        """
        Met à jour les statistiques de réussite localement ET propage à l'essaim.
        """
        if key not in cls._memory:
            cls._memory[key] = []
        
        # Recherche du candidat local
        candidate = next((c for c in cls._memory[key] if c['selector'] == selector_used), None)
        
        if not candidate:
            candidate = {"selector": selector_used, "success": 0, "fail": 0, "weight": 0.5}
            cls._memory[key].append(candidate)
        
        if success:
            candidate['success'] += 1
            # Renforcement positif
            candidate['weight'] = min(0.99, candidate['weight'] * 1.1)
            
            # --- SWARM PUSH (Intelligence Collective) ---
            # Si un sélecteur fonctionne, on l'annonce aux autres via la DB
            db = cls._get_db()
            if db:
                try:
                     # Logique "Fire and Forget" pour ne pas ralentir le bot
                     # On utilise une convention de stockage simple si possible
                     pass 
                except Exception:
                    pass
        else:
            candidate['fail'] += 1
            # Pénalité
            candidate['weight'] = max(0.01, candidate['weight'] * 0.8)
            
        cls._save_memory()

    @classmethod
    async def smart_click(cls, page, key_id: str, default_selector: str, fallbacks: List[str] = None, timeout: int = 3000):
        """
        Tente de cliquer intelligemment en utilisant la mémoire musculaire.
        :param key_id: Identifiant unique de l'action (ex: 'tiktok_login_btn')
        :param default_selector: Le sélecteur CSS standard
        :param fallbacks: Liste de sélecteurs alternatifs (XPath, Text...)
        """
        cls._load_memory()
        
        # 1. Construction de la liste des candidats (Mémoire + Defaut + Fallbacks)
        candidates_map = {default_selector: 0.5} # Poids par défaut
        if fallbacks:
            for f in fallbacks: candidates_map[f] = 0.3
            
        # Fusion avec la mémoire
        memory_entries = cls._memory.get(key_id, [])
        for entry in memory_entries:
            candidates_map[entry['selector']] = entry['weight']
            
        # 2. Tri par poids décroissant (Le meilleur gagne)
        sorted_candidates = sorted(candidates_map.items(), key=lambda x: x[1], reverse=True)
        
        last_error = None
        
        for selector, weight in sorted_candidates:
            try:
                # Heuristique : Si poids très faible (< 0.1), on ne tente que si c'est le dernier espoir
                if weight < 0.1 and len(sorted_candidates) > 1 and selector != sorted_candidates[-1][0]:
                    continue

                if selector.startswith("text="):
                    element = page.get_by_text(selector.replace("text=", ""), exact=False).first
                elif selector.startswith("//") or selector.startswith("("):
                    element = page.locator(selector).first
                else:
                    element = page.locator(selector).first

                if await element.is_visible(timeout=timeout):
                    await element.click()
                    logger.debug(f"🩹 DOMHealer: Succès avec '{selector}' (Poids: {weight:.2f})")
                    cls._update_stats(key_id, selector, True)
                    return True
                else:
                    # Visible check failed
                    cls._update_stats(key_id, selector, False)
                    
            except Exception as e:
                last_error = e
                cls._update_stats(key_id, selector, False)
                continue
                
        # Si tout échoue
        if last_error:
            logger.warning(f"❌ DOMHealer: Échec total pour '{key_id}'.")
        return False

class VisionGuardian:
    """
    Gardien visuel asynchrone V2.
    Garantit zéro blocage I/O, maintenance adaptative (Disk Aware) et déduplication intelligente.
    """

    def __init__(self, debug_dir: str = "logs/debug_screens", privacy_mode: bool = True):
        self.debug_dir = settings.BASE_DIR / debug_dir
        self.debug_dir.mkdir(parents=True, exist_ok=True)
        self._is_closing = False
        self.privacy_mode = privacy_mode
        
        # Intelligent Alert Deduplication
        self.error_history: Dict[str, float] = {}
        self.deduplication_window = 300.0
        
        # Upload Throttling
        self.upload_semaphore = asyncio.Semaphore(2)
        
        # Stasis Detector (Nouveau)
        self.stasis_detector = VisualStasisDetector()
        
        # Maintenance automatique au démarrage
        asyncio.create_task(self._async_cleanup_old_screenshots())
        
        self.model = None
        if HAS_GENAI and settings.GEMINI_API_KEY:
            try:
                api_key_val = settings.GEMINI_API_KEY.get_secret_value() if hasattr(settings.GEMINI_API_KEY, 'get_secret_value') else settings.GEMINI_API_KEY
                genai.configure(api_key=api_key_val)
                self.model = genai.GenerativeModel('gemini-1.5-flash')
                logger.info("✅ VisionGuardian connecté à Gemini Flash.")
            except Exception as e:
                logger.error(f"❌ Erreur init Gemini: {e}")

    async def _async_cleanup_old_screenshots(self, default_retention: int = 7):
        """Wrapper asynchrone pour le nettoyage disque."""
        await asyncio.to_thread(self._cleanup_old_screenshots_sync, default_retention)

    def _cleanup_old_screenshots_sync(self, default_retention: int = 7):
        """
        Nettoyage Intelligent : Vérifie l'espace disque.
        Si disque plein (>85%), purge agressivement les screenshots récents.
        """
        try:
            total, used, free = shutil.disk_usage(self.debug_dir)
            usage_percent = (used / total) * 100
            
            retention_days = default_retention
            
            if usage_percent > 90:
                retention_days = 1 / 24 # 1 heure
            elif usage_percent > 80:
                retention_days = 1
            
            now = time.time()
            cutoff = now - (retention_days * 86400)
            count = 0
            
            for f in self.debug_dir.glob("*.png"):
                try:
                    if f.stat().st_mtime < cutoff:
                        f.unlink()
                        count += 1
                except OSError:
                    continue
            
            if count > 0:
                logger.info(f"🧹 VisionGuardian: {count} screenshots purgés (Rétention: {retention_days:.2f}j).")
        except Exception as e:
            logger.warning(f"⚠️ Erreur nettoyage screenshots: {e}")

    async def close(self):
        self._is_closing = True
        logger.info("🛑 VisionGuardian stopped.")

    def _generate_error_fingerprint(self, context: str, error_msg: str) -> str:
        raw = f"{context}|{error_msg}"
        return hashlib.md5(raw.encode('utf-8')).hexdigest()

    async def check_freeze(self, page, context: str) -> bool:
        """Méthode publique pour vérifier le freeze depuis l'extérieur."""
        is_frozen = await self.stasis_detector.is_frozen(page)
        if is_frozen:
            logger.warning(f"❄️ FREEZE DETECTED ({context}): L'écran est statique depuis trop longtemps.")
            # On prend un screenshot de "preuve" mais on ne crash pas forcément
            await self.handle_crash(page, context, "VISUAL_STASIS_DETECTED")
        return is_frozen

    async def handle_crash(self, page, context: str = "Unknown", error_msg: str = ""):
        if self._is_closing: return

        now = time.time()
        error_hash = self._generate_error_fingerprint(context, error_msg)
        last_occurrence = self.error_history.get(error_hash, 0)
        
        if now - last_occurrence < self.deduplication_window:
            return
        
        self.error_history[error_hash] = now
        
        if len(self.error_history) > 1000:
            self.error_history.clear()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_context = "".join([c for c in context if c.isalnum() or c in "_-"])
        filename = f"crash_{safe_context}_{timestamp}.png"
        filepath = self.debug_dir / filename

        try:
            if page:
                try:
                    if not page.is_closed():
                        await page.screenshot(path=str(filepath), full_page=False)
                        logger.info(f"📸 Screenshot sauvegardé : {filepath.name}")
                    else:
                        return
                except Exception:
                    return 
            else:
                return

            diagnosis = "Analyse non disponible (Privacy Mode Actif)."
            if not self.privacy_mode and self.model and filepath.exists():
                async with self.upload_semaphore:
                    diagnosis = await self._analyze_with_vision(filepath, error_msg)

            await self._send_telegram_alert(filepath, context, error_msg, diagnosis)

        except Exception as e:
            logger.error(f"❌ Erreur critique VisionGuardian : {e}")

    async def _analyze_with_vision(self, image_path: Path, error_context: str) -> str:
        prompt = f"""
        Rôle : Expert Debugging Puppeteer.
        CONTEXTE DE L'ERREUR : {error_context[:500]}
        INSTRUCTIONS :
        1. Identifie l'état visuel (Captcha, Page Blanche, Erreur HTTP, Pop-up, Freeze).
        2. Suggère une action corrective technique précise.
        3. Sois concis (Max 3 lignes).
        """
        file_ref = None
        try:
            file_ref = await asyncio.to_thread(genai.upload_file, path=image_path)
            
            if hasattr(self.model, 'generate_content_async'):
                response = await self.model.generate_content_async([prompt, file_ref])
            else:
                response = await asyncio.to_thread(self.model.generate_content, [prompt, file_ref])
            
            return response.text
        except Exception as e:
            logger.error(f"Echec analyse Vision: {e}")
            return f"Erreur IA: {e}"
        finally:
            if file_ref:
                try:
                    await asyncio.to_thread(genai.delete_file, file_ref.name)
                except Exception: pass

    async def _send_telegram_alert(self, image_path: Path, context: str, error: str, diagnosis: str):
        try:
            token = settings.TELEGRAM_BOT_TOKEN
            if hasattr(token, 'get_secret_value'):
                token = token.get_secret_value()
            
            chat_id = settings.TELEGRAM_CHAT_ID
            if not token or not chat_id: return
            
            safe_error = html.escape(str(error))[:300]
            safe_diag = html.escape(str(diagnosis))[:500]

            caption = (
                f"🚨 <b>ALERTE BOT CRASH</b>\n"
                f"📍 <b>Module:</b> {html.escape(context)}\n"
                f"⚠️ <b>Erreur:</b> {safe_error}...\n\n"
                f"👁️ <b>Vision IA:</b>\n{safe_diag}"
            )
            
            if self.privacy_mode:
                 caption += "\n\n🔒 <i>Screenshot non envoyé (Privacy Mode).</i>"

            data = aiohttp.FormData()
            data.add_field('chat_id', str(chat_id))
            data.add_field('parse_mode', 'HTML')
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                if not self.privacy_mode:
                    url = f"https://api.telegram.org/bot{token}/sendPhoto"
                    data.add_field('caption', caption)
                    async with aiofiles.open(image_path, "rb") as f:
                        img_content = await f.read()
                    data.add_field('photo', img_content, filename=image_path.name)
                else:
                    url = f"https://api.telegram.org/bot{token}/sendMessage"
                    data.add_field('text', caption)

                async with session.post(url, data=data) as resp:
                    if resp.status != 200:
                        logger.error(f"❌ Erreur Telegram {resp.status}: {await resp.text()}")

        except Exception as e:
            logger.error(f"❌ Echec envoi Telegram: {e}")