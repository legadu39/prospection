### prospection/pipeline_bridge.py
# pipeline_bridge.py - NEXUS INTELLIGENT PIPELINE V3.7 (GRAVITY SORTING & FAST-TRACK)
# -*- coding: utf-8 -*-

"""
NEXUS PIPELINE V3.7 - ARCHITECTURE COGNITIVE AVANCÉE
--------------------------------------------------------------------
1. Triage Sémantique : Scoring Python via dictionnaire de poids.
2. Atomic Claim : Utilisation optimisée de UPDATE ... RETURNING.
3. PID Backpressure Control : Régulation fine du débit (Flux tendu).
4. Auto-Scaling Batch : Ajustement continu de la taille des lots.
5. Short-Circuiting : Disqualification pré-IA basée sur l'expérience.
6. Smart Retry : Backoff exponentiel pour les erreurs transitoires.
7. Gravity Sorting : Priorité temporelle dynamique (Hot-Path).
8. Fast-Track (NEW) : Détection d'urgence et contournement IA pour vente directe.
"""

import time
import asyncio
import sys
import re
import json
from pathlib import Path

# --- BOOTLOADER: CONFIGURATION DU PATH ---
ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# --- IMPORTS ROBUSTES & SECURISES ---
try:
    from core.database import NexusDB
    from core.dispatcher import SponsorDispatcher
    from core.gemini_processor import HybridGenerator, GeminiJSONParser
    from core.prompts import get_qualification_prompt
    from core.logger_utils import setup_secure_logger

    try:
        from channels.tiktok.sniper import KeywordLearner
    except ImportError:
        KeywordLearner = None

    try:
        from config.rag_engine import RAGEngine
    except ImportError:

        class RAGEngine:
            def retrieve_context(self, txt):
                return ""

            def initialize(self):
                pass

except ImportError as e:
    print(f"🔴 ERREUR IMPORT PIPELINE: {e}")
    sys.exit(1)

# CONFIG LOGGING SÉCURISÉ
logger = setup_secure_logger("PipelineBridge")

# Instanciation globale
db = NexusDB()
rag = RAGEngine()
dispatcher_instance = SponsorDispatcher(db)
keyword_learner = KeywordLearner(db) if KeywordLearner else None

# --- INTELLIGENCE SEMANTIQUE (POIDS - STRATÉGIE V3) ---
# Mise à jour V3 : Pénalisation IOBSP (Banque/Crédit) & Boost Trading/PropFirm
KEYWORD_WEIGHTS = {
    # SIGNALS POSITIFS (Trading / Business / Urgence)
    "urgent": 10,
    "bloqué": 10,
    "problème": 8,
    "aide": 7,
    "argent": 5,
    "crypto": 8,
    "investir": 6,
    "tuto": 4,
    "trading": 15,
    "prop": 12,
    "challenge": 12,
    "funded": 10,
    "capital": 8,
    "funding": 8,
    "bitcoin": 6,
    "analyse": 5,
    "lien": 5,
    "mp": 3,
    "dispo": 2,
    # SIGNALS NEGATIFS (Noise / Spam / Hors-Cible IOBSP)
    "fake": -20,
    "arnaque": -50,
    "police": -100,
    "scam": -50,
    "bot": -10,
    # HARD BLOCK BANCAIRE (Conformité V3)
    "banque": -50,
    "crédit": -100,
    "prêt": -100,
    "taux": -20,
    "emprunt": -100,
    "immobilier": -20,
    "bourso": -50,
    "fortuneo": -50,  # Nettoyage Legacy
}

# --- LOGIQUE FAST-TRACK ---
# Regex pour détecter l'intention d'achat immédiate
INSTANT_CLOSE_REGEX = re.compile(
    r"(je veux|acheter|buy now|code promo|link please|lien svp|c'est ou|commencer|go dm)",
    re.IGNORECASE,
)

# --- ETAT GLOBAL PID & AUTO-SCALING ---
PIPELINE_STATE = {
    "previous_backlog": 0,
    "previous_time": time.time(),
    "current_batch_size": 20,
    "integral_error": 0.0,
    "target_processing_time": 15.0,
}


def recover_stale_leads():
    """
    Au démarrage, vérifie s'il y a des leads bloqués en 'PROCESSING_AI'.
    """
    try:
        logger.info("♻️ Vérification des leads zombies (Crash Recovery)...")
        with db.session() as conn:
            conn.execute(
                """
                UPDATE leads 
                SET status='NEW', updated_at=? 
                WHERE status='PROCESSING_AI'
            """,
                (time.time(),),
            )

            logger.info("✅ Maintenance de démarrage effectuée.")
    except Exception as e:
        logger.error(f"❌ Echec Recovery au démarrage: {e}")


async def watchdog_zombie_task():
    """
    Tâche de fond : Vérifie périodiquement les leads bloqués depuis trop longtemps (> 1h).
    """
    logger.info("🐕 Watchdog Zombie Task démarré.")
    while True:
        try:
            await asyncio.sleep(3600)  # Check toutes les heures
            cutoff = time.time() - 3600

            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _execute_zombie_cleanup, cutoff)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"⚠️ Erreur Watchdog: {e}")


def _execute_zombie_cleanup(cutoff_time):
    try:
        with db.session() as conn:
            conn.execute(
                """
                UPDATE leads 
                SET status='NEW', updated_at=? 
                WHERE status='PROCESSING_AI' AND updated_at < ?
            """,
                (time.time(), cutoff_time),
            )
    except Exception:
        pass


def get_backlog_size() -> int:
    try:
        with db.session() as conn:
            row = conn.execute("SELECT COUNT(*) as cnt FROM leads WHERE status='NEW'").fetchone()
            return row["cnt"] if db.use_postgres else row[0]
    except Exception:
        return 0


def fetch_and_claim_leads(limit: int = 50):
    """
    INTELLIGENCE N°1 & 3: TRIAGE SEMANTIQUE HYBRIDE & GRAVITE TEMPORELLE
    1. Récupération large (Batch x 4)
    2. Scoring Python (Mots clés + Decay Temporel)
    3. Claim atomique des Top-N
    """
    try:
        # 1. Fetch RAW (Lecture seule, pas de lock immédiat)
        fetch_limit = limit * 4
        with db.session() as conn:
            # On prend large pour pouvoir trier en Python
            cursor = conn.execute(
                """
                SELECT id, comment_text, source, author, created_at 
                FROM leads 
                WHERE status='NEW' 
                ORDER BY created_at ASC 
                LIMIT ?
            """,
                (fetch_limit,),
            )
            raw_leads = [dict(row) for row in cursor.fetchall()]

        if not raw_leads:
            return []

        # 2. Scoring Python (Heuristique fine + Gravité)
        scored_leads = []
        now = time.time()

        for lead in raw_leads:
            base_score = 0
            text = (lead.get("comment_text") or "").lower()

            # A. Analyse mots clés
            for word, weight in KEYWORD_WEIGHTS.items():
                if word in text:
                    base_score += weight

            # B. Bonus Canal
            if "TIKTOK" in lead.get("source", ""):
                base_score += 5

            # C. Filtre longueur
            if len(text) < 5:
                base_score -= 10

            # D. Intelligence Temporelle (Gravity Decay)
            # Un lead vieux de 1h vaut moins qu'un lead vieux de 1min
            created_at = lead.get("created_at", now)
            if isinstance(created_at, str):  # Fallback si parsing bizarre
                try:
                    created_at = float(created_at)
                except Exception:
                    created_at = now

            age_seconds = max(1, now - created_at)
            age_hours = age_seconds / 3600.0

            # Formule de Gravité : Score / (1 + Age_Heures)
            # Un lead "Urgent" (Score 20) vieux de 24h devient Score ~0.8
            # Un lead "Moyen" (Score 5) vieux de 1min reste Score ~5
            # Cela force le traitement immédiat des nouveaux entrants ("Hot Path")
            final_score = base_score / (1.0 + age_hours)

            scored_leads.append((final_score, lead))

        # 3. Tri (Score décroissant) - Le plus chaud et pertinent en premier
        scored_leads.sort(key=lambda x: x[0], reverse=True)
        top_leads = [x[1] for x in scored_leads[:limit]]
        top_ids = [lead["id"] for lead in top_leads]

        if not top_ids:
            return []

        # 4. Claim Atomique (Mise à jour d'état)
        with db.session(immediate=True) as conn:
            placeholders = ",".join(["?"] * len(top_ids))

            if db.use_postgres:
                # Version PG (plus propre)
                query = f"""
                    UPDATE leads 
                    SET status='PROCESSING_AI', updated_at=%s
                    WHERE id IN ({placeholders})
                    RETURNING id, comment_text, source, author, url, ai_process_info
                """
                cursor = conn.execute(query, [time.time()] + top_ids)
                return [dict(row) for row in cursor.fetchall()]
            else:
                # Version SQLite
                conn.execute(
                    f"""
                    UPDATE leads 
                    SET status='PROCESSING_AI', updated_at=? 
                    WHERE id IN ({placeholders})
                """,
                    [time.time()] + top_ids,
                )

                # Re-select pour retourner les données complètes
                cursor = conn.execute(
                    f"""
                    SELECT id, comment_text, source, author, url, ai_process_info
                    FROM leads WHERE id IN ({placeholders})
                """,
                    top_ids,
                )
                return [dict(row) for row in cursor.fetchall()]

    except Exception as e:
        logger.error(f"❌ DB Claim Error (Global): {e}")
        return []


def extract_meaningful_keywords(text: str) -> list:
    if not text:
        return []
    text = text.lower()
    known_keywords = list(KEYWORD_WEIGHTS.keys())
    found = []
    for kw in known_keywords:
        if kw in text:
            found.append(kw)
    return found


async def _process_single_lead_task(lead: dict, hybrid_gen: HybridGenerator):
    lead_id = lead.get("id")
    context_txt = lead.get("comment_text") or lead.get("text") or ""

    try:
        # --- INTELLIGENCE N°3 : SHORT-CIRCUITING & FAST-TRACK ---

        # 1. Filtre Technique (Trop court)
        if len(context_txt) < 3:
            with db.session() as conn:
                conn.execute(
                    "UPDATE leads SET status='SKIP', updated_at=? WHERE id=?",
                    (time.time(), lead_id),
                )
            return

        # 2. Filtre Sécurité (Blacklist)
        blacklist = ["arnaque", "fake", "bot", "scam", "police"]
        if any(bad in context_txt.lower() for bad in blacklist):
            with db.session() as conn:
                conn.execute(
                    "UPDATE leads SET status='SKIP', updated_at=? WHERE id=?",
                    (time.time(), lead_id),
                )
            return

        # 3. FAST-TRACK (Déterministe)
        # Si le lead dit "je veux acheter", on ne perd pas de temps/argent avec l'IA.
        if INSTANT_CLOSE_REGEX.search(context_txt):
            logger.info(
                f"⚡ FAST-TRACK: Lead {lead_id[:8]} identifié comme 'Haute Intention'. Bypass IA."
            )

            # On qualifie directement avec un score de confiance maximal
            ai_data = {
                "status": "QUALIFIED",
                "confidence": 1.0,
                "reasoning": "Determinisitc Regex Match (Fast-Track)",
                "intent_category": "NEED",
            }

            with db.session() as conn:
                conn.execute(
                    """
                    UPDATE leads 
                    SET status='QUALIFIED', ai_confidence=1.0, ai_process_info=?, updated_at=?
                    WHERE id=?
                """,
                    (json.dumps(ai_data), time.time(), lead_id),
                )
            return  # Fin de traitement pour ce lead

        # --- FLUX IA STANDARD ---
        rag_context = ""
        if hasattr(rag, "retrieve_context"):
            if asyncio.iscoroutinefunction(rag.retrieve_context):
                rag_context = await rag.retrieve_context(context_txt)
            else:
                rag_context = await asyncio.to_thread(rag.retrieve_context, context_txt)

        source = lead.get("source", "TIKTOK").upper()
        p_type = "TIKTOK" if "TIKTOK" in source else "GENERIC"

        prompt = get_qualification_prompt(lead, p_type, rag_context)
        final_prompt = f"{prompt}\n\nIMPORTANT: OUTPUT STRICT JSON."

        raw_resp, ai_source = await hybrid_gen.generate(
            final_prompt, lead.get("comment_text", ""), headless_connector=None
        )

        if not raw_resp:
            logger.warning(f"⚠️ Echec génération pour {lead_id}.")
            db.fail_lead(lead_id, "AI_NO_RESPONSE")
            return

        parsed = GeminiJSONParser.parse(raw_resp)
        status = parsed.get("status", "SKIP").upper()
        draft = parsed.get("draft", "")
        confidence = parsed.get("confidence", 0.0)

        if status not in ["QUALIFIED", "SKIP"]:
            status = "SKIP"

        with db.session() as conn:
            meta_str = json.dumps(parsed)
            conn.execute(
                """
                UPDATE leads 
                SET status=?, ai_draft=?, ai_confidence=?, ai_process_info=?, updated_at=?
                WHERE id=?
            """,
                (status, draft, confidence, meta_str, time.time(), lead_id),
            )

        if (
            status == "QUALIFIED"
            and keyword_learner
            and hasattr(keyword_learner, "update_word_hit")
        ):
            pass  # Feedback loop activé dans le module sniper

        logger.info(f"   🤖 {lead_id[:8]} -> {status} ({int(confidence*100)}%) via {ai_source}")

    except Exception as e:
        # --- INTELLIGENCE N°5 : SMART RETRY ---
        is_transient = (
            isinstance(e, (asyncio.TimeoutError, ConnectionError)) or "timeout" in str(e).lower()
        )

        current_info = lead.get("ai_process_info")
        info_dict = {}
        try:
            info_dict = (
                json.loads(current_info) if isinstance(current_info, str) else (current_info or {})
            )
        except Exception:
            pass

        retry_count = info_dict.get("retry_count", 0)

        if is_transient and retry_count < 3:
            delay = 2**retry_count
            logger.warning(f"♻️ Smart Retry ({retry_count+1}/3) pour {lead_id} dans {delay}s...")

            try:
                info_dict["retry_count"] = retry_count + 1
                with db.session() as conn:
                    conn.execute(
                        """
                        UPDATE leads 
                        SET status='NEW', updated_at=?, ai_process_info=?
                        WHERE id=?
                    """,
                        (time.time() + delay, json.dumps(info_dict), lead_id),
                    )
            except Exception:
                pass
        else:
            logger.error(f"   💥 Erreur Fatale Lead {lead_id}: {e}")
            db.fail_lead(lead_id, "PIPELINE_ERROR")


async def run_ai_processing_cycle():
    """
    Traite les leads avec régulation PID.
    """
    current_size = PIPELINE_STATE["current_batch_size"]
    start_time = time.time()

    batch = await asyncio.to_thread(fetch_and_claim_leads, limit=int(current_size))

    if not batch:
        return False, 0.0

    logger.info(
        f"🧠 Pipeline: Processing Batch {len(batch)} leads (Target Size: {int(current_size)})..."
    )

    hybrid_gen = HybridGenerator()
    tasks = []
    for lead in batch:
        tasks.append(_process_single_lead_task(lead, hybrid_gen))

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    duration = time.time() - start_time

    # PID CONTROLLER LOGIC
    target = PIPELINE_STATE["target_processing_time"]
    error = target - duration

    Kp = 0.5
    Ki = 0.1

    PIPELINE_STATE["integral_error"] += error
    PIPELINE_STATE["integral_error"] = max(-50, min(50, PIPELINE_STATE["integral_error"]))

    correction = (Kp * error) + (Ki * PIPELINE_STATE["integral_error"])
    new_size = current_size + correction
    new_size = max(5, min(100, new_size))

    PIPELINE_STATE["current_batch_size"] = new_size

    if abs(correction) > 2.0:
        logger.debug(
            f"🎛️ PID Regulator: Batch {int(current_size)} -> {int(new_size)} (Error: {error:.2f}s)"
        )

    try:
        if keyword_learner and hasattr(keyword_learner, "save"):
            await asyncio.to_thread(keyword_learner.save)
    except Exception:
        pass

    return True, duration


async def run_dispatch_cycle():
    try:
        await asyncio.to_thread(dispatcher_instance.process_dispatch_cycle)
    except Exception as e:
        logger.error(f"❌ Erreur Dispatch Cycle: {e}")


async def run_pipeline():
    logger.info("💎 NEXUS PIPELINE V3.7 STARTED (PID + GRAVITY + FAST-TRACK).")

    if hasattr(rag, "initialize"):
        if asyncio.iscoroutinefunction(rag.initialize):
            await rag.initialize()
        else:
            rag.initialize()

    recover_stale_leads()

    watchdog_task = asyncio.create_task(watchdog_zombie_task())

    try:
        while True:
            try:
                backlog = await asyncio.to_thread(get_backlog_size)
                now = time.time()

                tasks = [
                    run_ai_processing_cycle(),
                    run_dispatch_cycle(),
                ]

                results = await asyncio.gather(*tasks, return_exceptions=True)

                process_result = results[0]
                did_process_leads = False
                cycle_duration = 0.0

                if isinstance(process_result, tuple):
                    did_process_leads, cycle_duration = process_result
                elif isinstance(process_result, bool):
                    did_process_leads = process_result

                # PID Backpressure Sleep Logic
                delta_t = now - PIPELINE_STATE["previous_time"]
                if delta_t == 0:
                    delta_t = 0.001

                velocity = (backlog - PIPELINE_STATE["previous_backlog"]) / delta_t

                PIPELINE_STATE["previous_backlog"] = backlog
                PIPELINE_STATE["previous_time"] = now

                base_sleep = 5.0

                if backlog > 500:
                    final_sleep = 0.1
                    logger.warning(f"🚨 BACKLOG CRITIQUE ({backlog}). Turbo Mode.")
                elif backlog == 0 and not did_process_leads:
                    final_sleep = 30.0
                else:
                    if velocity > 0.5:
                        final_sleep = 1.0
                    elif velocity < -0.5:
                        final_sleep = 10.0
                    else:
                        final_sleep = base_sleep

                await asyncio.sleep(final_sleep)

            except asyncio.CancelledError:
                # Re-raise pour sortir de la boucle et atteindre le bloc finally
                raise
            except Exception as e:
                logger.critical(f"🔥 Crash Loop Pipeline: {e}")
                await asyncio.sleep(10)

    except asyncio.CancelledError:
        logger.info("🛑 Arrêt pipeline demandé — nettoyage en cours...")
    finally:
        # 1. Annuler la tâche watchdog proprement
        if not watchdog_task.done():
            watchdog_task.cancel()
            try:
                await watchdog_task
            except asyncio.CancelledError:
                pass

        # 2. Persister l'état du keyword learner
        if keyword_learner is not None:
            try:
                await asyncio.to_thread(keyword_learner.save)
                logger.info("💾 KeywordLearner state saved.")
            except Exception as e:
                logger.error(f"⚠️ KeywordLearner save failed: {e}")

        # 3. Fermer les connexions DB proprement
        try:
            db.close()
            logger.info("🔒 DB connections closed.")
        except Exception as e:
            logger.error(f"⚠️ DB close failed: {e}")

        logger.info("✅ Pipeline shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(run_pipeline())
    except KeyboardInterrupt:
        pass
