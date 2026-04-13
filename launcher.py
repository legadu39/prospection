### prospection/launcher.py
# launcher.py - NEXUS SERVICE ORCHESTRATOR V28.0 (IMMUNITY & MUTATION)
# -*- coding: utf-8 -*-

"""
NEXUS SERVICE ORCHESTRATOR V28.0
--------------------------------
Orchestrateur de services pour l'agence média Nexus.
Gère le cycle de vie des composants d'acquisition (Bots) et de monétisation (AdServer).

AMÉLIORATIONS INTELLIGENCE (V28.0) :
1. Crash Budget : Tolérance aux pannes calculée par minute glissante.
2. Backoff Exponentiel : Temporisation progressive en cas d'instabilité.
3. Mutation d'Environnement (NEW) : Rotation IP/Cache proactive avant relance critique.
4. Logging Unifié : Capture centralisée des sorties standard/erreur.
5. Graceful Shutdown : Arrêt propre des sous-processus.
"""

import sys
import os
import time
import threading
import subprocess
import signal
import queue
import logging
import shutil
from datetime import datetime
from collections import deque
from pathlib import Path

# Tentative d'import psutil pour gestion fine des processus
try:
    import psutil
except ImportError:
    print("⚠️ Module 'psutil' manquant. Installation recommandée pour un arrêt propre.")
    psutil = None

# --- CONFIGURATION DU CHEMIN ---
ROOT_DIR = Path(__file__).resolve().parent
sys.path.append(str(ROOT_DIR))

try:
    from core.database import NexusDB
    from core.settings import settings
    # On importe le SupplyChainManager s'il est disponible
    try:
        from core.supply_chain_manager import FleetManager
        HAS_SUPPLY_CHAIN = True
    except ImportError:
        HAS_SUPPLY_CHAIN = False
except ImportError as e:
    print(f"🔴 ERREUR CRITIQUE IMPORT: {e}")
    sys.exit(1)

# --- CONFIGURATION LOGGING ---
LOG_DIR = settings.LOGS_DIR
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | ORCHESTRATOR | %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "orchestrator.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Orchestrator")

# --- DÉFINITION DES SERVICES ---
SERVICES = {
    # Cœur du système (Priorité Haute)
    "ad_exchange": {
        "cmd": ["uvicorn", "core.ad_exchange_server:app", "--host", "0.0.0.0", "--port", "8000"],
        "type": "server",
        "critical": True,
        "restart_policy": "always"
    },
    "pipeline_bridge": {
        "cmd": [sys.executable, "pipeline_bridge.py"],
        "type": "worker",
        "critical": True,
        "restart_policy": "always"
    },
    # Canaux d'acquisition (Priorité Moyenne - Tolérance aux pannes)
    "tiktok_sniper": {
        "cmd": [sys.executable, "channels/tiktok/sniper.py"],
        "type": "bot",
        "critical": False,
        "restart_policy": "smart",
        "mutation_capable": True  # Eligible à la rotation IP
    },
    "tiktok_sender": {
        "cmd": [sys.executable, "channels/tiktok/sender.py"],
        "type": "bot",
        "critical": False,
        "restart_policy": "smart",
        "mutation_capable": True
    },
    "reddit_listener": {
        "cmd": [sys.executable, "channels/reddit/audience_listener.py"],
        "type": "bot",
        "critical": False,
        "restart_policy": "smart",
        "mutation_capable": False
    }
}

# --- CLASSE D'ORCHESTRATION INTELLIGENTE ---

class ServiceOrchestrator:
    def __init__(self):
        self.processes = {}
        self.log_queues = {}
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        
        # INTELLIGENCE N°3 : CRASH BUDGET MEMORY
        # Structure: { "service_name": deque([timestamp1, timestamp2], maxlen=10) }
        self.crash_history = {} 
        
        # Configuration Résilience
        self.CRASH_BUDGET_WINDOW = 60.0 # Fenêtre de surveillance (secondes)
        self.MAX_CRASHES_PER_MIN = 5    # Seuil critique avant abandon
        self.MUTATION_THRESHOLD = 3     # Seuil avant tentative de mutation (changement IP)
        self.BACKOFF_BASE = 4.0         # Temps d'attente de base (secondes)

    def _log_reader(self, name, pipe, level):
        """Lit les logs des sous-processus et les redirige."""
        with pipe:
            for line in iter(pipe.readline, b''):
                try:
                    msg = line.decode('utf-8', errors='replace').strip()
                    if msg:
                        # On préfixe avec le nom du service pour clarté
                        logger.log(level, f"[{name}] {msg}")
                except Exception:
                    pass

    def _mutate_environment(self, service_name: str):
        """
        Stratégie d'Auto-Réparation Active : 
        Modifie l'environnement (IP, Cache) si un service échoue trop souvent.
        """
        logger.warning(f"💉 IMMUNITY: Injection d'une mutation pour sauver {service_name}...")
        
        try:
            # 1. Purge des fichiers temporaires / Cache Chrome
            profile_path = Path(f"/tmp/chrome_profiles/{service_name}")
            if profile_path.exists():
                shutil.rmtree(profile_path, ignore_errors=True)
                logger.info(f"   🧹 Cache purgé pour {service_name}")

            # 2. Rotation IP (si le module existe)
            rotator_script = ROOT_DIR / "core" / "mobile_rotator.py"
            if rotator_script.exists():
                logger.info("   🔄 Appel du Mobile Rotator...")
                subprocess.run(
                    [sys.executable, str(rotator_script), "--force-rotate"],
                    check=False, timeout=10
                )
            
            logger.info("✅ Mutation terminée. Environnement assaini.")
            return True
        except Exception as e:
            logger.error(f"❌ Echec de la mutation: {e}")
            return False

    def _assess_service_health(self, name: str) -> float:
        """
        Calcule le temps d'attente (Backoff) et décide des actions correctives.
        Retourne :
          0.0 -> Redémarrage immédiat
          >0.0 -> Attente (Backoff)
          -1.0 -> Abandon (Trop instable)
        """
        now = time.time()
        
        if name not in self.crash_history:
            self.crash_history[name] = deque(maxlen=10)
            
        history = self.crash_history[name]
        
        # 1. Nettoyage de l'historique (Fenêtre glissante)
        while len(history) > 0 and (now - history[0] > self.CRASH_BUDGET_WINDOW):
            history.popleft()
            
        # 2. Enregistrement du crash actuel
        history.append(now)
        crashes_recent = len(history)
        
        config = SERVICES.get(name, {})
        
        # 3. Logique de Décision (Auto-Réparation vs Protection)
        if crashes_recent > self.MAX_CRASHES_PER_MIN:
            logger.critical(f"🛑 SERVICE {name} INSTABLE ({crashes_recent} crashs/min). Arrêt de sécurité.")
            return -1.0 # Abandon
        
        # INTELLIGENCE : Trigger de Mutation
        if crashes_recent >= self.MUTATION_THRESHOLD and config.get("mutation_capable"):
            self._mutate_environment(name)
            # Après une mutation, on peut réduire légèrement le backoff car on espère avoir réglé le problème
            return 2.0 
            
        elif crashes_recent > 2:
            # Backoff Exponentiel : 2^2=4s, 2^3=8s, 2^4=16s...
            wait_time = self.BACKOFF_BASE * (2 ** (crashes_recent - 2))
            wait_time = min(wait_time, 300) # Plafond à 5 min
            logger.warning(f"⚠️ Service {name} instable. Backoff {wait_time}s avant relance.")
            return wait_time
            
        return 0.0 # Redémarrage immédiat

    def start_service(self, name, config):
        """Lance un service individuel."""
        cmd = config["cmd"]
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        
        try:
            # Création du processus
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                cwd=str(ROOT_DIR) # S'assurer d'être à la racine
            )
            
            self.processes[name] = process
            
            # Threads de lecture de logs non-bloquants
            t_out = threading.Thread(target=self._log_reader, args=(name, process.stdout, logging.INFO))
            t_err = threading.Thread(target=self._log_reader, args=(name, process.stderr, logging.ERROR))
            t_out.daemon = True
            t_err.daemon = True
            t_out.start()
            t_err.start()
            
            logger.info(f"✅ Service démarré : {name} (PID: {process.pid})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Echec démarrage {name}: {e}")
            return False

    def monitor(self):
        """Boucle principale de surveillance."""
        logger.info("🔭 Surveillance active des services...")
        
        # Démarrage initial
        for name, config in SERVICES.items():
            self.start_service(name, config)
            time.sleep(0.5) # Ramp-up pour éviter surcharge CPU
            
        while not self.stop_event.is_set():
            with self.lock:
                active_procs = list(self.processes.items())
                
            for name, proc in active_procs:
                if proc.poll() is not None: # Le processus est mort
                    exit_code = proc.returncode
                    
                    # INTELLIGENCE : Analyse d'impact & Mutation
                    backoff = self._assess_service_health(name)
                    
                    if backoff < 0:
                        # Cas critique : Abandon
                        config = SERVICES[name]
                        if config.get("critical", False):
                            logger.critical(f"💀 Service Critique {name} mort définitivement. Arrêt global.")
                            self.stop_event.set()
                            break
                        else:
                            logger.error(f"🚫 Service non-critique {name} désactivé.")
                            with self.lock:
                                del self.processes[name]
                            continue
                            
                    elif backoff > 0:
                        # Cas instable : Attente intelligente
                        logger.info(f"⏳ Attente de refroidissement pour {name}...")
                        time.sleep(backoff)
                    
                    # Tentative de redémarrage
                    logger.warning(f"🔄 Redémarrage de {name} (Code {exit_code})...")
                    with self.lock:
                        del self.processes[name]
                    
                    self.start_service(name, SERVICES[name])
            
            time.sleep(1.0) # Tick de surveillance

    def kill_all(self):
        """Arrêt propre et total (Graceful Shutdown)."""
        logger.info("💀 Arrêt demandé. Terminaison des services...")
        self.stop_event.set()
        
        with self.lock:
            procs = list(self.processes.values())
            
        # 1. SIGTERM (Demande polie)
        for p in procs:
            try:
                p.terminate()
            except Exception:
                pass
                
        # Attente pour fermeture propre
        if psutil:
            try:
                gone, alive = psutil.wait_procs([psutil.Process(p.pid) for p in procs if p.pid], timeout=3)
            except Exception:
                time.sleep(3)
        else:
            time.sleep(3)
            
        # 2. SIGKILL (Force brute si nécessaire)
        for p in procs:
            try:
                if p.poll() is None:
                    p.kill()
            except Exception:
                pass
        
        logger.info("👋 Fin de l'orchestration.")

def main():
    # Nettoyage terminal
    if os.name == 'nt':
        os.system('cls')
    else:
        os.system('clear')
        
    print("="*60)
    print("🚀 NEXUS ORCHESTRATOR V28.0 - IMMUNITY EDITION")
    print(f"📂 Logs: {LOG_DIR}")
    print("="*60 + "\n")
    
    # Init DB une seule fois
    NexusDB.init_db_once()
    db = NexusDB()
    
    orchestrator = ServiceOrchestrator()
    
    # Démarrage du Yield Optimizer (Supply Chain) en thread de fond si disponible
    if HAS_SUPPLY_CHAIN:
        logger.info("🧠 Démarrage du Yield Optimizer (Supply Chain Manager)...")
        yield_manager = FleetManager(db)
        t_yield = threading.Thread(target=yield_manager.run_monitor_loop)
        t_yield.daemon = True
        t_yield.start()
    else:
        logger.warning("⚠️ Module Supply Chain manquant. Mode dégradé.")

    try:
        orchestrator.monitor()
    except KeyboardInterrupt:
        logger.info("🛑 Interruption clavier reçue.")
    except Exception as e:
        logger.critical(f"🔥 Orchestrator Crash Loop: {e}")
    finally:
        orchestrator.kill_all()
        db.close_local_connection()

if __name__ == "__main__":
    main()