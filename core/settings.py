### prospection/core/settings.py
import os
import sys
import shutil
import glob
from pathlib import Path
from typing import List, Dict, Optional, Any
from pydantic import Field, validator, root_validator, SecretStr
# FIX CRITIQUE: Compatibilité Pydantic V2 (installé via pydantic-settings dans le Dockerfile)
try:
    from pydantic_settings import BaseSettings
except ImportError:
    from pydantic import BaseSettings

# Définition du chemin racine (3 niveaux au-dessus : core/ -> src/ -> root)
BASE_DIR = Path(__file__).resolve().parent.parent

class Settings(BaseSettings):
    # --- INFRASTRUCTURE & PATHS ---
    BASE_DIR: Path = BASE_DIR
    LOGS_DIR: Path = BASE_DIR / "logs"
    DB_PATH: Path = BASE_DIR / "tiktok_guerilla.db"
    # CHROME_BIN peut être None initialement, l'auto-repair s'en chargera
    CHROME_BIN: Optional[str] = os.getenv("CHROME_BIN")
    CHROME_PROFILES_DIR: Path = BASE_DIR / "chrome_profiles"
    ACCOUNTS_IMPORT_DIR: Path = BASE_DIR / "import"
    LOCKS_DIR: Path = BASE_DIR / "locks" 

    # --- DATABASE CONFIGURATION (HIGH MRR REQUIREMENT) ---
    # AUDIT FIX: Passage en True par défaut pour la Prod 10K (PostgreSQL Obligatoire)
    USE_POSTGRES: bool = True 
    POSTGRES_USER: str = "postgres"
    # SECURITE: Valeur par défaut pour éviter le crash en dev/test si non fourni.
    # En PROD, cette valeur DOIT être écrasée par une variable d'environnement réelle.
    POSTGRES_PASSWORD: SecretStr = Field(default=SecretStr("postgres"), env="POSTGRES_PASSWORD")
    POSTGRES_DB: str = "apex_db"
    POSTGRES_HOST: str = "localhost"
    # FIX AUDIT: Typage strict int pour compatibilité drivers
    POSTGRES_PORT: int = 5432
    
    # Tuning automatique via smart_db_pool_sizing
    DB_POOL_MIN: int = 20
    DB_POOL_MAX: int = 100 

    # --- SCALING & CONCURRENCY ---
    # Ajusté dynamiquement via auto_scale_workers_based_on_resources
    MAX_CONCURRENT_WORKERS: int = 50 
    HEADLESS_MODE: bool = True 

    # --- NETWORK & PROXIES ---
    ROTATING_PROXY_URL: Optional[str] = None 
    MIN_ACCOUNT_THRESHOLD: int = 15
    
    # --- SECURITY & API ---
    # CORS Security : Liste des domaines autorisés pour le Redirector
    ALLOWED_ORIGINS: List[str] = Field(default=["*"], env="ALLOWED_ORIGINS")
    
    # SECURITE : Pas de valeur par défaut pour la clé maître, crash volontaire si absente
    SECURITY_MASTER_KEY: SecretStr = Field(..., env="SECURITY_MASTER_KEY")
    CAPTCHA_API_KEY: Optional[str] = None
    
    # --- SMART GEO-ROUTING (V3.0 INFRASTRUCTURE) ---
    # Requis pour le routage IP (US -> Prop Firm / FR -> PSAN)
    GEOIP_API_KEY: Optional[SecretStr] = Field(default=None, env="GEOIP_API_KEY")
    GEOIP_PROVIDER: str = Field(default="ipstack", env="GEOIP_PROVIDER") # Options: ipstack, maxmind
    DEFAULT_ROUTING_ZONE: str = "GLOBAL" # Zone de repli si échec détection
    
    # --- SUPPLY CHAIN AUTOMATION ---
    ACCOUNTS_PROVIDER_API_URL: Optional[str] = None
    ACCOUNTS_PROVIDER_API_KEY: Optional[SecretStr] = None
    PROVIDER_LEAD_TIME_SECONDS: int = 3600 

    # --- TIKTOK SPECIFIC ---
    CDP_TIKTOK_PORT: int = 9222
    TIKTOK_API_PATTERNS: List[str] = [
        "/api/comment/list",
        "/api/recommend/item_list",
        "/api/commit/follow/user",
        "/api/user/detail"
    ]

    # --- REDDIT SPECIFIC ---
    CDP_REDDIT_PORT: int = 9223
    # Ajout Failover Port pour robustesse
    CDP_REDDIT_FAILOVER: int = 9224

    # --- AI & CONTENT ---
    OPENAI_API_KEY: Optional[SecretStr] = None
    GEMINI_API_KEY: Optional[SecretStr] = None
    POSTBACK_SECRET_TOKEN: SecretStr = SecretStr("POSTBACK_DEFAULT_DEV_TOKEN")
    
    # --- REVENUE STREAMS (AGENCY MODEL) ---
    # URL de retour après redirection (Dashboard Partenaire / Offres)
    # MODIFIED V3: Rebranded to generic media name (Nexus Insights) to avoid "Finance" keyword
    PARTNER_DASHBOARD_URL: str = Field(default="https://partenaires.nexus-insights.com/dashboard", env="PARTNER_DASHBOARD_URL")

    # --- RAG & KNOWLEDGE ---
    KNOWLEDGE_BASE_PATH: Path = BASE_DIR / "config" / "knowledge_base.json"
    
    # --- INTELLIGENCE & FEEDBACK LOOP ---
    # Fichier partagé pour les ordres de chasse (Orchestrator -> Hunter)
    HUNTING_ORDERS_PATH: Path = BASE_DIR / "config" / "hunting_orders.json"

    # =========================================================================
    # INTELLIGENCE & AUTO-CONFIGURATION (V25.1 - AUDITED & SCALABLE)
    # =========================================================================

    @root_validator(pre=True)
    def detect_environment_context(cls, values):
        """
        Intelligence N°3 : Détection de Contexte (Docker vs Desktop).
        Active le Headless Mode automatiquement en conteneur.
        """
        # 1. Détection Docker (Fichier .dockerenv présent à la racine)
        is_docker = os.path.exists("/.dockerenv") or os.environ.get("DOCKER_CONTAINER") == "1"
        
        # 2. Auto-configuration Headless
        if is_docker:
            values["HEADLESS_MODE"] = True
            # En Docker, on sait souvent où est Chrome
            if not values.get("CHROME_BIN"):
                # Fallback standard Linux/Docker
                values["CHROME_BIN"] = "/usr/bin/google-chrome"
        else:
            # Sur Desktop, si HEADLESS n'est pas forcé explicitement, on le met à False pour le debug visuel
            if "HEADLESS_MODE" not in values:
                values["HEADLESS_MODE"] = False
                
        return values

    @validator("MAX_CONCURRENT_WORKERS", pre=True, always=True)
    def auto_scale_workers_based_on_resources(cls, v, values):
        """
        Intelligence N°1 (AMÉLIORÉE) : Auto-Dimensionnement CPU & RAM.
        Calcule le nombre optimal de bots en fonction du facteur limitant (CPU ou RAM).
        Optimisé pour VPS low-cost et Serveurs dédiés.
        """
        try:
            import psutil
            import os
            
            # --- 1. FACTEUR RAM ---
            # Récupérer la RAM totale en Go
            total_ram_gb = psutil.virtual_memory().total / (1024 ** 3)
            # On laisse 2 Go pour l'OS, la DB et le Dispatcher
            available_ram_for_bots = max(0, total_ram_gb - 2)
            # Heuristique RAM : 1 Worker Chrome ~= 350-400 Mo (sécurité accrue)
            # Ratio : ~2.5 bots par Go disponible
            ram_limit = int(available_ram_for_bots * 2.5)
            
            # --- 2. FACTEUR CPU ---
            # Récupérer le nombre de cœurs logiques
            cpu_count = os.cpu_count() or 1
            # Heuristique CPU : Chrome mange du CPU au chargement. 
            # On autorise environ 3 à 4 bots par cœur logique pour ne pas saturer le load average.
            cpu_limit = cpu_count * 4
            
            # --- 3. CALCUL DU PLAFOND (Le plus petit des deux gagne) ---
            safe_limit = min(ram_limit, cpu_limit)
            
            # Gestion de la valeur par défaut ou fournie par l'utilisateur (.env)
            try:
                requested = int(v) if v is not None else 50
            except (ValueError, TypeError):
                requested = 50

            # --- 4. ARBITRAGE ---
            if requested > safe_limit:
                if safe_limit < 1: safe_limit = 1 # Toujours au moins 1 bot
                # Log via print car le logger n'est pas encore instancié à ce stade
                print(f"⚠️ INTELLIGENCE SYSTEM: Ressources limitées détectées.", flush=True)
                print(f"   - RAM: {total_ram_gb:.1f} GB (Supporte ~{ram_limit} bots)", flush=True)
                print(f"   - CPU: {cpu_count} Cores (Supporte ~{cpu_limit} bots)", flush=True)
                print(f"👉 Auto-correction : {requested} -> {safe_limit} workers (Facteur limitant : {'CPU' if cpu_limit < ram_limit else 'RAM'}).", flush=True)
                return safe_limit
                
            return requested
            
        except ImportError:
            print("⚠️ 'psutil' manquant. Auto-scaling désactivé (Risque de crash).")
            return v

    @root_validator(skip_on_failure=True)
    def smart_db_pool_sizing(cls, values):
        """
        Intelligence N°2 : Déduction de Configuration (Workflows).
        FIX CRITIQUE V25.1 : Empêche l'explosion du pool de connexions (PgBouncer logic).
        AUDIT FIX : Calibrage pour 50 Bots simultanés.
        """
        workers = values.get("MAX_CONCURRENT_WORKERS", 1)
        is_worker_process = os.environ.get("IS_WORKER_PROCESS") == "true"
        
        if is_worker_process:
            # --- MODE WORKER (Sender, Sniper, etc.) ---
            # Un worker ne doit JAMAIS consommer plus d'une ou deux connexions.
            # Avec 50 workers, cela fait 50-100 connexions.
            values["DB_POOL_MIN"] = 1
            values["DB_POOL_MAX"] = 2
        else:
            # --- MODE LAUNCHER / API ---
            # Le processus maître a besoin d'un pool plus large pour le dispatch MAB.
            # Augmentation significative pour supporter la charge de 50 bots rapportant au QG.
            values["DB_POOL_MIN"] = min(20, workers)
            values["DB_POOL_MAX"] = min(120, int(workers * 2) + 20)
            
        return values

    @validator("CHROME_BIN", always=True)
    def self_heal_chrome_path(cls, v):
        """
        Intelligence N°4 : Auto-Réparation des Chemins (Playwright Docker Fix).
        Cherche Chrome si le chemin configuré est invalide.
        """
        # 1. Priorité absolue : Variable d'environnement (Fix Docker)
        env_bin = os.getenv("CHROME_BIN")
        if env_bin and os.path.exists(env_bin):
            return env_bin

        # 2. Si le chemin fourni dans .env existe, c'est validé
        if v and os.path.exists(v):
            return v
            
        # 3. Stratégie de repli (Auto-Repair)
        print(f"[CHROME] Configured path '{v}' not found. Attempting auto-discovery...", flush=True)

        # A. Recherche Playwright spécifique (Docker)
        playwright_path = os.environ.get("PLAYWRIGHT_BROWSERS_PATH", "/ms-playwright")
        if os.path.exists(playwright_path):
            found = glob.glob(f"{playwright_path}/**/chrome-linux/chrome", recursive=True)
            if found:
                print(f"[CHROME] Playwright Chrome found: {found[0]}", flush=True)
                return found[0]
            found = glob.glob(f"{playwright_path}/**/chrome", recursive=True)
            if found:
                for f in found:
                    if os.access(f, os.X_OK) and "driver" not in f:
                        print(f"[CHROME] Playwright Chrome found (fallback): {f}", flush=True)
                        return f

        # B. Recherche dans le PATH système
        chk = shutil.which("google-chrome") or shutil.which("chrome") or shutil.which("chromium")
        if chk:
            print(f"[CHROME] Found in PATH: {chk}", flush=True)
            return chk

        # C. Recherche aux endroits standards (Windows/Linux/Mac)
        common_paths = [
            "/usr/bin/google-chrome",
            "/opt/google/chrome/google-chrome",
            "/usr/bin/chromium-browser",
            "/usr/bin/chromium",
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
        ]

        for path in common_paths:
            if os.path.exists(path):
                print(f"[CHROME] Found in standard location: {path}", flush=True)
                return path

        # Si on est ici, c'est critique
        if v:
            raise ValueError(f"CRITICAL: Chrome binary not found at '{v}' and auto-discovery failed.")

        return None

    @validator("USE_POSTGRES", always=True)
    def force_postgres_for_high_concurrency(cls, v, values):
        """Bloque le démarrage si on tente >10 workers sur SQLite."""
        workers = values.get("MAX_CONCURRENT_WORKERS", 1)
        if workers > 10 and not v:
            raise ValueError("🚨 SÉCURITÉ : Impossible de lancer plus de 10 workers sur SQLite. Activez PostgreSQL ou réduisez MAX_CONCURRENT_WORKERS.")
        return v

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True
        extra = "ignore"

# Instanciation unique
try:
    settings = Settings()
except Exception as e:
    print(f"[ERREUR CONFIGURATION]: {e}", file=sys.stderr)
    sys.exit(1)

# Création des répertoires critiques au démarrage
settings.LOGS_DIR.mkdir(parents=True, exist_ok=True)
settings.CHROME_PROFILES_DIR.mkdir(parents=True, exist_ok=True)
settings.ACCOUNTS_IMPORT_DIR.mkdir(parents=True, exist_ok=True)
settings.LOCKS_DIR.mkdir(parents=True, exist_ok=True)
# S'assurer que le dossier config existe pour les hunting_orders
(settings.BASE_DIR / "config").mkdir(parents=True, exist_ok=True)