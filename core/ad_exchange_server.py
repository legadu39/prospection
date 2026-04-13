# core/ad_exchange_server.py - NEXUS AD EXCHANGE V3.3 (COMPLIANCE HARDENING & SMART YIELD)
# Infrastructure d'Intermédiation Publicitaire & Gestion de Mandats.
# Assure l'attribution programmatique avec Pacing Prédictif et Sécurité Juridique "Global First".

from fastapi import FastAPI, Request, BackgroundTasks, Response, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
import time
import logging
import random
import hashlib
import secrets
import calendar
import os
import requests 
import json
from datetime import datetime, timezone
from typing import Optional, Tuple, List, Dict, Any
from pathlib import Path

# --- CORE IMPORTS ---
from core.database import NexusDB
from core.settings import settings

app = FastAPI(title="Nexus Ad Exchange", version="3.3.0")
db = NexusDB()
logger = logging.getLogger("AdExchange")

# --- SECURITY CONFIGURATION ---
allowed_origins = settings.ALLOWED_ORIGINS
if "*" in allowed_origins and len(allowed_origins) == 1:
    logger.warning("⚠️ SECURITY NOTICE: Wildcard CORS detected. Production restriction advised.")

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# --- ASSET SERVING (FRONTEND BUNDLE) ---
STATIC_DIR = Path("/app/static_site")

if STATIC_DIR.exists():
    app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="assets")
else:
    logger.warning(f"⚠️ ASSET WARNING: Frontend build missing at {STATIC_DIR}.")

# --- IN-MEMORY CACHE LAYERS (HIGH VELOCITY) ---
REQUEST_CACHE: Dict[str, Tuple[float, Optional[str]]] = {}
CACHE_TTL = 30.0
RETRY_WINDOW = 180.0

VELOCITY_CACHE: Dict[str, List[float]] = {}
BURST_LIMIT = 5
BURST_WINDOW = 60

PREFETCH_CACHE: Dict[str, Dict[str, Any]] = {}
PREFETCH_TTL = 60.0

HEALTH_STATUS_CACHE: Dict[str, Tuple[bool, float]] = {}
HEALTH_TTL = 60.0

PARTNER_PERFORMANCE: Dict[str, Dict[str, Any]] = {}

# --- DATA MODELS ---
class AttributionModel(BaseModel):
    click_id: str
    amount: Optional[float] = 0.0
    secret: str
    transaction_id: Optional[str] = None
    strategy_tag: Optional[str] = None

# --- INITIALIZATION ---
@app.on_event("startup")
async def startup_event():
    """Ensure persistence tables exist."""
    try:
        with db.session() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS attribution_history (
                    user_hash TEXT,
                    mandate_id TEXT,
                    created_at REAL,
                    PRIMARY KEY (user_hash, mandate_id)
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_attr_hash ON attribution_history(user_hash)")
        logger.info("✅ Persistence Layer Initialized (SQLite/NexusDB).")
    except Exception as e:
        logger.error(f"❌ DB Init Error: {e}")

# --- UTILITY FUNCTIONS ---

def clean_cache_layers():
    """Maintenance routine for memory caches."""
    now = time.time()
    expired_req = [k for k, v in REQUEST_CACHE.items() if now - v[0] > max(CACHE_TTL, RETRY_WINDOW)]
    for k in expired_req: del REQUEST_CACHE[k]
    
    expired_tok = [k for k, v in PREFETCH_CACHE.items() if now - v['created_at'] > PREFETCH_TTL]
    for k in expired_tok: del PREFETCH_CACHE[k]
    
    for pid in list(VELOCITY_CACHE.keys()):
        VELOCITY_CACHE[pid] = [t for t in VELOCITY_CACHE[pid] if now - t < BURST_WINDOW]
        if not VELOCITY_CACHE[pid]: del VELOCITY_CACHE[pid]
        
    expired_health = [k for k, v in HEALTH_STATUS_CACHE.items() if now - v[1] > HEALTH_TTL]
    for k in expired_health: del HEALTH_STATUS_CACHE[k]

def get_persistent_history(user_hash: str) -> List[str]:
    """Retrieves user history from Persistent Storage."""
    try:
        with db.session() as conn:
            rows = conn.execute(
                "SELECT mandate_id FROM attribution_history WHERE user_hash = ?", 
                (user_hash,)
            ).fetchall()
            return [r[0] for r in rows]
    except Exception as e:
        logger.error(f"⚠️ History Read Error: {e}")
        return []

def get_country_from_headers(request: Request) -> str:
    """
    Extracts country code with FAIL-SAFE LOGIC (BP V3.0 Requirement).
    Prioritizes Safety: If IP is unknown/local, defaults to 'FR' behavior (Restricted) to avoid AMF Risk.
    """
    # 1. Cloudflare / Proxy Header (Primary Source of Truth)
    cf_ip_country = request.headers.get("CF-IPCountry")
    if cf_ip_country and len(cf_ip_country) == 2:
        return cf_ip_country.upper()
    
    # 2. X-Forwarded-For Analysis (Secondary)
    # Note: Dans une prod complète, un GeoIP lookup local serait fait ici.
    
    # 3. SMART FALLBACK : Analyse de la langue (Accept-Language)
    lang = request.headers.get("accept-language", "").lower()
    if "fr" in lang:
        return "FR"
    if "de" in lang:
        return "DE"
    if "es" in lang:
        return "ES"
    if "it" in lang:
        return "IT"
    
    # 4. FAIL-SAFE SECURITY V3.0
    # Si on ne sait pas, on assume que c'est une juridiction restrictive (EU/FR) pour éviter le risque légal.
    # On ne renvoie "GLOBAL" (US Routing) que si on est sûr.
    client_host = request.client.host
    if client_host in ["127.0.0.1", "localhost", "::1"]:
        # Local development -> Default to Config, but warn
        return settings.DEFAULT_ROUTING_ZONE
        
    # Par défaut, on protège.
    return "FR" 

def assess_inventory_quality(request: Request, ip: str) -> float:
    """
    IVT (Invalid Traffic) Detection Engine.
    Returns a Quality Score (0.0 - 1.0).
    """
    score = 1.0
    headers = request.headers
    ua = headers.get("user-agent", "").lower()

    if not headers.get("sec-fetch-dest") and not headers.get("sec-ch-ua"):
        score -= 0.3

    mode = headers.get("sec-fetch-mode", "")
    if mode and mode != "navigate":
        score -= 0.4

    bot_signatures = ["headless", "bot", "crawl", "spider", "preview", "curl", "wget"]
    if any(sig in ua for sig in bot_signatures):
        return 0.0 

    user_hash = hashlib.md5(f"{ip}".encode()).hexdigest()
    
    if user_hash in REQUEST_CACHE and (time.time() - REQUEST_CACHE[user_hash][0] < 5.0):
         score -= 0.2

    history = get_persistent_history(user_hash)
    if len(history) > 10:
        score -= 0.5

    return max(0.0, score)

def verify_mandate_availability(url: str) -> bool:
    """Availability Check with Memory Caching and Content Verification Support."""
    now = time.time()
    
    if url in HEALTH_STATUS_CACHE:
        is_up, last_check = HEALTH_STATUS_CACHE[url]
        if now - last_check < HEALTH_TTL:
            return is_up

    try:
        r = requests.head(url, timeout=0.8, allow_redirects=True)
        # V3 Hardening: 404/500 are dead.
        is_alive = r.status_code < 400
    except Exception:
        is_alive = False
    
    HEALTH_STATUS_CACHE[url] = (is_alive, now)
    
    if not is_alive:
        logger.warning(f"🚨 SLA ALERT: Endpoint {url} unreachable. Pause Mandate.")
        
    return is_alive

def check_pacing_safety(mandate_id: str) -> bool:
    """Traffic Pacing Control."""
    now = time.time()
    timestamps = VELOCITY_CACHE.get(mandate_id, [])
    timestamps = [t for t in timestamps if now - t < BURST_WINDOW]
    
    if len(timestamps) >= BURST_LIMIT:
        return False
    
    timestamps.append(now)
    VELOCITY_CACHE[mandate_id] = timestamps
    return True

def record_conversion_event(lead_id: str, mandate_id: str, ip: str, user_agent: str, was_failover: bool = False):
    """
    Async Attribution Logging with DB Persistence.
    """
    try:
        anonymized_id = hashlib.sha256(lead_id.encode()).hexdigest()[:12]
        
        log_msg = f"✅ ATTRIBUTION: Segment {anonymized_id} -> Mandate {mandate_id}"
        if was_failover:
            log_msg += " (Via Yield Optimization)"
        
        logger.info(log_msg)
        
        if mandate_id not in PARTNER_PERFORMANCE:
            PARTNER_PERFORMANCE[mandate_id] = {"impressions": 0, "conversions": 0, "last_updated": time.time()}
        PARTNER_PERFORMANCE[mandate_id]["impressions"] += 1

        user_hash = hashlib.md5(f"{ip}".encode()).hexdigest()
        
        with db.session() as conn:
            conn.execute("""
                UPDATE leads 
                SET status='CONVERSION_INITIATED', updated_at=?, assigned_sponsor_id=?
                WHERE id=?
            """, (time.time(), mandate_id, lead_id))
            
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO attribution_history (user_hash, mandate_id, created_at)
                    VALUES (?, ?, ?)
                """, (user_hash, mandate_id, time.time()))
            except Exception as e:
                logger.debug(f"History insert skipped: {e}")

            description = 'Traffic Allocation' + (' (Optimized)' if was_failover else '')
            try:
                conn.execute("""
                    INSERT INTO ledger (transaction_date, type, partner_id, amount, reference_external, description, balance_after)
                    VALUES (?, 'MEDIA_BUY', ?, 0, ?, ?, 0)
                """, (time.time(), mandate_id, lead_id, description))
            except Exception as e:
                logger.debug(f"Ledger entry skipped: {e}")
            
    except Exception as e:
        logger.error(f"❌ ATTRIBUTION ERROR (Segment {lead_id}): {e}")

def calculate_pacing_factor(verified: int, limit: int, program_type: str = "GENERIC") -> float:
    """
    🧠 INTELLIGENCE V3.3: Yield Management Prédictif & Market Volatility.
    """
    if limit <= 0: return 1.0
    
    now = datetime.now()
    day_of_month = now.day
    days_in_month = calendar.monthrange(now.year, now.month)[1]
    days_left = max(1, days_in_month - day_of_month)
    
    # 1. Calcul de la vélocité nécessaire
    # Combien de leads devons-nous encore générer pour atteindre le hard cap ?
    remaining_quota = limit - verified
    if remaining_quota <= 0: return 0.0 # Quota atteint

    # Pacing linéaire théorique
    ideal_pace = (limit / days_in_month) * day_of_month
    
    # Ratio de performance (Êtes-vous en avance ou en retard ?)
    # < 1.0 = En retard (Besoin de push)
    # > 1.0 = En avance (Besoin de frein)
    pacing_ratio = verified / max(1, ideal_pace)
    
    pacing_score = 1.0
    
    # 2. Logique de rattrapage (Catch-up Logic)
    if pacing_ratio < 0.5: 
        pacing_score = 2.5 # Retard critique -> Boost massif
    elif pacing_ratio < 0.8: 
        pacing_score = 1.5 # Retard modéré -> Boost
    elif pacing_ratio > 1.2: 
        pacing_score = 0.5 # Avance -> Freinage
    elif pacing_ratio > 1.5:
        pacing_score = 0.1 # Trop d'avance -> Freinage fort
    
    # 3. Market Volatility Boost (Fin de mois)
    # Si on est dans les 5 derniers jours et qu'il reste du stock Prop Firm, on force la vente.
    if days_left <= 5 and "PROP_FIRM" in program_type and remaining_quota > 0:
        pacing_score *= 2.0
        
    return pacing_score

def allocate_inventory_to_mandate(requested_mandate_id: str, exclude_ids: List[str] = [], country_code: str = "GLOBAL") -> Tuple[str, str, bool, str]:
    """
    PROGRAMMATIC ALLOCATION ENGINE (V3.3 - STRICT GEO-FENCING).
    """
    euro_zone = ["FR", "BE", "CH", "LU", "MC", "DE", "IT", "ES", "NL", "PT"]
    anglo_zone = ["US", "UK", "CA", "AU", "NZ", "IE"]
    
    is_euro_traffic = country_code in euro_zone
    is_anglo_traffic = country_code in anglo_zone
    
    # Fallbacks Stratégiques V3.3 (Alignement BP V3.0)
    # Règle d'Or: EU -> Crypto PSAN/SaaS (Pas de Prop Firm)
    if is_euro_traffic:
        fallback_url = "https://meria.com/" # PSAN Safe
        fallback_program = "CRYPTO_PSAN"
    elif is_anglo_traffic:
        fallback_url = "https://apextraderfunding.com/" # High Yield
        fallback_program = "PROP_FIRM"
    else:
        fallback_url = "https://www.binance.com/" # Global Volume
        fallback_program = "CRYPTO_EXCHANGE"
        
    fallback_tuple = (fallback_url, "GENERIC_FALLBACK", True, fallback_program)

    try:
        with db.session() as conn:
            target = conn.execute("""
                SELECT id, url_template, active, monthly_limit_hard, verified_count_month, 
                       priority, conversion_rate_estimate, program, balance_available, financial_terms, allowed_geos
                FROM sponsors 
                WHERE id=?
            """, (requested_mandate_id,)).fetchone()

            needs_reallocation = False

            if target:
                prog_type = str(target['program']).upper()
                allowed_geos_json = target['allowed_geos']
                try:
                    allowed_geos = json.loads(allowed_geos_json) if allowed_geos_json else ["GLOBAL"]
                except:
                    allowed_geos = ["GLOBAL"]

                # 1. Vérification Géographique Explicite
                if "GLOBAL_EXCEPT_US_FR" in allowed_geos and country_code in ["US", "FR"]:
                     logger.info(f"🌍 GEO BLOCKED: {country_code} excluded from {requested_mandate_id}.")
                     needs_reallocation = True
                
                elif "GLOBAL" not in allowed_geos and "GLOBAL_EXCEPT_US_FR" not in allowed_geos:
                    if country_code not in allowed_geos:
                         logger.info(f"🌍 GEO BLOCKED: {country_code} not in allowed list for {requested_mandate_id}.")
                         needs_reallocation = True
                
                # 2. RÈGLE D'OR V3 (COMPLIANCE AMF): Prop Firms STRICTEMENT interdites aux résidents FR
                if country_code == "FR" and "PROP_FIRM" in prog_type:
                    logger.warning(f"🛡️ AMF COMPLIANCE SHIELD: Blocking Prop Firm {requested_mandate_id} for FR user. Rerouting to SaaS.")
                    needs_reallocation = True

            if requested_mandate_id in exclude_ids:
                logger.info(f"🛡️ FREQUENCY CAPPING: Mandate {requested_mandate_id} excluded.")
                needs_reallocation = True
            
            elif not target or target['active'] != 1:
                needs_reallocation = True
                
            else:
                balance = db.to_decimal(target['balance_available'] if db.use_postgres else target['balance_available'])
                limit = target['monthly_limit_hard']
                verified = target['verified_count_month']
                
                if balance < 15.0:
                    logger.warning(f"📉 BUDGET CONTROL: Mandate {target['id']} paused (Funds: {balance}€). Reallocating.")
                    needs_reallocation = True
                elif limit > 0 and verified >= limit:
                    logger.info(f"📊 QUOTA REACHED: Mandate {target['id']} fulfilled ({verified}/{limit}).")
                    needs_reallocation = True
                elif not check_pacing_safety(target['id']):
                     logger.warning(f"🚦 PACING CONTROL: Velocity limit for {target['id']}.")
                     needs_reallocation = True

            if not needs_reallocation:
                return (target['url_template'] or fallback_url, target['id'], False, target['program'])
            
            # --- PHASE 2: REALLOCATION INTELLIGENTE (YIELD OPTIMIZATION) ---
            logger.info(f"🔄 INVENTORY REALLOCATION triggered for {requested_mandate_id} (Geo: {country_code})")

            candidates = conn.execute("""
                SELECT id, url_template, monthly_limit_hard, verified_count_month, 
                       priority, conversion_rate_estimate, program, balance_available, allowed_geos
                FROM sponsors 
                WHERE active=1 
            """).fetchall()

            valid_candidates = []
            for c in candidates:
                cand_id = c['id']
                if cand_id in exclude_ids: continue
                
                # Filtrage Geo Candidat
                try:
                    allowed = json.loads(c['allowed_geos']) if c['allowed_geos'] else ["GLOBAL"]
                except:
                    allowed = ["GLOBAL"]
                
                if "GLOBAL_EXCEPT_US_FR" in allowed and country_code in ["US", "FR"]: continue
                if "GLOBAL" not in allowed and "GLOBAL_EXCEPT_US_FR" not in allowed:
                    if country_code not in allowed: continue

                # Filtrage Compliance (Prop Firms interdites en FR)
                if country_code == "FR" and "PROP_FIRM" in str(c['program']).upper(): continue

                bal = db.to_decimal(c['balance_available'] if db.use_postgres else c['balance_available'])
                if bal < 15.0: continue 
                
                lim = c['monthly_limit_hard']
                ver = c['verified_count_month']
                if lim > 0 and ver >= lim: continue
                
                valid_candidates.append(c)

            if not valid_candidates:
                return fallback_tuple

            # 3. Scoring Algorithm (Yield Management V3.3)
            best_candidate = None
            highest_score = -1.0
            
            for cand in valid_candidates:
                priority = max(1, cand['priority'])
                bal = float(cand['balance_available'] if db.use_postgres else cand['balance_available'])
                
                # Geo Weight Adjustment
                geo_weight = 1.0
                program_type = str(cand['program']).upper()

                if is_euro_traffic:
                    if program_type == 'CRYPTO_PSAN': geo_weight = 5.0 # Priorité absolue FR
                    elif program_type == 'SAAS': geo_weight = 3.0
                elif is_anglo_traffic:
                    if program_type == 'PROP_FIRM': geo_weight = 4.0 # Priorité absolue US
                else: 
                     if program_type == 'CRYPTO_EXCHANGE': geo_weight = 3.0

                # Intelligence V3.3: Pacing Factor Prédictif
                pacing_multiplier = calculate_pacing_factor(cand['verified_count_month'], cand['monthly_limit_hard'], cand['program'])

                score = (bal / 20.0) * priority * geo_weight * pacing_multiplier
                score *= random.uniform(0.9, 1.1) 

                if score > highest_score:
                    if check_pacing_safety(cand['id']):
                        highest_score = score
                        best_candidate = cand

            if best_candidate:
                return (best_candidate['url_template'] or fallback_url, best_candidate['id'], True, best_candidate['program'])
            
            return fallback_tuple

    except Exception as e:
        logger.error(f"⚠️ ALLOCATION ERROR: {e}")
        return (fallback_url, requested_mandate_id, False, "UNKNOWN")

# --- MIDDLEWARE ---
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    """Injects Security Headers."""
    response = await call_next(request)
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    if "text/html" not in response.headers.get("content-type", ""):
        response.headers["Content-Security-Policy"] = "default-src 'none';"
    return response

# --- ENDPOINTS ---

@app.get("/claim")
async def show_landing_page(id: str = "demo", p: str = "default", mode: Optional[str] = None):
    if (STATIC_DIR / "index.html").exists():
        return FileResponse(STATIC_DIR / "index.html")
    else:
        return HTMLResponse("<h1>System Maintenance</h1><p>Platform updating. Please retry shortly.</p>", status_code=503)

@app.get("/api/geo-check")
async def get_geo_context(request: Request):
    """
    NEW ENDPOINT V3.3: Provides Server-Side Geo Truth to Frontend.
    Prevents client-side spoofing and ensures UI matches Compliance Rules.
    """
    country = get_country_from_headers(request)
    # Zone logic
    if country in ["FR", "BE", "CH", "LU", "MC", "DE", "IT", "ES", "NL", "PT"]:
        zone = "EU"
    else:
        zone = "US" # Default for Global
        
    return JSONResponse(content={"country": country, "zone": zone})

@app.get("/api/offers")
async def get_dynamic_offers(request: Request):
    """
    Public Endpoint for Dynamic Offer Configuration.
    Supports Client-Side Load Balancing and UI Adaptation.
    """
    try:
        with db.session() as conn:
            rows = conn.execute("""
                SELECT program, balance_available, active, financial_terms, id
                FROM sponsors 
                WHERE active = 1
            """).fetchall()
            
            if rows:
                offers_config = []
                for row in rows:
                    terms = json.loads(row['financial_terms']) if isinstance(row['financial_terms'], str) else row['financial_terms']
                    offers_config.append({
                        "program": row['program'],
                        "program_id": row['id'],
                        "balance_available": row['balance_available'],
                        "active": bool(row['active']),
                        "type": terms.get('category', 'UNKNOWN'),
                        "currency": terms.get('currency', 'USD'), # V3: Dynamic Currency
                        "amount": terms.get('target_cpa', 0) if terms.get('model') == 'REVENUE_SHARE' else 100
                    })
                return JSONResponse(content={"status": "live", "offers": offers_config})
    except Exception as e:
        logger.warning(f"DB Config Fetch Error: {e}. Fallback to file.")

    sponsors_file = Path("config/sponsors.json")
    if sponsors_file.exists():
        with open(sponsors_file, "r") as f:
            data = json.load(f)
            return JSONResponse(content={"status": "static", "offers": data})
            
    return JSONResponse(content={"status": "error", "offers": []}, status_code=500)

@app.get("/warmup/{lead_id}/{partner_id}")
async def preflight_allocation(lead_id: str, partner_id: str, request: Request):
    clean_pid = "".join(c for c in partner_id if c.isalnum() or c in "-_")
    client_ip = request.client.host
    user_hash = hashlib.md5(f"{client_ip}".encode()).hexdigest()
    country_code = get_country_from_headers(request)
    
    if clean_pid == "nexus_tech_access":
        token = secrets.token_urlsafe(16)
        PREFETCH_CACHE[token] = {
            "url": settings.PARTNER_DASHBOARD_URL,
            "partner_id": "nexus_agency_internal",
            "program": "PARTNER_PROGRAM",
            "is_reallocated": False,
            "created_at": time.time()
        }
        return JSONResponse(content={"status": "ready", "token": token, "target_preview": "NEXUS_PARTNER"})

    try:
        user_seen = get_persistent_history(user_hash)
        
        real_url, final_pid, is_realloc, prog = allocate_inventory_to_mandate(
            clean_pid, 
            exclude_ids=user_seen, 
            country_code=country_code
        )
        
        if not verify_mandate_availability(real_url):
            logger.warning(f"🚑 HEALTH CHECK: Mandate {final_pid} endpoint down. Reallocating.")
            current_excludes = user_seen + [final_pid]
            real_url, final_pid, is_realloc, prog = allocate_inventory_to_mandate(
                clean_pid, 
                exclude_ids=current_excludes,
                country_code=country_code
            )

        token = secrets.token_urlsafe(16)
        
        PREFETCH_CACHE[token] = {
            "url": real_url,
            "partner_id": final_pid,
            "program": prog,
            "is_reallocated": is_realloc,
            "created_at": time.time()
        }

        load_ratio = 0.5
        try:
            with db.session() as conn:
                 row = conn.execute("SELECT monthly_limit_hard, verified_count_month FROM sponsors WHERE id=?", (final_pid,)).fetchone()
                 if row and row['monthly_limit_hard'] > 0:
                     load_ratio = row['verified_count_month'] / row['monthly_limit_hard']
        except: pass

        return JSONResponse(content={
            "status": "ready",
            "token": token,
            "target_preview": final_pid,
            "intelligence": {"load_ratio": load_ratio}
        })
    except Exception as e:
        logger.error(f"Preflight failed: {e}")
        return JSONResponse(content={"status": "error"}, status_code=500)

@app.post("/ping/{lead_id}")
async def confirm_engagement(lead_id: str):
    clean_id = "".join(c for c in lead_id if c.isalnum() or c in "-_")
    success = db.confirm_lead_hold(clean_id)
    if success: return {"status": "confirmed"}
    return {"status": "ignored"}

@app.get("/click/{lead_id}/{partner_id}")
async def track_and_redirect(
    lead_id: str, 
    partner_id: str, 
    request: Request, 
    background_tasks: BackgroundTasks,
    t: Optional[str] = None,
    exclude_pid: Optional[str] = None
):
    """PRIMARY ATTRIBUTION ENDPOINT (V3.3 SMART ROUTING)."""
    safe_lead_id = "".join(c for c in lead_id if c.isalnum() or c in "-_")
    safe_partner_id = "".join(c for c in partner_id if c.isalnum() or c in "-_")
    client_ip = request.client.host
    user_agent = request.headers.get('user-agent', 'Unknown')
    user_hash = hashlib.md5(f"{client_ip}".encode()).hexdigest()
    country_code = get_country_from_headers(request)
    now = time.time()
    
    quality_score = assess_inventory_quality(request, client_ip)
    if quality_score < 0.3:
        logger.warning(f"⛔ IVT BLOCKED: Score {quality_score:.2f} for {client_ip}.")
        return RedirectResponse(url="https://www.google.com")

    if safe_partner_id == "nexus_tech_access":
         return RedirectResponse(url=settings.PARTNER_DASHBOARD_URL)

    if t and t in PREFETCH_CACHE:
        cached = PREFETCH_CACHE.pop(t)
        
        if cached.get("partner_id") == "nexus_agency_internal":
             return RedirectResponse(url=cached["url"])

        if exclude_pid and cached["partner_id"] == exclude_pid:
             logger.info(f"🚫 TOKEN REVOKED: Explicit exclusion for {exclude_pid}.")
        else:
            if db.atomic_dispatch_transaction(safe_lead_id, cached["partner_id"], cached["program"], 0):
                background_tasks.add_task(
                    record_conversion_event, 
                    safe_lead_id, cached["partner_id"], client_ip, user_agent, cached["is_reallocated"]
                )
                return RedirectResponse(url=cached["url"])

    excluded_partners = []
    
    if exclude_pid:
        excluded_partners.append("".join(c for c in exclude_pid if c.isalnum() or c in "-_"))

    if user_hash in REQUEST_CACHE:
        last_time, last_pid = REQUEST_CACHE[user_hash]
        if now - last_time < 15.0 and last_pid:
            excluded_partners.append(last_pid)
    
    excluded_partners.extend(get_persistent_history(user_hash))
    unique_excludes = list(set(excluded_partners))
    
    attempts = 0
    max_attempts = 3
    
    # Fallback par défaut défini par la zone géo
    if country_code in ["FR", "BE", "CH", "LU", "MC", "DE"]:
        final_url = "https://meria.com/" # Safe PSAN
    elif country_code in ["US", "UK", "CA", "AU"]:
        final_url = "https://apextraderfunding.com/" 
    else:
        final_url = "https://www.binance.com/" 
    
    while attempts < max_attempts:
        url, pid, is_realloc, prog = allocate_inventory_to_mandate(
            safe_partner_id, 
            exclude_ids=unique_excludes,
            country_code=country_code
        )
        
        if pid == "GENERIC_FALLBACK":
            final_url = url
            break
            
        if db.atomic_dispatch_transaction(safe_lead_id, pid, prog, 0):
            REQUEST_CACHE[user_hash] = (now, pid)
            
            req_hash = hashlib.md5(f"{client_ip}-{pid}".encode()).hexdigest()
            is_dup = False
            if req_hash in REQUEST_CACHE:
                if now - REQUEST_CACHE[req_hash][0] < CACHE_TTL: is_dup = True
            
            if not is_dup:
                background_tasks.add_task(
                    record_conversion_event, 
                    safe_lead_id, pid, client_ip, user_agent, was_failover=is_realloc
                )
            
            if not (url.startswith("http://") or url.startswith("https://")):
                url = final_url
                
            return RedirectResponse(url=url)
        else:
            unique_excludes.append(pid)
            attempts += 1
            
    logger.error(f"⚠️ ALLOCATION FAILED: Fallback served for {safe_lead_id}")
    return RedirectResponse(url=final_url)

@app.post("/postback")
async def process_conversion_feedback(payload: AttributionModel, background_tasks: BackgroundTasks):
    expected = settings.POSTBACK_SECRET_TOKEN.get_secret_value()
    if payload.secret != expected:
        raise HTTPException(status_code=403, detail="Invalid Token")

    logger.info(f"💰 CONVERSION CONFIRMED: {payload.click_id} -> {payload.amount}€")
    
    def _finalize_transaction():
        success = db.register_conversion_event(
            lead_id=payload.click_id, 
            amount=payload.amount, 
            transaction_id=payload.transaction_id
        )
        if success:
            logger.info("✅ LEDGER UPDATED: Transaction finalized and fees calculated.")

    background_tasks.add_task(_finalize_transaction)
    return {"status": "accepted"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")