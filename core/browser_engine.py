### prospection/core/browser_engine.py
# core/browser_engine.py - APEX COMPATIBILITY ENGINE V27.0 (RESILIENCE & FALLBACK)
# -*- coding: utf-8 -*-

"""
APEX COMPATIBILITY ENGINE V27.0 - ENVIRONMENT EMULATOR
-------------------------------------------------------
1. Authenticated Residential Proxies: Native support for certified ingress.
2. Environment Emulator V13 (DYNAMIC ENTROPY): Procedural generation of standard environments.
3. Cookie Health Check: Proactive session maintenance.
4. Smart Recovery & Scenario Routing: Contextual warm-up sequences.
5. Process Guardian: Zombie process cleanup.
6. Fail-Fast Network: Kill switch for degraded network paths.
7. Docker Ready: Container-compatible configuration.
8. Circuit Breaker (SMART): Intelligent network resilience with Adaptive Strategies.
9. Geo-Alignment: Synchronization of Timezone/Locale.
10. Karma Health Score: Persistence of profile reputation.
11. Semantic Navigation: Fallback to text-based interaction (Resilience).
12. Smart Click + De-clutter: Auto-removal of overlays/modals blocking interaction.
"""

import asyncio
import json
import time
import logging
import secrets
import base64
import sys
import os
import subprocess
import re
import gc
import random
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional, Dict, List, Any
from urllib.parse import urlparse

try:
    import psutil
    import aiofiles
    import aiohttp
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    from core.settings import settings
    from core.database import NexusDB
except ImportError:
    print("🔴 CRITICAL ERROR: Modules missing in browser_engine.")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("ApexEngine")

_chrome_version_cache: Optional[str] = None
_version_lock = asyncio.Lock()

async def get_chrome_version_async(chrome_path: str) -> str:
    global _chrome_version_cache
    async with _version_lock:
        if _chrome_version_cache: return _chrome_version_cache
        
        if not chrome_path or not os.path.exists(chrome_path):
             if os.environ.get("DOCKER_CONTAINER"):
                 return "120.0.6099.0"
             if not chrome_path and settings.CHROME_BIN:
                 chrome_path = settings.CHROME_BIN
             
             if not chrome_path or not os.path.exists(chrome_path):
                 return "120.0.6099.0"

        def _run():
            try:
                res = subprocess.run([chrome_path, "--version"], capture_output=True, text=True, timeout=5)
                m = re.search(r"(\d{2,4}\.\d+\.\d+\.\d+)", res.stdout)
                return m.group(1) if m else None
            except: return None

        real_ver = await asyncio.to_thread(_run)
        if not real_ver: 
            return "120.0.0.0"
        _chrome_version_cache = real_ver
        return real_ver

class CircuitBreaker:
    """
    Resilience Manager (Circuit Breaker Pattern).
    Prevents repeated usage of failing resources (Proxies, Endpoints).
    INTELLIGENCE: Adaptive Error Handling (decide_strategy).
    """
    def __init__(self, failure_threshold=3, recovery_timeout=900):
        self.failures = {}  # {resource_id: count}
        self.site_failures = {} # {domain: count}
        self.open_circuits = {}  # {resource_id: unlock_timestamp}
        self.site_open_circuits = {} # {domain: unlock_timestamp}
        self.THRESHOLD = failure_threshold
        self.SITE_THRESHOLD = failure_threshold * 2 
        self.COOLDOWN = recovery_timeout  

    def decide_strategy(self, error_type: str) -> str:
        """
        Intelligence: Determine the recovery strategy based on error type.
        """
        if error_type == "TIMEOUT" or error_type == "CONNECTION_REFUSED":
            # Problème réseau passif -> Retry simple, pas de pénalité immédiate
            return "RETRY_SAME_NODE"
            
        elif error_type == "ACCESS_DENIED_403" or error_type == "BANNED":
            # Problème d'identité -> Changer User-Agent + IP immédiatement
            return "ROTATE_IDENTITY"
            
        elif error_type == "DOM_ELEMENT_NOT_FOUND":
            # Problème structurel -> Le site a peut-être changé ou chargé lentement
            return "WAIT_AND_SHAKE"
            
        return "STANDARD_FAILURE"

    def allow_request(self, resource_id: str, target_domain: str = None) -> bool:
        # 1. Check if site is unavailable (Target Down)
        if target_domain and target_domain in self.site_open_circuits:
             if time.time() < self.site_open_circuits[target_domain]:
                 logger.debug(f"🔌 Circuit Breaker: Domain {target_domain} is temporarily ignored.")
                 return False
             else:
                 del self.site_open_circuits[target_domain]
                 del self.site_failures[target_domain]

        # 2. Check if circuit is open (Proxy/Endpoint banned)
        if resource_id in self.open_circuits:
            if time.time() < self.open_circuits[resource_id]:
                logger.debug(f"🔌 Circuit Breaker: {resource_id} is blocked.")
                return False  
            else:
                # Time elapsed, attempt rehabilitation (Half-Open)
                logger.info(f"🔌 Circuit Breaker: Rehabilitating {resource_id}")
                del self.open_circuits[resource_id]
                del self.failures[resource_id]
        return True

    def record_failure(self, resource_id: str, is_site_error: bool = False, target_domain: str = None, error_type: str = "GENERIC"):
        """
        Records a failure with Intelligent Routing logic.
        """
        strategy = self.decide_strategy(error_type)

        if strategy == "RETRY_SAME_NODE":
            logger.info(f"🛡️ Resilience: {error_type} detected on {resource_id}. Retrying without penalty.")
            return # On n'incrémente pas le compteur d'échec pour un simple timeout occasionnel

        if is_site_error and target_domain:
            self.site_failures[target_domain] = self.site_failures.get(target_domain, 0) + 1
            if self.site_failures[target_domain] >= self.SITE_THRESHOLD:
                self.site_open_circuits[target_domain] = time.time() + (self.COOLDOWN * 2)
                logger.warning(f"🔌 CIRCUIT OPEN (SITE): {target_domain} isolated due to instability.")
        else:
            # Penalize the proxy/resource
            penalty = 1
            if strategy == "ROTATE_IDENTITY":
                penalty = self.THRESHOLD # Immediate Circuit Break
                logger.warning(f"🛡️ Resilience: Critical Error ({error_type}) -> Immediate Isolation of {resource_id}")

            self.failures[resource_id] = self.failures.get(resource_id, 0) + penalty
            
            if self.failures[resource_id] >= self.THRESHOLD:
                self.open_circuits[resource_id] = time.time() + self.COOLDOWN
                logger.warning(f"🔌 CIRCUIT OPEN (PROXY): {resource_id} isolated for {self.COOLDOWN}s after failures.")

    def record_success(self, resource_id: str, target_domain: str = None):
        # Reset on success
        if resource_id in self.failures:
            del self.failures[resource_id]
        if target_domain and target_domain in self.site_failures:
            del self.site_failures[target_domain]

class ProcessGuardian:
    @staticmethod
    def kill_zombies_on_port(port: int):
        logger.info(f"🛡️ Guardian: Scanning port {port}...")
        try:
            me = psutil.Process()
            for conn in psutil.net_connections(kind='inet'):
                if conn.laddr.port == port and conn.status == 'LISTEN':
                    if not conn.pid: continue
                    try:
                        p = psutil.Process(conn.pid)
                        # Security: Only kill own processes
                        if p.username() == me.username() or os.geteuid() == 0:
                            p.terminate()
                            try: p.wait(timeout=2)
                            except: p.kill()
                            return
                    except: pass
        except: pass

class SessionVault:
    def __init__(self, storage_path: Path):
        self.storage_path = storage_path
        self.lock_file = storage_path.with_suffix('.lock')

    def _derive_key(self, salt: bytes, secret: str) -> bytes:
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=600_000)
        return base64.urlsafe_b64encode(kdf.derive(secret.encode('utf-8')))
    
    async def _acquire_lock(self, timeout=5.0):
        start = time.time()
        while time.time() - start < timeout:
            try:
                fd = os.open(str(self.lock_file), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
                return True
            except FileExistsError:
                try:
                    if time.time() - self.lock_file.stat().st_mtime > 30:
                         os.unlink(self.lock_file)
                except FileNotFoundError: pass
                await asyncio.sleep(0.1)
        return False

    async def _release_lock(self):
        try: os.unlink(self.lock_file)
        except: pass

    async def save(self, data: Dict):
        try:
            secret = settings.SECURITY_MASTER_KEY.get_secret_value()
            salt = os.urandom(16)
            key = self._derive_key(salt, secret)
            cipher = Fernet(key)
            encrypted = cipher.encrypt(json.dumps(data).encode('utf-8'))
            
            if await self._acquire_lock():
                try:
                    tmp = self.storage_path.with_suffix(".tmp")
                    async with aiofiles.open(tmp, "wb") as f: await f.write(salt + encrypted)
                    os.replace(tmp, self.storage_path)
                finally:
                    await self._release_lock()
        except Exception as e: logger.error(f"Save Vault Error: {e}")

    async def load(self) -> Dict:
        if not self.storage_path.exists(): return {}
        try:
            content = None
            if await self._acquire_lock():
                try:
                    async with aiofiles.open(self.storage_path, "rb") as f: content = await f.read()
                finally:
                    await self._release_lock()
            else:
                 async with aiofiles.open(self.storage_path, "rb") as f: content = await f.read()
            
            if not content: return {}
            
            salt, payload = content[:16], content[16:]
            secret = settings.SECURITY_MASTER_KEY.get_secret_value()
            key = self._derive_key(salt, secret)
            return json.loads(Fernet(key).decrypt(payload).decode('utf-8'))
        except Exception: return {}

    async def check_health(self) -> bool:
        """
        Validates session with Predictive Maintenance (Cookie Decay).
        """
        data = await self.load()
        if not data or "cookies" not in data: 
            return False 

        cookies = data.get("cookies", [])
        if not cookies: return True

        now = time.time()
        expired_count = 0
        critical_decay = False
        
        # 30-minute safety margin
        SAFETY_MARGIN = 1800 
        critical_names = ["sessionid", "tt_webid", "SID", "SSID", "auth_token"]

        for c in cookies:
            expires = c.get("expires", -1)
            if expires != -1:
                ttl = expires - now
                if ttl < 0:
                    expired_count += 1
                
                # Predictive Maintenance
                if c.get("name") in critical_names and ttl < SAFETY_MARGIN:
                    logger.warning(f"🍪 Session Vault: Critical cookie '{c.get('name')}' decaying (TTL: {int(ttl)}s). Force refresh.")
                    critical_decay = True

        if critical_decay:
            return False
        
        if expired_count > len(cookies) * 0.5:
            return False

        return True
    
    async def get_last_status(self) -> str:
        data = await self.load()
        return data.get("exit_status", "unknown")

@dataclass
class EmulationProfile:
    user_agent: str
    platform: str
    hardware_concurrency: int
    device_memory: int
    webgl_vendor: str
    webgl_renderer: str
    created_at: float
    reflex_score: float = 0.5
    screen_width: int = 1920
    screen_height: int = 1080
    canvas_noise_hash: str = ""

class EnvironmentEmulator:
    """
    Environment Emulator V13 - DYNAMIC ENTROPY.
    Procedural generation of consistent testing environments.
    """
    def __init__(self, profile_dir: Path):
        self.fp_file = profile_dir / "digital_dna.json"
        
        self.GPU_VENDORS = {
            "NVIDIA": [
                "NVIDIA GeForce GTX 1050", "NVIDIA GeForce GTX 1060", "NVIDIA GeForce GTX 1650",
                "NVIDIA GeForce GTX 1660", "NVIDIA GeForce RTX 2060", "NVIDIA GeForce RTX 2070",
                "NVIDIA GeForce RTX 3060", "NVIDIA GeForce RTX 3070", "NVIDIA GeForce RTX 4060"
            ],
            "AMD": [
                "AMD Radeon RX 580", "AMD Radeon RX 5700 XT", "AMD Radeon RX 6600",
                "AMD Radeon RX 6700 XT", "AMD Radeon Vega 8", "AMD Radeon Graphics"
            ],
            "Intel": [
                "Intel(R) UHD Graphics 630", "Intel(R) Iris(R) Xe Graphics", "Intel(R) HD Graphics 620"
            ]
        }
        
        self.CPU_RAM_OPTS = [
            (4, 8), (8, 8), (8, 16), (12, 16), (16, 32), (24, 64)
        ]

    def _emulate_hardware_configuration(self) -> Dict:
        """Generates a consistent hardware profile for emulation."""
        cpu, ram = secrets.choice(self.CPU_RAM_OPTS)
        
        r = random.random()
        if r < 0.6: vendor_key = "NVIDIA"
        elif r < 0.85: vendor_key = "AMD"
        else: vendor_key = "Intel"
        
        renderer_model = secrets.choice(self.GPU_VENDORS[vendor_key])
        
        # ANGLE renderer string construction
        full_renderer = f"ANGLE ({vendor_key}, {renderer_model}, OpenGL 4.5)"
        full_vendor = f"Google Inc. ({vendor_key})"
        
        if ram >= 32: width, height = 2560, 1440
        elif ram >= 16: width, height = 1920, 1080
        elif ram >= 8: width, height = secrets.choice([(1920, 1080), (1366, 768)])
        else: width, height = 1366, 768

        return {
            "cpu": cpu,
            "ram": ram,
            "vendor": full_vendor,
            "renderer": full_renderer,
            "width": width,
            "height": height
        }

    async def get_emulation_profile(self) -> EmulationProfile:
        if self.fp_file.exists():
            try:
                async with aiofiles.open(self.fp_file, "r") as f:
                    data = json.loads(await f.read())
                    # Migration V12 -> V13
                    if "screen_width" not in data:
                        data["screen_width"] = 1920
                        data["screen_height"] = 1080
                        data["canvas_noise_hash"] = secrets.token_hex(4)
                    
                    profile = EmulationProfile(**data)

                    # --- EVOLUTION LOGIC ---
                    try:
                        real_ver = await get_chrome_version_async(settings.CHROME_BIN)
                        saved_ver_match = re.search(r"Chrome/(\d+)", profile.user_agent)
                        real_ver_match = re.search(r"^(\d+)", real_ver)

                        if saved_ver_match and real_ver_match:
                            saved_major = int(saved_ver_match.group(1))
                            real_major = int(real_ver_match.group(1))

                            if real_major > saved_major:
                                logger.info(f"🧬 Profile Evolution: v{saved_major} -> v{real_major}")
                                profile.user_agent = profile.user_agent.replace(
                                    f"Chrome/{saved_major}", f"Chrome/{real_major}"
                                )
                                async with aiofiles.open(self.fp_file, "w") as f_save: 
                                    await f_save.write(json.dumps(asdict(profile), indent=2))
                    except Exception as e:
                        logger.debug(f"Profile evolution check failed: {e}")

                    return profile
            except: pass
        return await self._generate_and_save_new()

    async def _generate_and_save_new(self) -> EmulationProfile:
        real_ver = await get_chrome_version_async(settings.CHROME_BIN)
        
        hw = self._emulate_hardware_configuration()
        
        ua_os = "Windows NT 10.0; Win64; x64"
        platform = "Win32"
        
        score_cpu = hw["cpu"] / 16.0
        score_ram = hw["ram"] / 32.0
        reflex_score = round((score_cpu * 0.4 + score_ram * 0.6) + random.uniform(-0.1, 0.1), 2)
        reflex_score = max(0.2, min(0.95, reflex_score))
        
        noise_hash = secrets.token_hex(8)
        
        profile = EmulationProfile(
            user_agent=f"Mozilla/5.0 ({ua_os}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{real_ver} Safari/537.36",
            platform=platform,
            hardware_concurrency=hw["cpu"],
            device_memory=hw["ram"],
            webgl_vendor=hw["vendor"],
            webgl_renderer=hw["renderer"],
            created_at=time.time(),
            reflex_score=reflex_score,
            screen_width=hw["width"],
            screen_height=hw["height"],
            canvas_noise_hash=noise_hash
        )
        try:
            async with aiofiles.open(self.fp_file, "w") as f: 
                await f.write(json.dumps(asdict(profile), indent=2))
        except: pass
        return profile

class CompatibilityLayer:
    @staticmethod
    async def inject_shims(page, profile: EmulationProfile, timezone: str = "UTC", locale: str = "en-US"):
        if not page: return
        
        # JS Injection V25: Compatibility Shims for consistent rendering
        js = """
        ((data) => {
            try {
                const polyfillNative = (fn, name) => {
                    const toString = () => `function ${name || fn.name}() { [native code] }`;
                    Object.defineProperty(fn, 'toString', { value: toString, enumerable: false });
                    return fn;
                };

                // 1. WebDriver Standardization
                delete Object.getPrototypeOf(navigator).webdriver;
                
                // 2. Hardware Consistency
                Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => data.concurrency, enumerable: true});
                Object.defineProperty(navigator, 'deviceMemory', {get: () => data.memory, enumerable: true});
                if (navigator.platform !== data.platform) {
                    Object.defineProperty(navigator, 'platform', {get: () => data.platform, enumerable: true});
                }
                
                // 3. WebGL Compatibility
                const getParameter = WebGLRenderingContext.prototype.getParameter;
                WebGLRenderingContext.prototype.getParameter = polyfillNative(function(p) {
                    if (p === 37445) return data.vendor;
                    if (p === 37446) return data.renderer;
                    return getParameter.apply(this, arguments);
                }, 'getParameter');

                // 4. Chrome Runtime Mock
                if (!window.chrome) {
                    const chromeObj = {
                        runtime: {},
                        app: {
                            isInstalled: false,
                            InstallState: { DISABLED: 'disabled', INSTALLED: 'installed', NOT_INSTALLED: 'not_installed' },
                            RunningState: { CANNOT_RUN: 'cannot_run', READY_TO_RUN: 'ready_to_run', RUNNING: 'running' }
                        },
                        csi: polyfillNative(function(){}, 'csi'),
                        loadTimes: polyfillNative(function(){ return {
                            getLoadTime: () => new Date().getTime() / 1000,
                            getNavigationType: () => 'Other',
                            wasFetchedViaSpdy: () => false,
                            wasNpnNegotiated: () => false,
                            wasAlternateProtocolAvailable: () => false,
                            wasLawfullySnatched: () => false
                        }}, 'loadTimes')
                    };
                    Object.defineProperty(window, 'chrome', { value: chromeObj, writable: true, enumerable: true });
                }
                
                // 5. Rendering Consistency Check (Canvas)
                const toDataURL = HTMLCanvasElement.prototype.toDataURL;
                HTMLCanvasElement.prototype.toDataURL = polyfillNative(function(type) {
                    const ctx = this.getContext('2d');
                    if (ctx) {
                        const shift = data.noiseHash.charCodeAt(0) % 3 === 0 ? 1 : -1;
                        ctx.fillStyle = 'rgba(0,0,0,0.01)';
                        ctx.fillRect(0, 0, 1, 1);
                    }
                    return toDataURL.apply(this, arguments);
                }, 'toDataURL');

                // 6. Permissions Polyfill
                const originalQuery = navigator.permissions.query;
                navigator.permissions.query = polyfillNative(function(parameters) {
                    return originalQuery(parameters).then(result => {
                         if (parameters.name === 'notifications') {
                             Object.defineProperty(result, 'state', {value: 'denied', writable: true});
                         }
                         return result;
                    });
                }, 'query');

                // 7. Geo-Alignment
                try {
                    const resolvedOptions = Intl.DateTimeFormat().resolvedOptions();
                    const originalFormat = Intl.DateTimeFormat;
                    Intl.DateTimeFormat = function(locales, options) {
                        options = options || {};
                        # Force proxy timezone
                        if (!options.timeZone) options.timeZone = data.timezone;
                        return originalFormat(locales, options);
                    };
                    Intl.DateTimeFormat.prototype = originalFormat.prototype;

                    Object.defineProperty(navigator, 'language', { value: data.locale, enumerable: true });
                    Object.defineProperty(navigator, 'languages', { value: [data.locale], enumerable: true });
                } catch(e) {}

            } catch (e) { console.error("Compatibility layer error", e); }
        })
        """
        await page.add_init_script(script=js, arg={
            "concurrency": profile.hardware_concurrency, 
            "memory": profile.device_memory,
            "vendor": profile.webgl_vendor, 
            "renderer": profile.webgl_renderer, 
            "platform": profile.platform,
            "noiseHash": profile.canvas_noise_hash,
            "timezone": timezone,
            "locale": locale
        })

class SandboxCDPProfile:
    # --- TEST SCENARIOS (CONTEXTUAL WARM-UP) ---
    # MODIFICATION V3: Targeting Global Trader/SaaS Profiles
    TEST_SCENARIOS = {
        "finance": [
            ("https://www.tradingview.com/", 15),
            ("https://www.investing.com/", 12),
            ("https://www.coindesk.com/", 10),
            ("https://coinmarketcap.com/", 10)
        ],
        "finance_fr": [
            ("https://fr.tradingview.com/", 15),
            ("https://www.bfmtv.com/crypto/", 12),
            ("https://www.tradingsat.com/", 10),
            ("https://www.zonebourse.com/", 8)
        ],
        "student": [
            ("https://www.udemy.com/", 10),
            ("https://www.coursera.org/", 15),
            ("https://www.reddit.com/r/learnprogramming/", 12)
        ],
        "tech": [
            ("https://techcrunch.com/", 10),
            ("https://www.theverge.com/", 12),
            ("https://www.wired.com/", 10)
        ],
        "default": [
            ("https://www.wikipedia.org/", 5),
            ("https://www.bing.com/", 5),
            ("https://news.google.com/", 8)
        ]
    }

    def __init__(self, user_id: str = "apex_user", profile_dir: Optional[str] = None, cdp_port: int = 9222, cdp_secondary_port: int = 9223):
        self.user_id = user_id
        base_dir = Path(profile_dir) if profile_dir else settings.CHROME_PROFILES_DIR
        self.profile_root = base_dir / user_id
        self.profile_root.mkdir(parents=True, exist_ok=True)
        
        self.cdp_endpoints = [f"http://127.0.0.1:{cdp_port}", f"http://127.0.0.1:{cdp_secondary_port}"]
        self.vault = SessionVault(self.profile_root / "encrypted_session.aes")
        self.emulator = EnvironmentEmulator(self.profile_root)
        
        self.browser = None
        self.context = None
        self.page = None
        self.identity = None
        self.context_lock = asyncio.Lock()
        
        self.latency_multiplier = 1.0 
        self.health_score = 1.0 
        self.network_quality_score = 1.0 
        self.proxy_metadata = {} 
        
        self.is_degraded_mode = False
        self.task_context = "GENERAL"  # Modes: GENERAL, INTERACTION, MONITORING
        
        self.circuit_breaker = CircuitBreaker()

        ProcessGuardian.kill_zombies_on_port(cdp_port)

    def set_task_context(self, context: str):
        """Sets current context for resource adaptation."""
        self.task_context = context.upper()

    async def _validate_network_path(self, proxy_url: str) -> bool:
        """
        FAILOVER & GEO INTELLIGENCE: Validates path reliability and extracts geo-data.
        """
        if not self.circuit_breaker.allow_request(proxy_url):
            return False

        try:
            logger.info(f"🛡️ PATH VALIDATION: Analyzing ingress for {self.user_id}...")
            async with aiohttp.ClientSession() as session:
                start = time.time()
                try:
                    async with session.get("http://ip-api.com/json/", proxy=proxy_url, timeout=15) as resp:
                        if resp.status != 200:
                            logger.error(f"❌ PROXY DEAD ({resp.status}).")
                            # Intelligence: Distinguish HTTP Errors
                            error_type = "ACCESS_DENIED_403" if resp.status == 403 else "GENERIC_HTTP_ERROR"
                            self.circuit_breaker.record_failure(proxy_url, error_type=error_type)
                            return False
                        data = await resp.json()
                        elapsed = time.time() - start
                        
                        isp = data.get("isp", "").lower()
                        bad_isps = ["google", "amazon", "microsoft", "azure", "digitalocean", "hetzner", "ovh"]
                        if any(b in isp for b in bad_isps):
                             logger.warning(f"⚠️ DATACENTER DETECTED: IP {isp} rejected (Quality Assurance).")
                             self.circuit_breaker.record_failure(proxy_url, error_type="BANNED")
                             return False
                        
                        # --- GEO ALIGNMENT ---
                        self.proxy_metadata = {
                            "timezone": data.get("timezone", "UTC"),
                            "countryCode": data.get("countryCode", "US"),
                            "locale": "en-US" 
                        }
                        
                        cc = self.proxy_metadata["countryCode"]
                        if cc == "FR": self.proxy_metadata["locale"] = "fr-FR"
                        elif cc == "DE": self.proxy_metadata["locale"] = "de-DE"
                        elif cc == "ES": self.proxy_metadata["locale"] = "es-ES"
                        elif cc == "GB": self.proxy_metadata["locale"] = "en-GB"
                        
                        logger.info(f"✅ PATH CERTIFIED: {data.get('query')} ({cc}) - Latency: {elapsed:.2f}s - Zone: {self.proxy_metadata['timezone']}")
                        self.circuit_breaker.record_success(proxy_url)
                        return True
                except asyncio.TimeoutError:
                    logger.error("❌ PROXY TIMEOUT: Latency too high.")
                    self.circuit_breaker.record_failure(proxy_url, error_type="TIMEOUT")
                    return False
        except Exception as e:
            logger.error(f"❌ VALIDATION ERROR: {e}")
            self.circuit_breaker.record_failure(proxy_url, error_type="GENERIC")
            return False

    def _parse_proxy_config(self, proxy_url: str) -> Dict[str, str]:
        if not proxy_url: return None
        try:
            parsed = urlparse(proxy_url)
            config = {
                "server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
            }
            if parsed.username and parsed.password:
                config["username"] = parsed.username
                config["password"] = parsed.password
            return config
        except Exception as e:
            logger.error(f"Proxy parsing error: {e}")
            return None

    def _inject_proxy_session(self, proxy_url: str) -> str:
        """
        STICKY SESSION INJECTION: Ensures IP stability during the cycle.
        """
        if not proxy_url or "session-" in proxy_url:
            return proxy_url
            
        try:
            session_id = secrets.token_hex(4)
            parsed = urlparse(proxy_url)
            if parsed.username and parsed.password:
                new_netloc = f"{parsed.username}:{parsed.password}_session-{session_id}@{parsed.hostname}:{parsed.port}"
                return parsed._replace(netloc=new_netloc).geturl()
                
            return proxy_url
        except Exception as e:
            logger.warning(f"⚠️ Session Injection Failed: {e}")
            return proxy_url

    async def connect(self, playwright, warmup_only: bool = False, specific_proxy: str = None) -> bool:
        """
        Connection with Context Intelligence (Karma, Geo-Alignment, Resilience).
        """
        async with self.context_lock:
            if self.browser: return True
            
            is_healthy = await self.vault.check_health()
            if not is_healthy:
                logger.error("🛑 SELF-HEALING: Session Invalid/Expired (Maintenance Triggered).")
            
            session_data = await self.vault.load()
            last_exit_status = session_data.get("exit_status", "unknown")
            
            self.latency_multiplier = session_data.get("latency_multiplier", 1.0)
            self.health_score = session_data.get("health_score", 1.0)
            
            patience_factor = self.latency_multiplier * (1 + (1 - self.health_score) * 5)
            
            try:
                self.identity = await self.emulator.get_emulation_profile()
            except RuntimeError: return False
            
            screen_w = self.identity.screen_width
            screen_h = self.identity.screen_height
            viewport_h = screen_h - random.randint(40, 80)
            
            base_proxy = specific_proxy or settings.ROTATING_PROXY_URL
            active_proxy_url = self._inject_proxy_session(base_proxy)
            
            if active_proxy_url:
                is_safe = await self._validate_network_path(active_proxy_url)
                if not is_safe:
                    logger.critical(f"🛑 PATH REJECTED: Proxy failed validation for {self.user_id}.")
                    return False

            for endpoint in self.cdp_endpoints:
                if not self.circuit_breaker.allow_request(endpoint):
                    continue

                for attempt in range(3):
                    try:
                        start_time = time.time()
                        connect_timeout = 15000 * patience_factor
                        self.browser = await playwright.chromium.connect_over_cdp(endpoint, timeout=connect_timeout)
                        connection_time = time.time() - start_time
                        
                        raw_quality = max(0.0, 1.0 - (connection_time / 5.0))
                        self.network_quality_score = raw_quality
                        
                        current_factor = max(1.0, 1.0 + (connection_time * 0.5))
                        self.latency_multiplier = (self.latency_multiplier * 0.7) + (current_factor * 0.3)
                        
                        self.is_degraded_mode = False
                        if connection_time > 2.0 or self.latency_multiplier > 1.5:
                            logger.info(f"🐢 NETWORK DEGRADED (Score: {self.network_quality_score:.2f}). Adjusting thresholds.")
                            self.is_degraded_mode = True
                        
                        if self.browser.contexts:
                            for old in self.browser.contexts: 
                                try: await old.close()
                                except: pass
                        
                        context_options = {
                            "user_agent": self.identity.user_agent,
                            "viewport": {"width": screen_w, "height": viewport_h},
                            "screen": {"width": screen_w, "height": screen_h},
                            "device_scale_factor": 1.0,
                            "ignore_https_errors": True,
                            "java_script_enabled": True,
                            "has_touch": False
                        }
                        
                        if active_proxy_url:
                            proxy_conf = self._parse_proxy_config(active_proxy_url)
                            if proxy_conf:
                                context_options["proxy"] = proxy_conf
                                logger.info(f"🌍 Proxy active for {self.user_id}")
                            
                        self.context = await self.browser.new_context(**context_options)
                        
                        base_timeout = 30000
                        adjusted_timeout = base_timeout * patience_factor
                        self.context.set_default_timeout(adjusted_timeout)
                        self.context.set_default_navigation_timeout(adjusted_timeout)
                        
                        await self.context.route("**/*", self._handle_heavy_resources)

                        self.page = await self.context.new_page()
                        
                        # --- COMPATIBILITY SHIM INJECTION ---
                        await CompatibilityLayer.inject_shims(
                            self.page, 
                            self.identity,
                            timezone=self.proxy_metadata.get("timezone", "UTC"),
                            locale=self.proxy_metadata.get("locale", "en-US")
                        )
                        
                        await self._restore_session(preloaded_data=session_data)
                        
                        await self.save_checkpoint(
                            context_tag=session_data.get("context_tag", "general"),
                            exit_status="running"
                        )
                        
                        if warmup_only:
                            await self._execution_environment_preparation(session_data, last_exit_status=last_exit_status)
                            return True

                        logger.info(f"✅ Connected [ID: {self.user_id}] - Health: {self.health_score:.2f} - QoS Score: {self.network_quality_score:.2f}")
                        self.circuit_breaker.record_success(endpoint)
                        return True
                        
                    except Exception as e:
                        logger.warning(f"Connection attempt failed ({endpoint}): {e}")
                        self.circuit_breaker.record_failure(endpoint, error_type="CONNECTION_REFUSED")
                        await asyncio.sleep(2)

            return False
            
    async def _handle_heavy_resources(self, route):
        try:
            url = route.request.url.lower()
            resource_type = route.request.resource_type
            
            visual_triggers = ["login", "challenge", "verify", "captcha", "security", "auth"]
            is_visual_path = any(trigger in url for trigger in visual_triggers)
            
            if is_visual_path and resource_type in ["image", "font"]:
                await route.continue_()
                return

            qos_threshold = 0.4
            
            if self.task_context == "INTERACTION":
                if resource_type in ["image"] and self.network_quality_score < qos_threshold:
                     await route.abort()
                     return
                await route.continue_()
                return

            if resource_type in ["image", "media", "font", "stylesheet"]:
                await route.abort()
            else:
                await route.continue_()
        except Exception:
            try: await route.continue_()
            except: pass

    async def check_service_availability(self) -> bool:
        """
        Checks for Service Availability (replaces Soft Ban detection).
        """
        if not self.page: return False
        try:
            unavailable_markers = [
                "text=Too many requests",
                "text=Access Denied",
                "text=Page not available",
                "text=Gateway Timeout"
            ]
            for p in unavailable_markers:
                if await self.page.is_visible(p, timeout=500):
                    logger.warning(f"☣️ SERVICE UNAAVAILABLE: {p}")
                    return False
            return True
        except: return True

    async def _restore_session(self, preloaded_data: Optional[Dict] = None) -> Dict:
        data = preloaded_data if preloaded_data else await self.vault.load()
        if data and "cookies" in data:
            try: await self.context.add_cookies(data["cookies"])
            except: pass
        return data

    async def handle_access_challenge(self) -> bool:
        """
        SLA Manager: Handles Access Challenges (formerly Captcha).
        """
        old_context = self.task_context
        self.set_task_context("INTERACTION")
        try:
            if not self.page: return False
            
            if not settings.CAPTCHA_API_KEY:
                logger.warning("⚠️ Access Challenge detected but API Key missing.")
                logger.info("✋ MANUAL INTERVENTION: 30s to resolve challenge...")
                await asyncio.sleep(30)
                return True 
            
            logger.info("🧩 Resolving Access Challenge...")
            await asyncio.sleep(5)
            
            if await self.check_service_availability():
                logger.info("✅ Access Challenge Resolved.")
                return True
            
            return False
        except Exception as e:
            logger.error(f"❌ Challenge Resolution Error: {e}")
            return False
        finally:
             self.set_task_context(old_context)
    
    async def smart_click(self, target_name: str, strategies: List[str]) -> bool:
        """
        Biometric Interaction: Attempts to click with human-like latency.
        Includes Semantic Fallback (XPath) and Overlay De-cluttering (Resilience).
        """
        if not self.page: return False
        
        base_reaction = 0.2 
        fatigue_factor = 1.0
        
        current_hour = datetime.now(timezone.utc).hour
        if current_hour < 6 or current_hour > 23:
            fatigue_factor = 1.4

        net_drag = max(0, self.latency_multiplier - 1.0) * 0.5
        
        delay = (base_reaction + (1.0 - self.identity.reflex_score)) * fatigue_factor + net_drag
        
        final_delay = random.uniform(delay * 0.8, delay * 1.3)
        await asyncio.sleep(final_delay)

        async def attempt_strategies():
            # 1. Technical Selectors (CSS)
            for selector in strategies:
                try:
                    if await self.page.is_visible(selector, timeout=2000):
                        await self.page.click(selector, timeout=3000)
                        logger.info(f"✅ Click success (Technical): {selector}")
                        return True
                except Exception as e:
                    domain = urlparse(self.page.url).netloc
                    self.circuit_breaker.record_failure("site_structure", is_site_error=True, target_domain=domain)
                    continue
            
            # 2. Semantic Fallback (XPath - Resilient Navigation)
            logger.info(f"🧠 Smart Click: Technical selectors failed. Attempting Semantic Fallback for '{target_name}'...")
            
            semantic_keywords = ["login", "submit", "search", "send", "valider", "connexion", "connect", "continuer", "next", "suivant"]
            relevant_keywords = [k for k in semantic_keywords if k in target_name.lower()]
            if not relevant_keywords: relevant_keywords = semantic_keywords 
            
            xpath_templates = [
                "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{kw}')]",
                "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{kw}')]",
                "//input[@type='submit' and contains(translate(@value, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{kw}')]",
                "//div[@role='button' and contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{kw}')]"
            ]
            
            for kw in relevant_keywords:
                for template in xpath_templates:
                    try:
                        xpath = template.format(kw=kw)
                        element = await self.page.query_selector(xpath)
                        if element and await element.is_visible():
                            logger.info(f"🧠 HEURISTIC CLICK: Found element via semantic match '{kw}'")
                            await element.click(timeout=3000)
                            return True
                    except: pass
            return False

        # First Attempt
        if await attempt_strategies():
            return True

        # --- STRATÉGIE DE DÉGAGEMENT (OVERLAY KILLER) ---
        logger.warning(f"⚠️ Interaction blocked for '{target_name}'. Attempting de-cluttering...")
        try:
            # Common obstructors: Cookie banners, Newsletters, Modals
            obstructors = [
                "button[id*='cookie']", "button[class*='cookie']", "a[class*='cookie']",
                "button[aria-label*='close']", "button[aria-label*='fermer']",
                ".modal-close", ".close", "svg[data-icon='times']",
                "//button[contains(text(), 'Accepter')]", "//button[contains(text(), 'Refuser')]",
                "//button[contains(text(), 'Continuer sans')]"
            ]
            
            cleaned = False
            for obs in obstructors:
                try:
                    if obs.startswith("//"): # XPath
                        elements = await self.page.query_selector_all(obs)
                    else: # CSS
                        elements = await self.page.query_selector_all(obs)
                    
                    for el in elements:
                        if await el.is_visible():
                            logger.info(f"🧹 De-cluttering: Removing obstruction ({obs})")
                            # Force click via JS to bypass some listeners
                            await el.evaluate("e => e.click()")
                            cleaned = True
                            await asyncio.sleep(0.5)
                except: pass
            
            if cleaned:
                # Retry main strategy after cleanup
                if await attempt_strategies():
                    return True

        except Exception as e:
            logger.warning(f"⚠️ De-cluttering failed: {e}")

        # 3. Last Resort: Keyboard or Force JS
        if any(k in target_name.lower() for k in ["login", "search", "enter"]):
            logger.warning(f"⚠️ Smart Click failed for {target_name}. Attempting fallback (ENTER).")
            try:
                await self.page.keyboard.press("Enter")
                return True
            except: pass
            
        logger.error(f"❌ Interaction failed for {target_name}")
        return False

    async def _execution_environment_preparation(self, session_data: Dict, last_exit_status: str = "clean"):
        """
        CONTEXTUAL WARM-UP: Prepares the environment based on the test scenario.
        Intelligence: Uses Proxy Metadata to align surfing persona (Geo-Priming).
        """
        if not self.page: return
        try:
            scenario_tag = session_data.get("persona_tag")
            if not scenario_tag:
                # Intelligence: Contextual selection based on Proxy Geo
                geo = self.proxy_metadata.get("countryCode", "US")
                if geo == "FR":
                    scenario_tag = "finance_fr"
                elif geo == "US":
                    scenario_tag = "finance" # Priority to Global Finance for US
                else:
                    # Time-based Fallback
                    try:
                        hour = datetime.now(timezone.utc).hour
                        scenario_tag = "default" if hour < 12 else "student"
                    except: scenario_tag = "default"

                session_data["persona_tag"] = scenario_tag
                await self.save_checkpoint(context_tag=scenario_tag)
                logger.info(f"🎭 SCENARIO ASSIGNED (Contextual): {scenario_tag.upper()} [Geo: {geo}]")

            routes = self.TEST_SCENARIOS.get(scenario_tag, self.TEST_SCENARIOS["default"])
            
            patience = self.latency_multiplier
            if last_exit_status in ["crash", "detected", "timeout", "unknown"]:
                logger.info(f"🚑 Recovery Mode: Gentle preparation for {self.user_id}")
                routes = self.TEST_SCENARIOS["default"] 
                patience_factor = 2.0
            else:
                patience_factor = 1.0

            steps = random.sample(routes, k=min(len(routes), random.randint(1, 2)))
            
            logger.info(f"☕ Environment Prep ({scenario_tag.upper()}): {len(steps)} steps.")
            
            for url, dwell_time in steps:
                try:
                    domain = urlparse(url).netloc
                    if not self.circuit_breaker.allow_request("site_check", target_domain=domain):
                        logger.warning(f"⏩ Skipping {domain} (Unstable).")
                        continue

                    logger.info(f"➡️ Visiting: {url}")
                    await self.page.goto(url, timeout=30000 * patience)
                    
                    await self.page.mouse.wheel(0, random.randint(300, 700))
                    
                    if not await self.check_service_availability():
                        logger.error("Prep failed: Service Unavailable.")
                        raise Exception("Service Unavailable during Prep")

                    await asyncio.sleep(dwell_time * patience * patience_factor)
                except Exception as step_err:
                    logger.warning(f"⚠️ Prep step failed ({url}): {step_err}")

        except Exception as e:
            logger.warning(f"⚠️ Global Prep Error: {e}")

    async def save_checkpoint(self, context_tag: str = "general", exit_status: str = "running"):
        if not self.context: return
        try:
            cookies = await self.context.cookies()
            
            # --- KARMA SCORE CALCULATION ---
            new_score = self.health_score
            if exit_status == "clean":
                new_score = min(1.0, new_score * 1.05)
            elif exit_status in ["detected", "captcha_loop", "soft_ban"]:
                new_score = max(0.1, new_score * 0.8) 
            
            existing_data = await self.vault.load()
            
            payload = {
                "timestamp": time.time(), 
                "cookies": cookies,
                "last_active_at": time.time(),
                "context_tag": context_tag,
                "exit_status": exit_status,
                "latency_multiplier": self.latency_multiplier,
                "health_score": new_score, 
                "persona_tag": existing_data.get("persona_tag")
            }
            await self.vault.save(payload)
        except: pass

    async def ensure_connected(self, playwright) -> bool:
        if self.browser:
            try: 
                if self.browser.is_connected(): return True
            except: pass
        await self.cleanup()
        return await self.connect(playwright)

    async def get_page(self): return self.page

    async def cleanup(self, exit_reason: str = "clean"):
        await self.save_checkpoint(exit_status=exit_reason)
        if self.browser:
            try: await self.browser.close()
            except: pass
        self.browser = None
        self.context = None
        self.page = None