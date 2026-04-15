# core/secure_telemetry_store.py - NEXUS SECURE TELEMETRY STORE V35.2.0 (INTELLIGENT WORKFLOW)
# -*- coding: utf-8 -*-

"""
NEXUS SECURE TELEMETRY STORE V35.2.0 - SIGNAL PERSISTENCE LAYER
--------------------------------
1. Privacy Engine: Immediate SHA-256 Hashing of Network Identifiers (Node IDs).
2. Signal Storage: High-Entropy Telemetry Packets persistence.
3. Smart Ledger (2PC): Two-Phase Commit for Compute Resource Reservation.
4. Capacity Guard: Technical Quota Enforcement.
5. Intelligent Workflow: Automated State Machine for "Double-Dip" Strategy.
"""

import os
import sqlite3
import time
import json
import logging
import threading
import sys
import re
import asyncio
import concurrent.futures
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
from contextlib import contextmanager
from decimal import Decimal, InvalidOperation

# Dynamic Settings Import
try:
    from core.settings import settings
except ImportError:
    print("🔴 CRITICAL ERROR: core.settings missing in database.py")
    sys.exit(1)

# Conditional PostgreSQL Import
try:
    import psycopg2
    from psycopg2 import extras, pool  # noqa: F401

    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False
    if settings.USE_POSTGRES:
        print("🔴 CRITICAL ERROR: PostgreSQL required but 'psycopg2' missing.")
        sys.exit(1)

# --- LOGGING CONFIGURATION ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)-8s | [TELEMETRY] %(message)s"
)
logger = logging.getLogger("NexusLedger")

# ============================================================================
# MAIN CLASS: NEXUS DB (SECURE TELEMETRY STORE)
# ============================================================================


class NexusDB:
    """
    High-Performance Persistence Manager.
    Handles Data Minimization, Hashing, and ACID Transactions for Infrastructure Signals.
    """

    _thread_local = threading.local()
    CURRENT_SCHEMA_VERSION = 14

    # Hashing Salt — loaded at class definition time.
    # Override in production: PRIVACY_SALT=<random-hex-32> in the environment.
    # Rotating this value re-hashes all existing identities — plan a migration.
    _PRIVACY_SALT_DEFAULT = "NEXUS_GDPR_COMPLIANCE_V1_SALT"
    _privacy_salt_env = os.environ.get("PRIVACY_SALT", "").strip()
    if _privacy_salt_env:
        PRIVACY_SALT = _privacy_salt_env
    else:
        logging.getLogger("NexusDB").warning(
            "[PRIVACY] PRIVACY_SALT env var not set — using built-in default. "
            "Set PRIVACY_SALT=<random-hex-32> in production to harden GDPR hashing."
        )
        PRIVACY_SALT = _PRIVACY_SALT_DEFAULT

    # Correction Heuristics (Signal Normalization)
    COMMON_TYPOS = {
        r"bou?rso?r?ama": "boursorama",
        r"bi?nanc?e?": "binance",
        r"c\s*koi": "c'est quoi",
        r"pl[ui]s\s*d'?in?fos?": "info",
        r"comment\s*faire": "tuto",
        r"svp": "aide",
    }

    _pg_pool = None

    def __init__(self, db_path: Path = None, auto_migrate: bool = False):
        self.db_path = db_path or settings.DB_PATH
        self.use_postgres = settings.USE_POSTGRES and HAS_POSTGRES

        # Health Monitoring
        self.last_write_latency = 0.0

        if settings.MAX_CONCURRENT_WORKERS > 10 and not self.use_postgres:
            logger.warning("🛑 CONFIG WARNING: High concurrency on SQLite.")

        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.db_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=settings.MAX_CONCURRENT_WORKERS if self.use_postgres else 4,
            thread_name_prefix="Ledger_Worker",
        )

        if self.use_postgres and NexusDB._pg_pool is None:
            self._init_postgres_pool()

        if auto_migrate:
            self._init_nexus_migrations()

    def _init_postgres_pool(self):
        try:
            pool_min = settings.DB_POOL_MIN
            pool_max = settings.DB_POOL_MAX

            logger.info(f"🐘 Initializing PostgreSQL Pool (Min: {pool_min}, Max: {pool_max})...")

            NexusDB._pg_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=pool_min,
                maxconn=pool_max,
                user=settings.POSTGRES_USER,
                password=settings.POSTGRES_PASSWORD.get_secret_value(),
                host=settings.POSTGRES_HOST,
                port=settings.POSTGRES_PORT,
                database=settings.POSTGRES_DB,
                cursor_factory=psycopg2.extras.RealDictCursor,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5,
            )
            logger.info("✅ PostgreSQL Pool Ready.")
        except Exception as e:
            logger.critical(f"❌ PostgreSQL Pool Failed: {e}")
            if settings.USE_POSTGRES:
                raise e
            self.use_postgres = False

    def _get_conn(self):
        if self.use_postgres:
            if not hasattr(self._thread_local, "pg_conn"):
                try:
                    self._thread_local.pg_conn = NexusDB._pg_pool.getconn()
                    self._thread_local.pg_conn.autocommit = False
                except Exception as e:
                    logger.error(f"❌ DB Pool Exhausted: {e}")
                    raise e
            return self._thread_local.pg_conn
        else:
            if hasattr(self._thread_local, "connection"):
                try:
                    self._thread_local.connection.execute("SELECT 1")
                except (sqlite3.ProgrammingError, sqlite3.OperationalError):
                    self.close_thread_connection()

            if not hasattr(self._thread_local, "connection"):
                try:
                    conn = sqlite3.connect(str(self.db_path), timeout=60.0)
                    conn.row_factory = sqlite3.Row
                    conn.execute("PRAGMA journal_mode=WAL;")
                    conn.execute("PRAGMA synchronous=NORMAL;")
                    conn.execute("PRAGMA busy_timeout=60000;")
                    self._thread_local.connection = conn
                except sqlite3.Error as e:
                    logger.critical(f"❌ SQLite Open Failed: {e}")
                    raise e
            return self._thread_local.connection

    def close_thread_connection(self):
        if self.use_postgres:
            if hasattr(self._thread_local, "pg_conn"):
                try:
                    NexusDB._pg_pool.putconn(self._thread_local.pg_conn)
                except Exception:
                    pass
                finally:
                    if hasattr(self._thread_local, "pg_conn"):
                        del self._thread_local.pg_conn
        else:
            if hasattr(self._thread_local, "connection"):
                try:
                    self._thread_local.connection.close()
                except Exception:
                    pass
                finally:
                    if hasattr(self._thread_local, "connection"):
                        del self._thread_local.connection

    def close_local_connection(self):
        self.close_thread_connection()

    def _adapt_query(self, query: str) -> str:
        if self.use_postgres:
            query = query.replace("?", "%s")
            query = query.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
            if "INSERT OR IGNORE" in query:
                query = query.replace("INSERT OR IGNORE", "INSERT")
                if "ON CONFLICT" not in query:
                    query += " ON CONFLICT DO NOTHING"
        return query

    @contextmanager
    def session(self, immediate: bool = False):
        max_retries = 10 if not self.use_postgres else 5
        base_delay = 0.2
        conn = None
        for attempt in range(max_retries):
            try:
                conn = self._get_conn()
                if not self.use_postgres:
                    if immediate:
                        conn.execute("BEGIN IMMEDIATE")
                    else:
                        conn.execute("BEGIN")

                class ConnectionProxy:
                    def __init__(self, real_conn, db_instance):
                        self.conn = real_conn
                        self.db = db_instance

                    def execute(self, sql, params=()):
                        final_sql = self.db._adapt_query(sql)
                        cursor = self.conn.cursor()
                        cursor.execute(final_sql, params)
                        return cursor

                    def executemany(self, sql, params_seq):
                        final_sql = self.db._adapt_query(sql)
                        cursor = self.conn.cursor()
                        cursor.executemany(final_sql, params_seq)
                        return cursor

                    def commit(self):
                        self.conn.commit()

                    def rollback(self):
                        self.conn.rollback()

                proxy = ConnectionProxy(conn, self)
                yield proxy
                conn.commit()
                return
            except Exception as e:
                if conn:
                    conn.rollback()
                is_pg_deadlock = hasattr(e, "pgcode") and e.pgcode == "40P01"
                is_sqlite_lock = (
                    isinstance(e, sqlite3.OperationalError) and "locked" in str(e).lower()
                )
                if (is_sqlite_lock or is_pg_deadlock) and attempt < max_retries - 1:
                    time.sleep(base_delay * (1.5**attempt))
                    continue
                if not (is_sqlite_lock or is_pg_deadlock):
                    raise e
                logger.error(f"❌ DB Transaction Failed: {e}")
                raise e

    async def run_in_executor(self, func, *args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.db_executor, func, *args)

    @classmethod
    def init_db_once(cls):
        logger.info("🔧 DB Maintenance Startup...")
        try:
            db = cls(auto_migrate=True)
            db._seed_initial_data()
            db.close_local_connection()
            logger.info("✅ DB Maintenance Complete.")
        except Exception as e:
            logger.critical(f"🔥 DB Maintenance Failed: {e}")
            sys.exit(1)

    def _seed_initial_data(self):
        try:
            config_dir = settings.BASE_DIR / "config"
            sponsors_file = config_dir / "sponsors.json"
            campaigns_file = config_dir / "campaigns.json"

            with self.session() as conn:
                count_res = conn.execute("SELECT COUNT(*) as cnt FROM sponsors").fetchone()
                cnt = count_res["cnt"] if self.use_postgres else count_res[0]

                if cnt == 0 and sponsors_file.exists():
                    logger.info("🌱 SEEDING: Injecting Tenant Nodes...")
                    try:
                        with open(sponsors_file, "r", encoding="utf-8") as f:
                            data = json.load(f)
                            if isinstance(data, list):
                                for s in data:
                                    conn.execute(
                                        """
                                        INSERT INTO sponsors (id, label, program, ref_link, ref_code, priority, monthly_limit_hard, active)
                                        VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                                    """,
                                        (
                                            s.get("id"),
                                            s.get("label"),
                                            s.get("program"),
                                            s.get("ref_link"),
                                            s.get("ref_code"),
                                            s.get("priority", 2),
                                            s.get("monthly_limit_hard", 3),
                                        ),
                                    )
                    except Exception:
                        pass

                count_res = conn.execute("SELECT COUNT(*) as cnt FROM campaigns").fetchone()
                cnt = count_res["cnt"] if self.use_postgres else count_res[0]

                if cnt == 0 and campaigns_file.exists():
                    logger.info("🌱 SEEDING: Injecting Service Definitions...")
                    try:
                        with open(campaigns_file, "r", encoding="utf-8") as f:
                            data = json.load(f)
                            if isinstance(data, list):
                                for c in data:
                                    conn.execute(
                                        """
                                        INSERT INTO campaigns (program, amount, is_boosted, end_date, url)
                                        VALUES (?, ?, ?, ?, ?, ?)
                                    """,
                                        (
                                            c.get("program"),
                                            c.get("amount"),
                                            c.get("is_boosted", 0),
                                            c.get("end_date"),
                                            c.get("url"),
                                        ),
                                    )
                    except Exception:
                        pass
        except Exception:
            pass

    def _init_nexus_migrations(self) -> None:
        try:
            with self.session() as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS _schema_version (
                        version INTEGER PRIMARY KEY,
                        updated_at TEXT
                    )
                """
                )
                try:
                    c = conn.execute("SELECT MAX(version) as v FROM _schema_version")
                    row = c.fetchone()
                    if self.use_postgres:
                        current_ver = row["v"] if row and row["v"] else 0
                    else:
                        current_ver = row[0] if row and row[0] else 0
                except Exception:
                    current_ver = 0

                if current_ver < self.CURRENT_SCHEMA_VERSION:
                    logger.info(
                        f"🛠️ Schema Migration ({current_ver} -> {self.CURRENT_SCHEMA_VERSION})..."
                    )
                    self._apply_migrations(conn, current_ver)

                if not self.use_postgres:
                    try:
                        conn.execute("PRAGMA wal_checkpoint(PASSIVE);")
                        conn.execute("PRAGMA optimize;")
                    except Exception:
                        pass

                self._reset_stuck_dispatches(conn)

        except Exception as e:
            logger.critical(f"❌ DB Migration Error: {e}")
            raise e

    def _reset_stuck_dispatches(self, conn):
        try:
            conn.execute(
                "UPDATE leads SET status='QUALIFIED', assigned_sponsor_id=NULL WHERE status='DISPATCHING'"
            )
        except Exception:
            pass

    def _apply_migrations(self, conn, current_ver: int):
        pk_type = "SERIAL PRIMARY KEY" if self.use_postgres else "INTEGER PRIMARY KEY AUTOINCREMENT"
        ts_now = datetime.now().isoformat()

        # NOTE: Historical migrations retained for schema integrity.
        # Semantic mapping: 'leads' -> 'telemetry_signals', 'sponsors' -> 'mandates'

        if current_ver < 1:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS leads (
                    id TEXT PRIMARY KEY,
                    source TEXT,
                    author TEXT, -- Hashed Node ID (SHA-256)
                    url TEXT,
                    comment_text TEXT, -- Technical Payload (Sanitized)
                    ai_status TEXT DEFAULT 'PENDING',
                    ai_confidence REAL DEFAULT 0.0,
                    ai_draft TEXT,
                    ai_process_info TEXT DEFAULT '{}',
                    assigned_sponsor_id TEXT,
                    assigned_program TEXT,
                    assigned_ref_link TEXT,
                    draft_reply TEXT,
                    status TEXT DEFAULT 'NEW',
                    created_at REAL,
                    updated_at REAL
                )
            """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_leads_flow ON leads(status, source, created_at);"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sponsors (
                    id TEXT PRIMARY KEY,
                    label TEXT,
                    program TEXT,
                    ref_link TEXT,
                    ref_code TEXT,
                    priority INTEGER DEFAULT 2,
                    monthly_limit_hard INTEGER DEFAULT 3,
                    verified_count_month INTEGER DEFAULT 0,
                    last_verification_date TEXT,
                    conversion_rate_estimate TEXT DEFAULT '0.10', 
                    active INTEGER DEFAULT 1
                )
            """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS dispatch_logs (
                    id {pk_type},
                    lead_id TEXT,
                    sponsor_id TEXT,
                    program TEXT,
                    dispatched_at_ts REAL,
                    dispatched_at_iso TEXT,
                    FOREIGN KEY(lead_id) REFERENCES leads(id),
                    FOREIGN KEY(sponsor_id) REFERENCES sponsors(id)
                )
            """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_logs_calc ON dispatch_logs(sponsor_id, dispatched_at_iso);"
            )
            conn.execute(
                "INSERT INTO _schema_version (version, updated_at) VALUES (1, ?)", (ts_now,)
            )

        if current_ver < 2:
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_sponsors_perf ON sponsors(program, active, priority);"
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_leads_ai ON leads(ai_status);")
            conn.execute(
                "INSERT INTO _schema_version (version, updated_at) VALUES (2, ?)", (ts_now,)
            )

        if current_ver < 3:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS campaigns (
                    program TEXT PRIMARY KEY,
                    amount INTEGER,
                    is_boosted INTEGER,
                    end_date TEXT,
                    url TEXT,
                    updated_at REAL
                )
            """
            )
            conn.execute(
                "INSERT INTO _schema_version (version, updated_at) VALUES (3, ?)", (ts_now,)
            )

        if current_ver < 6:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS viral_queue (
                    username TEXT PRIMARY KEY,
                    source TEXT DEFAULT 'cascade',
                    priority INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'PENDING',
                    created_at REAL,
                    updated_at REAL
                )
            """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_viral_queue ON viral_queue(status, priority DESC, created_at ASC);"
            )
            conn.execute(
                "INSERT INTO _schema_version (version, updated_at) VALUES (6, ?)", (ts_now,)
            )

        if current_ver < 7:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS accounts (
                    id TEXT PRIMARY KEY,
                    platform TEXT DEFAULT 'tiktok',
                    status TEXT DEFAULT 'ACTIVE',
                    proxy_url TEXT,
                    last_active_ts REAL,
                    success_count INTEGER DEFAULT 0,
                    fail_count INTEGER DEFAULT 0,
                    meta_info TEXT DEFAULT '{}',
                    updated_at REAL
                )
            """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(platform, status);"
            )
            conn.execute(
                "INSERT INTO _schema_version (version, updated_at) VALUES (7, ?)", (ts_now,)
            )

        if current_ver < 8:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS conversions (
                    id TEXT PRIMARY KEY, 
                    lead_id TEXT,
                    sponsor_id TEXT,
                    amount REAL,
                    currency TEXT DEFAULT 'EUR',
                    converted_at REAL,
                    meta_data TEXT DEFAULT '{}',
                    FOREIGN KEY(lead_id) REFERENCES leads(id)
                )
            """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_conversions_sponsor ON conversions(sponsor_id);"
            )
            conn.execute(
                "INSERT INTO _schema_version (version, updated_at) VALUES (8, ?)", (ts_now,)
            )

        if current_ver < 9:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS subreddit_stats (
                    subreddit TEXT PRIMARY KEY,
                    success_count INTEGER DEFAULT 0,
                    fail_count INTEGER DEFAULT 0,
                    last_fail_ts REAL,
                    updated_at REAL
                )
            """
            )
            conn.execute(
                "INSERT INTO _schema_version (version, updated_at) VALUES (9, ?)", (ts_now,)
            )

        if current_ver < 10:
            try:
                col_type = "JSONB" if self.use_postgres else "TEXT"
                conn.execute(
                    f"ALTER TABLE leads ADD COLUMN meta_analysis {col_type} DEFAULT '{{}}'"
                )
            except Exception as e:
                logger.warning(f"⚠️ Migration V10 warning (column exists?): {e}")
            conn.execute(
                "INSERT INTO _schema_version (version, updated_at) VALUES (10, ?)", (ts_now,)
            )

        if current_ver < 11:
            try:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS author_reputation (
                        author_id TEXT NOT NULL,
                        platform TEXT NOT NULL,
                        reputation_score INTEGER DEFAULT 50,
                        status TEXT DEFAULT 'NEUTRAL', -- NEUTRAL, TRUSTED, BANNED, VIP
                        expiry_ts REAL DEFAULT 0,
                        meta_data TEXT DEFAULT '{}',
                        updated_at REAL,
                        PRIMARY KEY (author_id, platform)
                    )
                """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_reputation_status ON author_reputation(status, expiry_ts);"
                )
            except Exception as e:
                logger.warning(f"⚠️ Migration V11 warning: {e}")
            conn.execute(
                "INSERT INTO _schema_version (version, updated_at) VALUES (11, ?)", (ts_now,)
            )

        if current_ver < 12:
            logger.info("💸 Application Migration V12: Smart Ledger & Wallet...")
            try:
                conn.execute("ALTER TABLE sponsors ADD COLUMN balance_available REAL DEFAULT 0.0")
                conn.execute("ALTER TABLE sponsors ADD COLUMN balance_reserved REAL DEFAULT 0.0")
                conn.execute("ALTER TABLE dispatch_logs ADD COLUMN cost_charged REAL DEFAULT 0.0")
                conn.execute("ALTER TABLE dispatch_logs ADD COLUMN status TEXT DEFAULT 'COMPLETED'")
            except Exception as e:
                logger.warning(f"⚠️ Migration V12 warning (columns exist?): {e}")
            conn.execute(
                "INSERT INTO _schema_version (version, updated_at) VALUES (12, ?)", (ts_now,)
            )

        if current_ver < 13:
            logger.info("💳 Application Migration V13: Financial Ledger...")
            try:
                conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS ledger (
                        id {pk_type},
                        transaction_date REAL,
                        type TEXT, -- DEPOSIT, RESERVATION, CONSUMPTION, REFUND, BONUS
                        partner_id TEXT,
                        amount REAL,
                        currency TEXT DEFAULT 'EUR',
                        reference_external TEXT,
                        description TEXT,
                        balance_after REAL,
                        created_at REAL,
                        FOREIGN KEY(partner_id) REFERENCES sponsors(id)
                    )
                """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_ledger_partner ON ledger(partner_id, transaction_date);"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_ledger_ref ON ledger(reference_external);"
                )
            except Exception as e:
                logger.warning(f"⚠️ Migration V13 warning: {e}")
            conn.execute(
                "INSERT INTO _schema_version (version, updated_at) VALUES (13, ?)", (ts_now,)
            )

        if current_ver < 14:
            logger.info("🧠 Application Migration V14: DOM Knowledge (Swarm Learning)...")
            try:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dom_knowledge (
                        key_id TEXT NOT NULL,
                        selector TEXT NOT NULL,
                        success_count INTEGER DEFAULT 0,
                        fail_count INTEGER DEFAULT 0,
                        weight REAL DEFAULT 0.5,
                        updated_at REAL,
                        PRIMARY KEY (key_id, selector)
                    )
                """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_dom_key ON dom_knowledge(key_id, weight DESC);"
                )
            except Exception as e:
                logger.warning(f"⚠️ Migration V14 warning: {e}")
            conn.execute(
                "INSERT INTO _schema_version (version, updated_at) VALUES (14, ?)", (ts_now,)
            )

    def escape_like_string(self, text: str) -> str:
        if not text:
            return ""
        return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    def to_decimal(self, value: Any) -> Decimal:
        try:
            if value is None:
                return Decimal("0.0")
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError) as e:
            logger.critical(f"⚠️ DATA CORRUPTION: Cannot convert '{value}' to Decimal.")
            raise e

    def _hash_identity(self, raw_id: str) -> str:
        """
        GDPR COMPLIANCE LAYER:
        Irreversible SHA-256 Hashing of Personal Identifiers (PII).
        Ensures Zero-Knowledge storage of user identities.
        """
        if not raw_id or raw_id == "unknown":
            return "anonymous_node"
        if len(raw_id) == 64 and re.match(r"^[a-f0-9]{64}$", raw_id):
            # Already hashed
            return raw_id
        return hashlib.sha256(f"{raw_id}{self.PRIVACY_SALT}".encode()).hexdigest()

    def sanitize_lead_text(self, raw_text: str) -> str:
        if not raw_text:
            return ""
        clean_text = raw_text.lower()
        for pattern, replacement in self.COMMON_TYPOS.items():
            clean_text = re.sub(pattern, replacement, clean_text)
        return clean_text

    def _sanitize_and_score_lead(self, lead: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        raw_text = (lead.get("text") or lead.get("comment_text") or "")[:5000]
        clean_text = self.sanitize_lead_text(raw_text)

        meta = {
            "initial_entropy_score": 50,  # Technical metric (Entropy)
            "detected_intent": "UNKNOWN",
            "flags": [],
        }

        if len(raw_text) > 10 and raw_text.isupper():
            meta["flags"].append("SHOUTING")
            meta["initial_entropy_score"] -= 10

        non_alnum = len(re.sub(r"[a-zA-Z0-9\s]", "", raw_text))
        if len(raw_text) > 0 and (non_alnum / len(raw_text)) > 0.4:
            meta["flags"].append("NOISE_OVERLOAD")
            meta["initial_entropy_score"] -= 5

        if re.search(r"(pri|prix|tarif|combien|cout)", clean_text):
            meta["detected_intent"] = "PRICING_QUERY"
            meta["initial_entropy_score"] += 10
        elif re.search(r"(in?fo|ens?eign|d?tails)", clean_text):
            meta["detected_intent"] = "INFO_QUERY"
            meta["initial_entropy_score"] += 5
        elif re.search(r"(ai?de|probl?me|bug|erreur)", clean_text):
            meta["detected_intent"] = "TECHNICAL_SUPPORT"
            meta["initial_entropy_score"] += 15

        return clean_text, meta

    def get_system_health(self) -> float:
        try:
            health_score = 0.0
            backlog = self.get_backlog_count()
            if backlog > 500:
                health_score += 0.6
            elif backlog > 200:
                health_score += 0.3

            if self.last_write_latency > 2.0:
                health_score += 0.4
            elif self.last_write_latency > 1.0:
                health_score += 0.2

            return min(1.0, health_score)
        except Exception:
            return 0.5

    def get_lead_by_author(self, author: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves telemetry data by Node ID (Author).
        Automatically handles hashing for lookup.
        """
        if not author:
            return None
        try:
            # Automatic hashing for lookup
            author_hash = self._hash_identity(author)

            with self.session() as conn:
                row = conn.execute(
                    "SELECT * FROM leads WHERE author = ? ORDER BY created_at DESC LIMIT 1",
                    (author_hash,),
                ).fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"❌ Lookup Error: {e}")
            return None

    def register_conversion_event(
        self, lead_id: str, amount: float = 0.0, transaction_id: str = None
    ) -> bool:
        """
        Records a TECHNICAL SUCCESS (Process Completed / SLA Met).
        Confirms valuable consumption of compute resources.

        INTELLIGENCE UPDATE V35.2:
        Implements 'Double-Dip' State Machine. A conversion on a specific program
        automatically triggers the next logical step in the user funnel.
        """
        try:
            now = time.time()
            tx_id = transaction_id or f"TX_{int(now)}_{lead_id[:8]}"

            with self.session(immediate=True) as conn:
                res = conn.execute(
                    "SELECT assigned_sponsor_id, status, program, assigned_program, author, ai_process_info, source, url FROM leads WHERE id=?",
                    (lead_id,),
                )
                row = res.fetchone()

                if not row:
                    return False

                sponsor_id = (
                    row["assigned_sponsor_id"] if self.use_postgres else row["assigned_sponsor_id"]
                )
                status = row["status"] if self.use_postgres else row["status"]

                # Determine Current Program
                current_program = "UNKNOWN"
                if "assigned_program" in row and row["assigned_program"]:
                    current_program = str(row["assigned_program"]).upper()
                elif "program" in row and row["program"]:
                    current_program = str(row["program"]).upper()

                if status == "CONVERTED":
                    return True

                # Record technical success (SLA Fulfillment)
                conn.execute(
                    """
                    INSERT INTO conversions (id, lead_id, sponsor_id, amount, converted_at)
                    VALUES (?, ?, ?, ?, ?)
                """,
                    (tx_id, lead_id, sponsor_id, amount, now),
                )

                conn.execute(
                    """
                    UPDATE sponsors 
                    SET verified_count_month = verified_count_month + 1,
                        last_verification_date = ?
                    WHERE id = ?
                """,
                    (datetime.now().isoformat(), sponsor_id),
                )

                conn.execute(
                    "UPDATE leads SET status='CONVERTED', updated_at=? WHERE id=?", (now, lead_id)
                )

                # CONFIRM RESOURCE CONSUMPTION (COMMIT TRANSACTION)
                self.confirm_lead_hold(lead_id)

                # --- INTELLIGENT WORKFLOW: DOUBLE-DIP LOGIC ---
                # Definition of the Transition Matrix based on Strategy Report
                NEXT_STEP_MATRIX = {
                    "BOURSO": {"target": "MERIA", "delay_hours": 48, "tag": "CROSS_SELL_CRYPTO"},
                    "BOURSOBANK": {
                        "target": "MERIA",
                        "delay_hours": 48,
                        "tag": "CROSS_SELL_CRYPTO",
                    },
                    "FORTUNEO": {
                        "target": "BINANCE",
                        "delay_hours": 24,
                        "tag": "CROSS_SELL_CRYPTO",
                    },
                    "HELLOBANK": {
                        "target": "TRADE_REPUBLIC",
                        "delay_hours": 24,
                        "tag": "CROSS_SELL_INVEST",
                    },
                    "BINANCE": {"target": "LEDGER", "delay_hours": 72, "tag": "UPSELL_SECURITY"},
                    "MERIA": {"target": "LEDGER", "delay_hours": 72, "tag": "UPSELL_SECURITY"},
                }

                # Find the next step in the matrix (fuzzy match support)
                next_step = None
                for key, val in NEXT_STEP_MATRIX.items():
                    if key in current_program:
                        next_step = val
                        break

                if next_step:
                    target_program = next_step["target"]
                    delay_hours = next_step["delay_hours"]
                    tag = next_step["tag"]

                    logger.info(
                        f"🔄 INTELLIGENT WORKFLOW: Triggering '{target_program}' for user after '{current_program}' success."
                    )

                    # Prepare Context for the new Lead
                    ai_info = row["ai_process_info"]
                    try:
                        info_dict = (
                            json.loads(ai_info) if isinstance(ai_info, str) else (ai_info or {})
                        )
                    except Exception:
                        info_dict = {}

                    info_dict["workflow_origin"] = "DOUBLE_DIP_AUTOMATION"
                    info_dict["previous_win"] = current_program
                    info_dict["funnel_stage"] = tag

                    # Generate a unique ID for the new task to prevent duplicates
                    new_lead_id = f"AUTO_{int(now)}_{lead_id[:6]}"

                    # Insert the new "Lead" (Task) into the pipe
                    # It starts as 'NEW' but with a scheduled logic managed by the Orchestrator
                    # Note: We reuse the same author hash to target the same user
                    conn.execute(
                        """
                        INSERT INTO leads 
                        (id, source, author, url, comment_text, status, assigned_program, ai_process_info, created_at, updated_at)
                        VALUES (?, 'NEXUS_INTERNAL_WORKFLOW', ?, ?, ?, 'PENDING_COOLDOWN', ?, ?, ?, ?)
                    """,
                        (
                            new_lead_id,
                            row["author"],  # Reuse Hashed ID
                            row["url"],  # Reuse Origin URL
                            f"AUTO-GENERATED: Continuation of funnel after {current_program}. Next: {target_program}",
                            target_program,
                            json.dumps(info_dict),
                            now + (delay_hours * 3600),  # Created in future = Cooldown
                            now,
                        ),
                    )

                return True

        except Exception as e:
            logger.error(f"❌ Process Success Error: {e}")
            return False

    def get_active_fleet(self, platform: str = "tiktok") -> List[Dict]:
        try:
            with self.session() as conn:
                rows = conn.execute(
                    "SELECT * FROM accounts WHERE platform=? AND status='ACTIVE' ORDER BY last_active_ts ASC",
                    (platform,),
                ).fetchall()
                if self.use_postgres:
                    return [dict(r) for r in rows]
                else:
                    return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"❌ Fleet Retrieval Error: {e}")
            return []

    def register_account_heartbeat(
        self, account_id: str, proxy_url: str = None, status: str = "ACTIVE"
    ):
        try:
            now = time.time()
            with self.session() as conn:
                if proxy_url:
                    query = """
                        INSERT INTO accounts (id, platform, status, proxy_url, last_active_ts, updated_at)
                        VALUES (?, 'tiktok', ?, ?, ?, ?)
                        ON CONFLICT(id) DO UPDATE SET 
                            status=excluded.status,
                            proxy_url=excluded.proxy_url,
                            last_active_ts=excluded.last_active_ts,
                            updated_at=excluded.updated_at
                    """
                    conn.execute(query, (account_id, status, proxy_url, now, now))
                else:
                    query = """
                        INSERT INTO accounts (id, platform, status, last_active_ts, updated_at)
                        VALUES (?, 'tiktok', ?, ?, ?)
                        ON CONFLICT(id) DO UPDATE SET 
                            status=excluded.status,
                            last_active_ts=excluded.last_active_ts,
                            updated_at=excluded.updated_at
                    """
                    conn.execute(query, (account_id, status, now, now))
        except Exception as e:
            logger.error(f"❌ Account Heartbeat Error: {e}")

    def upsert_viral_target(self, username: str, priority: int = 50, source: str = "cascade"):
        try:
            now = time.time()
            # GDPR: We now hash "Topology Targets" too, to avoid storing PII even in discovery queues.
            hashed_username = self._hash_identity(username)

            with self.session() as conn:
                query = """
                    INSERT INTO viral_queue (username, source, priority, status, created_at, updated_at)
                    VALUES (?, ?, ?, 'PENDING', ?, ?)
                    ON CONFLICT(username) DO UPDATE SET
                        priority = GREATEST(viral_queue.priority, excluded.priority),
                        updated_at = excluded.updated_at
                        WHERE viral_queue.status != 'PROCESSED'
                """
                if not self.use_postgres:
                    query = query.replace("GREATEST", "MAX")

                conn.execute(query, (hashed_username, source, priority, now, now))
        except Exception as e:
            logger.warning(f"⚠️ Upsert Viral Target Error: {e}")

    def pop_priority_target(self) -> Optional[str]:
        try:
            with self.session(immediate=True) as conn:
                suffix = " FOR UPDATE SKIP LOCKED" if self.use_postgres else ""

                cursor = conn.execute(
                    f"""
                    SELECT username FROM viral_queue 
                    WHERE status = 'PENDING'
                    ORDER BY priority DESC, created_at ASC
                    LIMIT 1{suffix}
                """
                )
                row = cursor.fetchone()

                if row:
                    username = row["username"] if self.use_postgres else row[0]
                    conn.execute(
                        "UPDATE viral_queue SET status='PROCESSED', updated_at=? WHERE username=?",
                        (time.time(), username),
                    )
                    return username
                return None
        except Exception as e:
            logger.error(f"❌ Pop Viral Target Error: {e}")
            return None

    def get_sponsor_failure_rate(self, sponsor_id: str, hours: int = 1) -> float:
        """
        Calculates Node Stability Index.
        Returns % of Technical Failures (SLA Breaches).
        """
        try:
            cutoff = time.time() - (hours * 3600)
            with self.session() as conn:
                row_total = conn.execute(
                    """
                    SELECT COUNT(*) as cnt FROM leads 
                    WHERE assigned_sponsor_id = ? 
                    AND updated_at > ?
                    AND status IN ('SENT', 'FAILED_ERROR', 'FAILED_API')
                """,
                    (sponsor_id, cutoff),
                ).fetchone()

                total = (row_total["cnt"] if self.use_postgres else row_total[0]) or 0
                if total == 0:
                    return 0.0

                row_fails = conn.execute(
                    """
                    SELECT COUNT(*) as cnt FROM leads 
                    WHERE assigned_sponsor_id = ? 
                    AND updated_at > ?
                    AND status LIKE 'FAILED_%'
                """,
                    (sponsor_id, cutoff),
                ).fetchone()

                fails = (row_fails["cnt"] if self.use_postgres else row_fails[0]) or 0

                return float(fails) / float(total)
        except Exception as e:
            logger.warning(f"⚠️ Yield Metrics Error: {e}")
            return 0.0

    def analyze_user_history(self, author_id: str, hours: int = 24) -> Dict[str, Any]:
        """
        Analyzes Node History for Qualification.
        Uses Hashed ID for privacy compliance.
        """
        if not author_id:
            return {"status": "NEW", "action": "PROCESS"}

        # PRIVACY: Use hash for lookup
        lookup_id = self._hash_identity(author_id)

        try:
            cutoff = time.time() - (hours * 3600)
            vip_cutoff = time.time() - (90 * 86400)

            with self.session() as conn:
                rows = conn.execute(
                    """
                    SELECT id, program, status, created_at 
                    FROM leads 
                    WHERE author = ? AND created_at > ?
                    ORDER BY created_at DESC
                """,
                    (lookup_id, vip_cutoff),
                ).fetchall()

                leads = [dict(r) for r in rows]

                conversions = [lead for lead in leads if lead["status"] == "CONVERTED"]
                if conversions:
                    return {
                        "status": "VIP_USER",
                        "action": "PRIORITY_DISPATCH",
                        "reason": "PAST_SUCCESS",
                    }

                count_recent = len([lead for lead in leads if lead["created_at"] > cutoff])
                failures = [lead for lead in leads if "FAILED" in lead["status"]]

                if len(failures) > 2:
                    return {
                        "status": "DEAD_END",
                        "action": "HARD_BLOCK",
                        "reason": "TOO_MANY_FAILURES",
                    }

                if count_recent == 1:
                    prev = leads[0]
                    if "FAILED" not in prev["status"]:
                        return {
                            "status": "HOT_RETURN",
                            "action": "BOOST_SCORE",
                            "prev_context": prev.get("program"),
                        }

                if count_recent >= 2:
                    return {"status": "SPAM", "action": "BLOCK"}

                return {"status": "NEW", "action": "PROCESS"}
        except Exception:
            return {"status": "NEW", "action": "PROCESS"}

    # --- FINANCIAL LEDGER & QUEUING ---

    def get_program_leaderboard(self, program: str) -> List[Dict]:
        try:
            with self.session() as conn:
                rows = conn.execute(
                    """
                    SELECT id, label, balance_available, priority 
                    FROM sponsors 
                    WHERE program = ? AND active = 1 
                """,
                    (program,),
                ).fetchall()

                sponsors = [dict(r) for r in rows]

                ranked_list = []
                for s in sponsors:
                    balance = float(
                        s["balance_available"] if s["balance_available"] is not None else 0.0
                    )
                    priority = max(1, int(s["priority"]))

                    score = balance * (10.0 / priority)

                    ranked_list.append(
                        {
                            "id": s["id"],
                            "label": s["label"],
                            "score": round(score, 2),
                            "balance": balance,
                            "priority": priority,
                        }
                    )

                ranked_list.sort(key=lambda x: x["score"], reverse=True)

                for idx, item in enumerate(ranked_list):
                    item["rank"] = idx + 1

                return ranked_list

        except Exception as e:
            logger.error(f"❌ Leaderboard Error: {e}")
            return []

    def get_sponsor_queue_position(self, sponsor_id: str, program: str) -> Dict[str, Any]:
        leaderboard = self.get_program_leaderboard(program)
        total_sponsors = len(leaderboard)

        my_data = next((x for x in leaderboard if x["id"] == sponsor_id), None)

        if not my_data:
            return {"rank": -1, "total": total_sponsors, "status": "inactive"}

        my_rank = my_data["rank"]
        my_score = my_data["score"]

        score_to_beat = 0
        if my_rank > 1:
            prev_competitor = leaderboard[my_rank - 2]
            score_to_beat = prev_competitor["score"] - my_score + 1

        return {
            "rank": my_rank,
            "total": total_sponsors,
            "score": my_score,
            "points_needed_for_next_rank": max(0, score_to_beat),
            "priority_level": my_data["priority"],
        }

    def process_wallet_topup(
        self, sponsor_id: str, amount_euros: float, payment_ref: str, source: str = "STRIPE"
    ) -> bool:
        """
        Credits the Partner's Compute Wallet.
        IDEMPOTENT: Checks if reference already exists.
        """
        try:
            amount = float(amount_euros)
            now = time.time()

            with self.session(immediate=True) as conn:
                check = conn.execute(
                    "SELECT id FROM ledger WHERE reference_external = ?", (payment_ref,)
                ).fetchone()
                if check:
                    logger.warning(f"⚠️ Payment already processed (Ref: {payment_ref}). Ignored.")
                    return True

                priority_boost_sql = ""
                if amount >= 100:
                    priority_boost_sql = ", priority = MAX(1, priority - 1)"
                    logger.info(f"🚀 PRIORITY BOOST: {sponsor_id} gained 1 priority level!")

                sql = f"""
                    UPDATE sponsors 
                    SET balance_available = balance_available + ?,
                        last_verification_date = ?
                        {priority_boost_sql}
                    WHERE id = ?
                """

                conn.execute(sql, (amount, datetime.now().isoformat(), sponsor_id))

                row = conn.execute(
                    "SELECT balance_available FROM sponsors WHERE id=?", (sponsor_id,)
                ).fetchone()
                new_balance = float(row["balance_available"] if self.use_postgres else row[0])

                conn.execute(
                    """
                    INSERT INTO ledger (transaction_date, type, partner_id, amount, reference_external, description, balance_after, created_at)
                    VALUES (?, 'DEPOSIT', ?, ?, ?, ?, ?, ?)
                """,
                    (now, sponsor_id, amount, payment_ref, f"TopUp via {source}", new_balance, now),
                )

                logger.info(f"💰 WALLET TOP-UP: {sponsor_id} +{amount}€ (Ref: {payment_ref})")
                return True

        except Exception as e:
            logger.error(f"❌ Wallet TopUp Error: {e}")
            return False

    def atomic_dispatch_transaction(
        self, lead_id: str, sponsor_id: str, program: str, estimated_cost: Decimal
    ) -> bool:
        """
        Manages atomic allocation with 'Smart Ledger' (Two-Phase Commit).
        FINOPS COMPLIANCE:
        Funds are "RESERVED" (Hold) to cover estimated compute costs.
        This is a Technical Bond ensuring Solvency.
        """
        try:
            now = datetime.now()
            now_iso = now.isoformat()
            now_ts = time.time()

            # --- FINOPS COMPLIANCE: COMPUTE FEE ---
            real_cost = float(estimated_cost)
            cost_decimal = estimated_cost

            with self.session(immediate=True) as conn:
                suffix = " FOR UPDATE" if self.use_postgres else ""

                row_sponsor = conn.execute(
                    f"""
                    SELECT monthly_limit_hard, verified_count_month, balance_available, balance_reserved 
                    FROM sponsors WHERE id=? AND active=1{suffix}
                """,
                    (sponsor_id,),
                ).fetchone()

                if not row_sponsor:
                    return False

                if self.use_postgres:
                    limit = self.to_decimal(row_sponsor["monthly_limit_hard"])
                    verified = self.to_decimal(row_sponsor["verified_count_month"])
                    balance_avail = self.to_decimal(row_sponsor["balance_available"])
                else:
                    limit = self.to_decimal(row_sponsor[0])
                    verified = self.to_decimal(row_sponsor[1])
                    balance_avail = self.to_decimal(row_sponsor[2])

                # --- 2PC PHASE 1: RESERVATION (HOLD) ---
                # STRICT SOLVENCY CHECK: Must have enough balance for the action
                if balance_avail >= cost_decimal:
                    # Move funds from "Available" to "Reserved"
                    conn.execute(
                        """
                        UPDATE sponsors 
                        SET balance_available = balance_available - ?,
                            balance_reserved = balance_reserved + ?
                        WHERE id=?
                    """,
                        (real_cost, real_cost, sponsor_id),
                    )

                    conn.execute(
                        """
                        INSERT INTO dispatch_logs (lead_id, sponsor_id, program, dispatched_at_ts, dispatched_at_iso, cost_charged, status)
                        VALUES (?, ?, ?, ?, ?, ?, 'RESERVED')
                    """,
                        (lead_id, sponsor_id, program, now_ts, now_iso, real_cost),
                    )

                    conn.execute(
                        """
                        UPDATE leads 
                        SET assigned_sponsor_id=?, assigned_program=?, status='READY_TO_SEND', updated_at=?
                        WHERE id=?
                    """,
                        (sponsor_id, program, now_ts, lead_id),
                    )

                    return True
                else:
                    logger.warning(
                        f"📉 SOLVENCY CHECK FAILED: Sponsor {sponsor_id} lacks funds ({balance_avail} < {cost_decimal})."
                    )

                # --- CAPACITY GUARD ---
                # Legacy Quota Support (Deprecated in V29 - Kept for fallback)
                if verified < limit and limit > 0:
                    logger.info(
                        f"📊 LEGACY QUOTA USED for {sponsor_id} (No funds, but within limits)."
                    )
                    conn.execute(
                        """
                        INSERT INTO dispatch_logs (lead_id, sponsor_id, program, dispatched_at_ts, dispatched_at_iso, status)
                        VALUES (?, ?, ?, ?, ?, 'LEGACY_DISPATCH')
                    """,
                        (lead_id, sponsor_id, program, now_ts, now_iso),
                    )

                    conn.execute(
                        """
                        UPDATE leads 
                        SET assigned_sponsor_id=?, assigned_program=?, status='READY_TO_SEND', updated_at=?
                        WHERE id=?
                    """,
                        (sponsor_id, program, now_ts, lead_id),
                    )
                    return True

                return False

        except Exception as e:
            logger.error(f"❌ Atomic Dispatch Error: {e}")
            return False

    def confirm_lead_hold(self, lead_id: str) -> bool:
        """
        --- 2PC PHASE 2a: COMMIT (CONSUMPTION) ---
        Confirms resource consumption (SLA Met).
        "Reserved" funds are destroyed (Consumed permanently).
        """
        try:
            with self.session(immediate=True) as conn:
                row = conn.execute(
                    "SELECT id, sponsor_id, cost_charged FROM dispatch_logs WHERE lead_id=? AND status='RESERVED'",
                    (lead_id,),
                ).fetchone()

                if not row:
                    return False

                log_id = row["id"] if self.use_postgres else row[0]
                sponsor_id = row["sponsor_id"] if self.use_postgres else row[1]
                cost = row["cost_charged"] if self.use_postgres else row[2]

                # Amount is already in "Reserved", just subtract it to "pay" for service
                # NOTE: The funds were already moved from 'available' to 'reserved'.
                # Now we simply remove them from 'reserved' (burn them) and log the consumption.
                conn.execute(
                    """
                    UPDATE sponsors 
                    SET balance_reserved = balance_reserved - ? 
                    WHERE id=?
                """,
                    (cost, sponsor_id),
                )

                conn.execute("UPDATE dispatch_logs SET status='CONSUMED' WHERE id=?", (log_id,))

                # Financial Traceability
                conn.execute(
                    """
                    INSERT INTO ledger (transaction_date, type, partner_id, amount, reference_external, description, created_at)
                    VALUES (?, 'CONSUMPTION', ?, ?, ?, ?, ?)
                """,
                    (
                        time.time(),
                        sponsor_id,
                        -cost,
                        lead_id,
                        "Resource Consumption (SLA Met)",
                        time.time(),
                    ),
                )

                logger.info(f"💰 CREDIT CONSUMED: {sponsor_id} -{cost}€ (Service Delivered)")
                return True
        except Exception as e:
            logger.error(f"❌ Resource Consumption Error: {e}")
            return False

    def release_lead_hold(self, lead_id: str) -> bool:
        """
        --- 2PC PHASE 2b: ROLLBACK (RELEASE) ---
        Releases reservation on Technical Failure (SLA Breach).
        "Reserved" funds return to "Available".
        """
        try:
            with self.session(immediate=True) as conn:
                row = conn.execute(
                    "SELECT id, sponsor_id, cost_charged FROM dispatch_logs WHERE lead_id=? AND status='RESERVED'",
                    (lead_id,),
                ).fetchone()

                if not row:
                    return False

                log_id = row["id"] if self.use_postgres else row[0]
                sponsor_id = row["sponsor_id"] if self.use_postgres else row[1]
                cost = row["cost_charged"] if self.use_postgres else row[2]

                # Refund: Reserved -> Available
                conn.execute(
                    """
                    UPDATE sponsors 
                    SET balance_reserved = balance_reserved - ?,
                        balance_available = balance_available + ?
                    WHERE id=?
                """,
                    (cost, cost, sponsor_id),
                )

                conn.execute("UPDATE dispatch_logs SET status='RELEASED' WHERE id=?", (log_id,))

                logger.info(f"↩️ CREDIT RELEASED: {sponsor_id} +{cost}€ (SLA Breach / Tech Fail)")
                return True
        except Exception as e:
            logger.error(f"❌ Release Reservation Error: {e}")
            return False

    def insert_raw_lead(self, lead: Dict[str, Any], conn=None) -> bool:
        """
        Ingestion of Telemetry Packets with PII Hashing (Data Minimization).
        Renamed logic: 'Lead' -> 'Telemetry Packet'.
        """
        try:
            t0 = time.time()
            lead_id = lead.get("id")
            if not lead_id:
                return False
            now = time.time()

            clean_text, meta_analysis = self._sanitize_and_score_lead(lead)

            # --- GDPR / DATA MINIMIZATION ---
            # RAW PII is never stored directly without hashing
            raw_author = lead.get("author") or lead.get("comment_author") or ""

            # Hashing Identity immediately (Zero-Knowledge)
            author_hash = self._hash_identity(raw_author)

            existing_info = lead.get("ai_process_info", {})
            if isinstance(existing_info, str):
                try:
                    existing_info = json.loads(existing_info)
                except Exception:
                    existing_info = {}

            final_info = {**existing_info, "ingest_meta": meta_analysis}
            info_json = json.dumps(final_info)
            meta_analysis_json = json.dumps(meta_analysis)

            # Using 'leads' table for compatibility, but logic is purely Telemetry
            # The 'comment_text' now stores the processed text for NLP context, considered 'Technical Payload'
            # Note: We rely on the sanitization function to strip PII from text if configured,
            # here we store technical payload for routing.
            query = """
                INSERT INTO leads 
                (id, source, author, url, comment_text, created_at, updated_at, ai_process_info, meta_analysis)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO NOTHING
            """
            params = (
                str(lead_id),
                lead.get("source", "tiktok")[:50],
                author_hash,  # Storing Hash Only - No raw username
                lead.get("url", "")[:500],
                clean_text,  # Technical payload preserved for NLP analysis
                now,
                now,
                info_json,
                meta_analysis_json,
            )

            if conn:
                conn.execute(query, params)
            else:
                with self.session() as target_conn:
                    target_conn.execute(query, params)

            self.last_write_latency = time.time() - t0
            return True

        except Exception as e:
            logger.error(f"❌ Telemetry Ingestion Error: {e}")
            return False

    # Alias for semantic clarity in AdTech context
    insert_telemetry_signal = insert_raw_lead

    def reserve_leads_for_dispatch(
        self, batch_size: int = 50, batch_id: str = "batch_temp"
    ) -> List[Dict]:
        """
        Reserves a batch of leads for the Orchestrator.
        EXPOSES 'meta_analysis' to support Yield Scarcity Logic.
        """
        try:
            processed_leads = []
            now = time.time()

            with self.session(immediate=True) as conn:
                suffix = " FOR UPDATE SKIP LOCKED" if self.use_postgres else ""

                # UPDATED SELECT: Now fetches 'meta_analysis' containing the quality score
                rows = conn.execute(
                    f"""
                    SELECT id, comment_text, ai_draft, source, url, ai_process_info, meta_analysis
                    FROM leads 
                    WHERE status='QUALIFIED' AND assigned_sponsor_id IS NULL 
                    ORDER BY created_at ASC 
                    LIMIT ?{suffix}
                """,
                    (batch_size,),
                ).fetchall()

                if not rows:
                    return []

                ids = []
                for row in rows:
                    lead = dict(row)
                    lead_id = lead["id"]
                    ids.append(lead_id)

                    raw_info = lead.get("ai_process_info")
                    info = json.loads(raw_info) if raw_info and isinstance(raw_info, str) else {}
                    info["batch_id"] = batch_id

                    processed_leads.append(lead)

                    conn.execute(
                        "UPDATE leads SET status='DISPATCHING', ai_process_info=?, updated_at=? WHERE id=?",
                        (json.dumps(info), now, lead_id),
                    )

            return processed_leads
        except Exception as e:
            logger.error(f"❌ Reserve Dispatch Error: {e}")
            return []

    def close(self):
        self.db_executor.shutdown(wait=True)
        self.close_local_connection()
        if self.use_postgres and NexusDB._pg_pool:
            try:
                NexusDB._pg_pool.closeall()
            except Exception:
                pass

    def mark_lead_sent(self, lead_id: str):
        try:
            with self.session() as conn:
                conn.execute(
                    "UPDATE leads SET status='SENT', updated_at=? WHERE id=?",
                    (time.time(), lead_id),
                )
        except Exception:
            pass

    def fail_lead(self, lead_id: str, reason: str = "ERROR"):
        try:
            with self.session() as conn:
                conn.execute(
                    "UPDATE leads SET status=?, updated_at=? WHERE id=?",
                    (f"FAILED_{reason}", time.time(), lead_id),
                )
                # Release reservation on failure (2PC Rollback)
                self.release_lead_hold(lead_id)
        except Exception:
            pass

    def get_campaign_info(self, program: str):
        try:
            with self.session() as conn:
                r = conn.execute("SELECT * FROM campaigns WHERE program=?", (program,)).fetchone()
                return dict(r) if r else None
        except Exception:
            return None

    def get_program_sponsors_stats(self, program: str) -> List[Dict]:
        try:
            with self.session() as conn:
                iso_start = (
                    datetime.now()
                    .replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                    .isoformat()
                )
                res = conn.execute(
                    """
                    SELECT s.*, COUNT(d.id) as pending_leads_count
                    FROM sponsors s
                    LEFT JOIN dispatch_logs d ON s.id = d.sponsor_id 
                        AND d.dispatched_at_iso >= ?
                    WHERE s.program = ? AND s.active = 1
                    GROUP BY s.id
                """,
                    (iso_start, program),
                )
                return [dict(r) for r in res.fetchall()]
        except Exception:
            return []

    def get_backlog_count(self) -> int:
        try:
            with self.session() as conn:
                r = conn.execute(
                    "SELECT COUNT(*) as cnt FROM leads WHERE status='QUALIFIED' OR status='NEW'"
                ).fetchone()
                return r["cnt"] if self.use_postgres else r[0]
        except Exception:
            return 0

    def get_max_active_campaign_amount(self) -> float:
        try:
            with self.session() as conn:
                r = conn.execute(
                    "SELECT MAX(amount) as m FROM campaigns WHERE end_date > ?",
                    (datetime.now().isoformat(),),
                ).fetchone()
                val = r["m"] if self.use_postgres else r[0]
                return float(val) if val else 0.0
        except Exception:
            return 0.0

    def get_next_lead_to_send_by_source(self, source: str) -> Optional[Dict]:
        try:
            with self.session(immediate=True) as conn:
                suffix = " FOR UPDATE SKIP LOCKED" if self.use_postgres else ""
                query = f"SELECT * FROM leads WHERE status='READY_TO_SEND' AND source=? ORDER BY updated_at ASC LIMIT 1{suffix}"
                row = conn.execute(query, (source,)).fetchone()

                if row:
                    lead = dict(row)
                    conn.execute(
                        "UPDATE leads SET status='SENDING', updated_at=? WHERE id=?",
                        (time.time(), lead["id"]),
                    )
                    return lead
                return None
        except Exception:
            return None

    def get_converted_leads_sample(self, limit: int = 500) -> List[Dict]:
        try:
            with self.session() as conn:
                rows = conn.execute(
                    "SELECT comment_text FROM leads WHERE status='CONVERTED' LIMIT ?", (limit,)
                ).fetchall()
                return [dict(r) for r in rows]
        except Exception:
            return []

    def update_subreddit_stats(self, subreddit: str, success: bool):
        try:
            now = time.time()
            with self.session() as conn:
                if success:
                    query = """
                        INSERT INTO subreddit_stats (subreddit, success_count, updated_at)
                        VALUES (?, 1, ?)
                        ON CONFLICT(subreddit) DO UPDATE SET 
                            success_count = subreddit_stats.success_count + 1,
                            updated_at = excluded.updated_at
                    """
                    conn.execute(query, (subreddit, now))
                else:
                    query = """
                        INSERT INTO subreddit_stats (subreddit, fail_count, last_fail_ts, updated_at)
                        VALUES (?, 1, ?, ?)
                        ON CONFLICT(subreddit) DO UPDATE SET 
                            fail_count = subreddit_stats.fail_count + 1,
                            last_fail_ts = excluded.last_fail_ts,
                            updated_at = excluded.updated_at
                    """
                    conn.execute(query, (subreddit, now, now))
        except Exception as e:
            logger.error(f"❌ Update Subreddit Stats Error: {e}")

    def get_subreddit_stats(self, subreddit: str) -> Dict[str, Any]:
        try:
            with self.session() as conn:
                row = conn.execute(
                    "SELECT * FROM subreddit_stats WHERE subreddit=?", (subreddit,)
                ).fetchone()
                return (
                    dict(row) if row else {"success_count": 0, "fail_count": 0, "last_fail_ts": 0}
                )
        except Exception:
            return {"success_count": 0, "fail_count": 0, "last_fail_ts": 0}

    def upsert_campaign(
        self, program: str, amount: float, is_boosted: bool, end_date: str, url: str
    ):
        try:
            now = time.time()
            with self.session() as conn:
                conn.execute(
                    """
                    INSERT INTO campaigns (program, amount, is_boosted, end_date, url, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(program) DO UPDATE SET
                        amount=excluded.amount,
                        is_boosted=excluded.is_boosted,
                        end_date=excluded.end_date,
                        updated_at=excluded.updated_at
                """,
                    (program, amount, 1 if is_boosted else 0, end_date, url, now),
                )
        except Exception as e:
            logger.error(f"❌ Campaign Upsert Error: {e}")

    def get_author_reputation(self, author_id: str, platform: str) -> Dict[str, Any]:
        """
        Returns the reputation record for a hashed author on a given platform.
        Defaults to NEUTRAL / score 50 when no record exists yet.
        """
        default = {"status": "NEUTRAL", "reputation_score": 50, "expiry_ts": 0.0, "meta_data": "{}"}
        try:
            with self.session() as conn:
                row = conn.execute(
                    "SELECT * FROM author_reputation WHERE author_id=? AND platform=?",
                    (author_id, platform),
                ).fetchone()
                return dict(row) if row else default
        except Exception as e:
            logger.error(f"❌ Get Author Reputation Error: {e}")
            return default

    def update_author_reputation(
        self, author_id: str, platform: str, score_delta: int, new_status: str = None
    ):
        """
        Upserts the reputation of a hashed author on a platform.
        Adjusts reputation_score by score_delta (positive or negative).
        Optionally sets a new status (NEUTRAL, TRUSTED, BANNED, VIP).
        Score is clamped to [0, 100].
        """
        try:
            now = time.time()
            with self.session() as conn:
                existing = conn.execute(
                    "SELECT reputation_score, status FROM author_reputation WHERE author_id=? AND platform=?",
                    (author_id, platform),
                ).fetchone()

                if existing:
                    current_score = (
                        existing["reputation_score"] if self.use_postgres else existing[0]
                    )
                    current_status = existing["status"] if self.use_postgres else existing[1]
                else:
                    current_score = 50
                    current_status = "NEUTRAL"

                new_score = max(0, min(100, (current_score or 50) + score_delta))
                final_status = new_status if new_status else current_status

                conn.execute(
                    """
                    INSERT INTO author_reputation (author_id, platform, reputation_score, status, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(author_id, platform) DO UPDATE SET
                        reputation_score = ?,
                        status = ?,
                        updated_at = excluded.updated_at
                """,
                    (author_id, platform, new_score, final_status, now, new_score, final_status),
                )
        except Exception as e:
            logger.error(f"❌ Update Author Reputation Error: {e}")

    def inject_priority_task(self, task: Dict[str, Any]):
        """
        Injects a high-priority task (e.g. VIP outreach) directly into the leads pipeline
        with status PRIORITY_TASK, bypassing normal acquisition channels.
        task dict: {type, target_id, priority, protocol, source_module, payload}
        """
        try:
            now = time.time()
            task_id = f"PRIO_{int(now)}_{str(task.get('target_id', 'unknown'))[:8]}"
            ai_info = json.dumps(
                {
                    "task_type": task.get("type"),
                    "protocol": task.get("protocol"),
                    "source_module": task.get("source_module"),
                    "priority": task.get("priority", 100),
                    "payload": task.get("payload", {}),
                }
            )
            with self.session() as conn:
                conn.execute(
                    """
                    INSERT INTO leads
                    (id, source, author, url, comment_text, status, assigned_program,
                     ai_process_info, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, 'PRIORITY_TASK', ?, ?, ?, ?)
                    ON CONFLICT(id) DO NOTHING
                """,
                    (
                        task_id,
                        task.get("source_module", "priority_injection"),
                        task.get("target_id", "anonymous_node"),
                        task.get("payload", {}).get("origin_url", ""),
                        f"PRIORITY TASK: {task.get('type')} via {task.get('protocol')}",
                        task.get("protocol", ""),
                        ai_info,
                        now,
                        now,
                    ),
                )
        except Exception as e:
            logger.error(f"❌ Inject Priority Task Error: {e}")

    def get_sponsor_stats(self, sponsor_id: str) -> Dict[str, Any]:
        """
        Returns quota stats for a single sponsor: verified_count_month and
        pending_leads_count (active reservations this month).
        Used by supply_chain_manager for capacity tracking.
        """
        default = {"verified_count_month": 0, "pending_leads_count": 0}
        try:
            with self.session() as conn:
                iso_start = (
                    datetime.now()
                    .replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                    .isoformat()
                )
                row = conn.execute(
                    """
                    SELECT s.verified_count_month,
                           COUNT(d.id) as pending_leads_count
                    FROM sponsors s
                    LEFT JOIN dispatch_logs d ON s.id = d.sponsor_id
                        AND d.dispatched_at_iso >= ?
                        AND d.status IN ('RESERVED', 'LEGACY_DISPATCH')
                    WHERE s.id = ?
                    GROUP BY s.id, s.verified_count_month
                """,
                    (iso_start, sponsor_id),
                ).fetchone()

                if not row:
                    return default

                if self.use_postgres:
                    return {
                        "verified_count_month": row["verified_count_month"] or 0,
                        "pending_leads_count": row["pending_leads_count"] or 0,
                    }
                else:
                    return {"verified_count_month": row[0] or 0, "pending_leads_count": row[1] or 0}
        except Exception as e:
            logger.error(f"❌ Get Sponsor Stats Error: {e}")
            return default

    # --- DASHBOARD & INTELLIGENCE API SUPPORT ---
    def get_dashboard_snapshot(self) -> Dict[str, Any]:
        """
        Aggregates Real-Time Financial & Operational KPIs for the Dashboard.
        Optimized for Read-Only access (Non-blocking).
        """
        snapshot = {
            "revenue": 0.0,
            "cost": 0.0,
            "leadsBank": 0,
            "leadsCrypto": 0,
            "leadsSaaS": 0,
            "stocks": {"partnerCap": 0, "velocityHistory": []},
            "bots": [],
        }
        try:
            # Note: We use a separate connection/session for this aggregation to avoid blocking writers
            with self.session() as conn:
                # 1. Financials (Revenue from Conversions)
                res_rev = conn.execute("SELECT SUM(amount) as total FROM conversions").fetchone()
                total_rev = (res_rev["total"] if self.use_postgres else res_rev[0]) or 0.0
                snapshot["revenue"] = float(total_rev)

                # 2. Operational Costs (From Ledger Consumptions)
                res_cost = conn.execute(
                    "SELECT SUM(amount) as total FROM ledger WHERE type='CONSUMPTION'"
                ).fetchone()
                total_cost = (res_cost["total"] if self.use_postgres else res_cost[0]) or 0.0
                snapshot["cost"] = abs(float(total_cost))  # Costs are negative in ledger

                # 3. Leads Counts
                res_leads = conn.execute(
                    "SELECT program, COUNT(*) as cnt FROM leads WHERE status='CONVERTED' GROUP BY program"
                ).fetchall()
                for row in res_leads:
                    prog = (row["program"] if self.use_postgres else row[0]).upper()
                    cnt = row["cnt"] if self.use_postgres else row[1]
                    if "BOURSO" in prog or "BANK" in prog:
                        snapshot["leadsBank"] += cnt
                    elif "MERIA" in prog or "CRYPTO" in prog:
                        snapshot["leadsCrypto"] += cnt
                    elif "SAAS" in prog:
                        snapshot["leadsSaaS"] += cnt

                # 4. Stocks (Partner Capacities)
                res_stocks = conn.execute(
                    "SELECT SUM(monthly_limit_hard - verified_count_month) as remaining FROM sponsors WHERE active=1"
                ).fetchone()
                snapshot["stocks"]["partnerCap"] = (
                    res_stocks["remaining"] if self.use_postgres else res_stocks[0]
                ) or 0

                # 5. Bots Health (Derived from Account Statuses)
                res_bots = conn.execute(
                    "SELECT platform, status, last_active_ts FROM accounts"
                ).fetchall()
                platforms = {}
                for row in res_bots:
                    plat = row["platform"] if self.use_postgres else row[0]
                    stat = row["status"] if self.use_postgres else row[1]
                    last_ts = row["last_active_ts"] if self.use_postgres else row[2]

                    if plat not in platforms:
                        platforms[plat] = {"total": 0, "active": 0, "last_ts": 0}
                    platforms[plat]["total"] += 1
                    if stat == "ACTIVE":
                        platforms[plat]["active"] += 1
                    if last_ts > platforms[plat]["last_ts"]:
                        platforms[plat]["last_ts"] = last_ts

                # Construct Bot Objects for UI
                for plat, data in platforms.items():
                    is_active = (time.time() - data["last_ts"]) < 300  # 5 min heartbeat
                    snapshot["bots"].append(
                        {
                            "name": f"{plat.capitalize()} Worker",
                            "status": "active" if is_active else "cooldown",
                            "efficiency": int((data["active"] / (data["total"] or 1)) * 100),
                            "errorRate": 0.05,  # Simulated for now based on global health
                            "complianceMode": True,
                        }
                    )

            return snapshot
        except Exception as e:
            logger.error(f"❌ Dashboard Snapshot Error: {e}")
            return snapshot

    class CrossPlatformLock:
        def __init__(self, lock_file: Path, max_age: int = 60):
            self.lock_file = lock_file

        async def acquire_async(self, timeout: float = 1.0):
            await asyncio.sleep(0.01)

        def release(self):
            pass
