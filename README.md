# Nexus — Affiliate Lead Acquisition Platform

Nexus is a fully automated **affiliate lead acquisition and routing platform** built for AdTech and yield management. It scouts TikTok and Reddit for high-intent trading/crypto leads, qualifies them through a Gemini AI pipeline, and routes each lead to the highest-yielding affiliate partner (prop firms, crypto wallets, SaaS tools) based on real-time CPA data and geo-compliance rules.

All user identities are hashed on ingestion (SHA-256 + rotating salt). Every outbound message carries a legal disclaimer. No PII is ever stored in plain text.

---

[![CI](https://github.com/legadu39/prospection/actions/workflows/ci.yml/badge.svg)](https://github.com/legadu39/prospection/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/badge/coverage-80%25-brightgreen)](https://github.com/legadu39/prospection/actions)
[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-lightgrey)](LICENSE)

---

## Table of Contents

1. [Architecture](#architecture)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Environment Variables](#environment-variables)
5. [Running the Platform](#running-the-platform)
6. [Project Structure](#project-structure)
7. [Running Tests](#running-tests)
8. [Docker](#docker)
9. [License](#license)

---

## Architecture

```
Channels (acquisition)
  ├── TikTok sniper / topology mapper / media optimizer
  └── Reddit audience listener / partner hunter
          │
          ▼  insert_raw_lead()
    NexusDB  (SQLite dev · PostgreSQL prod)
          │
          ▼  fetch_and_claim_leads()
    pipeline_bridge.py  ──►  Gemini 1.5 Flash (semantic qualification)
          │
          ▼  run_dispatch_cycle()
    core/dispatcher.py  ──►  ComputeGridOrchestrator (UCB1 yield optimizer)
          │
          ▼  get_next_lead_to_send_by_source()
    Senders
  ├── channels/tiktok/sender.py      (TikTok comment injection)
  ├── channels/reddit/sender.py      (Reddit reply gateway)
  └── channels/email/mailer_client.py (Brevo nurturing sequences)
          │
          ▼  postback /click /postback
    core/ad_exchange_server.py       (FastAPI · CPA attribution)
```

**Geo-routing rules (non-negotiable)**

| Zone | Allowed partners | Forbidden |
|------|-----------------|-----------|
| 🇺🇸 US / Global | Prop firms (APEX, FTMO, Topstep) | — |
| 🇫🇷 FR / EU | SaaS (TradingView), PSAN crypto (Ledger, Meria) | Prop firms |
| Everywhere | — | Banking, credit, loans |

---

## Prerequisites

| Tool | Minimum version | Notes |
|------|----------------|-------|
| Python | **3.11** | 3.12 also supported |
| Node.js | 18 LTS | Frontend build only |
| Google Chrome | stable | Managed by Playwright |
| PostgreSQL | 14+ | Production only — SQLite used in dev/test |
| Android device + ADB | any | Optional, for 4G IP rotation |

---

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/legadu39/prospection.git
cd prospection
```

### 2. Create a virtual environment

```bash
python -m venv .venv
# Linux / macOS
source .venv/bin/activate
# Windows
.venv\Scripts\activate
```

### 3. Install Python dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 4. Install Playwright browsers

```bash
playwright install chromium
playwright install-deps chromium
```

### 5. Set up environment variables

```bash
cp .env.example .env   # then edit .env with your values
```

See the [Environment Variables](#environment-variables) section for all required keys.

### 6. (Optional) Install pre-commit hooks

```bash
pip install pre-commit ruff black
pre-commit install
```

### 7. (Optional) Build the frontend

```bash
cd frontend
npm ci
npm run build
cd ..
```

---

## Environment Variables

Copy `.env.example` to `.env` and fill in every **required** field. Optional fields fall back to safe defaults.

### Security & Identity

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SECURITY_MASTER_KEY` | **yes** | — | Master key for GDPR identity hashing (SHA-256 salt). Use a long random string. |
| `POSTBACK_SECRET_TOKEN` | no | `POSTBACK_DEFAULT_DEV_TOKEN` | HMAC token for validating postback callbacks from ad networks. |

### Database

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `USE_POSTGRES` | no | `true` | Set to `false` to use SQLite (dev/test). |
| `POSTGRES_HOST` | prod | `localhost` | PostgreSQL hostname. |
| `POSTGRES_PORT` | no | `5432` | PostgreSQL port. |
| `POSTGRES_USER` | no | `postgres` | PostgreSQL user. |
| `POSTGRES_PASSWORD` | prod | `postgres` | PostgreSQL password. **Always override in production.** |
| `POSTGRES_DB` | no | `apex_db` | Database name. |
| `DB_PATH` | no | `tiktok_guerilla.db` | SQLite file path (only used when `USE_POSTGRES=false`). |

### AI & APIs

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GEMINI_API_KEY` | **yes** | — | Google Gemini 1.5 Flash API key (lead qualification). |
| `MAILER_API_KEY` | email channel | — | Brevo (formerly Sendinblue) API key for email sequences. |
| `CAPTCHA_API_KEY` | no | — | Anti-captcha service key (2captcha / CapMonster). |
| `GEOIP_API_KEY` | no | — | GeoIP provider key (ipstack or MaxMind) for geo-routing. |
| `GEOIP_PROVIDER` | no | `ipstack` | GeoIP provider: `ipstack` or `maxmind`. |

### Browser Automation

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `CHROME_BIN` | no | auto-detected | Path to Google Chrome binary. Auto-detected in Docker. |
| `CDP_TIKTOK_PORT` | no | `9222` | Chrome DevTools Protocol port for TikTok bots. |
| `CDP_REDDIT_PORT` | no | `9223` | Chrome DevTools Protocol port for Reddit bots. |
| `CDP_REDDIT_FAILOVER` | no | `9224` | Failover CDP port for Reddit. |
| `HEADLESS_MODE` | no | `true` in Docker | Set to `false` for local visual debugging. |
| `ROTATING_PROXY_URL` | no | — | Residential proxy URL (`http://user:pass@host:port`). |

### Supply Chain

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `ACCOUNTS_PROVIDER_API_URL` | no | — | URL of the account provisioning API. |
| `ACCOUNTS_PROVIDER_API_KEY` | no | — | API key for the account provisioning service. |
| `BREVO_TIMEOUT` | no | `10` | HTTP timeout (seconds) for Brevo API requests. |

---

## Running the Platform

### Development (direct Python)

```bash
# All 5 services in parallel (ad_exchange_server, pipeline_bridge,
# tiktok_sniper, tiktok_sender, reddit_listener)
python launcher.py

# Or start individual services
python core/ad_exchange_server.py          # FastAPI on port 8000
python pipeline_bridge.py                 # AI qualification loop
python channels/tiktok/sniper.py          # TikTok scan
python channels/reddit/audience_listener.py  # Reddit scan
```

The ad-exchange API exposes:
- `GET /health` — liveness check (`{"status":"ok","db":"connected","sponsors_loaded":N}`)
- `GET /serve` — lead routing endpoint
- `POST /postback` — CPA conversion callback

### Production checklist

```bash
# 1. Switch to PostgreSQL
echo "USE_POSTGRES=true" >> .env

# 2. Set a strong master key
echo "SECURITY_MASTER_KEY=$(openssl rand -hex 32)" >> .env

# 3. Set a real postback token
echo "POSTBACK_SECRET_TOKEN=$(openssl rand -hex 32)" >> .env

# 4. Launch
python launcher.py
```

---

## Project Structure

```
prospection/
│
├── launcher.py                   # Service orchestrator — starts all 5 sub-processes
├── pipeline_bridge.py            # Async AI pipeline (fetch → qualify → dispatch)
├── check_links.py                # Affiliate link audit utility
│
├── core/                         # Shared backend modules
│   ├── database.py               # 1-line alias → secure_telemetry_store.NexusDB
│   ├── secure_telemetry_store.py # NexusDB: SQLite/PostgreSQL, ACID, 2PC, GDPR hashing
│   ├── settings.py               # Pydantic v2 BaseSettings — single config source
│   ├── ad_exchange_server.py     # FastAPI: geo-routing, pacing, CPA attribution
│   ├── dispatcher.py             # SponsorDispatcher wrapping ComputeGridOrchestrator
│   ├── workload_orchestrator.py  # UCB1 yield optimizer, PID controller, scarcity curve
│   ├── browser_engine.py         # CDP/Playwright sandbox profiles, stealth injection
│   ├── humanizer.py              # Human behaviour simulation (physics mouse, typing)
│   ├── gemini_processor.py       # Gemini 1.5 Flash: semantic lead qualification
│   ├── vision_guardian.py        # DOM healing, captcha detection, selector learning
│   ├── mobile_rotator.py         # 4G IP rotation via ADB airplane-mode toggle
│   ├── offer_hunter.py           # Real-time bonus/offer scanner for partner programs
│   ├── supply_chain_manager.py   # Fleet manager, EPC waterfall, smart pacing
│   ├── time_manager.py           # Circadian scheduler — active windows, sleep gates
│   ├── prompts.py                # Gemini prompt library for lead qualification
│   └── logger_utils.py           # Secure logger: PII masking, file rotation
│
├── channels/                     # Acquisition & sending bots
│   ├── tiktok/
│   │   ├── sniper.py             # Keyword-driven video scanner
│   │   ├── partner_sniper.py     # Network topology mapper — discovers super-editors
│   │   ├── media_optimizer.py    # Engagement scanner, velocity detection
│   │   └── sender.py             # Comment injection with DCO and [Ad] compliance
│   ├── reddit/
│   │   ├── audience_listener.py  # GQL stream listener, intent classifier
│   │   ├── partner_hunter.py     # Post-level lead discovery, B2B vs prop-firm split
│   │   └── sender.py             # Reply gateway with network-based success detection
│   └── email/
│       └── mailer_client.py      # Brevo nurturing sequences, A/B subject testing
│
├── config/                       # Runtime configuration files
│   ├── sponsors.json             # Partner mandates, CPA amounts, yield tiers
│   ├── semantic_map.json         # Keyword→partner routing rules
│   ├── campaigns.json            # Active bonus campaigns
│   ├── knowledge_base.json       # RAG knowledge base
│   ├── activity_heatmap.json     # Circadian activity weights
│   ├── competitors_list.json     # Competitor filter
│   └── rag_engine.py             # RAG engine — context retrieval
│
├── tests/
│   ├── conftest.py               # Shared fixtures: SQLite :memory: DB, async HTTP client
│   ├── unit/
│   │   └── test_nexusdb_smoke.py # 15 tests covering NexusDB CRUD, reputation, dispatch
│   └── integration/              # (planned) full pipeline flow tests
│
├── frontend/                     # React + Vite dashboard (built to static_site/)
│
├── .github/
│   └── workflows/
│       ├── ci.yml                # lint → test → security on push/PR
│       └── release.yml           # changelog + GitHub Release on v* tags
│
├── Dockerfile                    # Multi-stage: Node frontend build + Python 3.11-slim
├── pytest.ini                    # asyncio_mode=auto, cov=core+channels, min 80%
├── .pre-commit-config.yaml       # black, ruff, pytest unit, API key detection
├── requirements.txt
└── .env                          # ← never committed (see .gitignore)
```

---

## Running Tests

```bash
# Unit tests only (fast, no browser, SQLite :memory:)
pytest tests/unit -v

# With coverage report
pytest tests/unit --cov=core --cov=channels --cov-report=term-missing

# All tests (unit + integration)
pytest

# Single test file
pytest tests/unit/test_nexusdb_smoke.py -v
```

**Testing conventions:**

- `NexusDB` is always tested with `NexusDB(db_path=Path(":memory:"))` — never mocked
- Playwright tests use a dedicated `test_profile` CDP profile — production profiles are never touched
- Integration tests must exercise the full flow: `NEW → QUALIFIED → DISPATCHING`

Coverage gate: **80 %** minimum (enforced in CI and locally via `pytest.ini`).

---

## Docker

```bash
# Build
docker build -t nexus:latest .

# Run (provide your .env file)
docker run --env-file .env -p 8000:8000 nexus:latest

# Health check
curl http://localhost:8000/health
```

The image uses a non-root `nexus` user, `tini` as PID 1, and installs Playwright/Chromium in a dedicated layer for cache efficiency.

---

## License

MIT — see [LICENSE](LICENSE).

> **Legal notice:** All outbound messages generated by this platform include a mandatory disclaimer:
> EN: *(Ad. Not financial advice. Trading involves risk.)*
> FR: *(Publicité. Ce site ne fournit pas de conseil en investissement financier. Les crypto-actifs sont risqués.)*
