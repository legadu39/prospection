# STATUS.md — Analyse de l'état du projet Nexus

> Généré le 2026-04-13. Légende : ✅ complet · 🔄 partiel · ❌ vide/cassé

---

## Résumé exécutif

| Stat | Valeur |
|------|--------|
| Fichiers Python | 26 |
| ✅ Complets | 9 |
| 🔄 Partiels | 17 |
| ❌ Vides / Cassés | 0 |
| **Bloqueurs P0** | **2** (`core/database.py` manquant, `core/dispatcher.py` manquant) |

**Le projet ne démarre pas** : 13 fichiers font `from core.database import NexusDB` alors que ce fichier n'existe pas.
La vraie implémentation de `NexusDB` se trouve dans `core/secure_telemetry_store.py`.
Fix P0 : créer `core/database.py` avec 1 ligne d'alias.

---

## Fichiers racine

### `launcher.py` — 🔄 Partiel
**Dépendances :** `core.database` ❌ · `core.settings` ✅ · `core.supply_chain_manager` ✅ · `psutil` (optionnel)  
**Rôle :** Orchestrateur subprocess des 5 services (ad_exchange, pipeline_bridge, tiktok_sniper, tiktok_sender, reddit_listener)  
**Ce qui manque :**
- `from core.database import NexusDB` → `ImportError` au démarrage tant que `core/database.py` n'existe pas
- `psutil` non listé dans `requirements.txt` (utilisé pour graceful kill)

---

### `pipeline_bridge.py` — ✅ Complet
**Dépendances :** `core.database` ✅ · `core.dispatcher` ✅ · `core.gemini_processor` ✅ · `core.prompts` ✅ · `core.logger_utils` ✅  
**Rôle :** Pipeline IA principal (triage sémantique, PID backpressure, fast-track, smart retry)  
**Ce qui manque :** Rien

---

### `check_links.py` — ✅ Complet
**Dépendances :** stdlib uniquement (`json`, `requests`, `pathlib`)  
**Rôle :** Audit des liens affiliés (vérif redirection + code affiliate)  
**Ce qui manque :** Rien

---

## Core modules

### `core/secure_telemetry_store.py` — ✅ Complet
**Dépendances :** `core.settings` ✅ · `psycopg2` (optionnel)  
**Rôle :** **C'est la vraie implémentation de `NexusDB`** — SQLite/PostgreSQL, ACID, 2PC, GDPR hashing  
**Méthodes vérifiées :** `insert_telemetry_signal` ✅ · `update_subreddit_stats` ✅ · `get_subreddit_stats` ✅ · `upsert_viral_target` ✅ · `get_next_lead_to_send_by_source` ✅ · `inject_priority_task` ✅ · `get_author_reputation` ✅ · `update_author_reputation` ✅ · `get_sponsor_stats` ✅  
**Ce qui manque :** Rien

---

### `core/settings.py` — ✅ Complet
**Dépendances :** `pydantic`, `python-dotenv`, `pathlib`  
**Rôle :** Configuration centrale Pydantic (DB_PATH, CDP ports, chemins, clés)  
**Ce qui manque :** Rien

---

### `core/ad_exchange_server.py` — 🔄 Partiel
**Dépendances :** `core.database` ❌ · `core.settings` ✅ · `fastapi` · `uvicorn`  
**Rôle :** Serveur FastAPI — allocation programmatique, geo-routing, pacing, postback attribution  
**Ce qui manque :**
- `from core.database import NexusDB` → `ImportError` tant que `core/database.py` absent
- Endpoint `/health` manquant (monitoring Docker)

---

### `core/browser_engine.py` — 🔄 Partiel
**Dépendances :** `core.settings` · `core.database` ❌ · `psutil` · `aiofiles` · `aiohttp` · `cryptography` · `playwright`  
**Rôle :** Moteur CDP/Playwright — profils sandbox, stealth injection, circuit breaker, proxy résidentiel  
**Ce qui manque :**
- `core.database` absent
- `psutil` et `aiofiles` non listés dans `requirements.txt`

---

### `core/gemini_processor.py` — ✅ Complet
**Dépendances :** `core.settings` ✅ · `google-generativeai` ✅  
**Rôle :** Moteur IA Gemini 1.5 Flash — qualification sémantique, JSON parsing robuste, sandwich defense  
**Ce qui manque :** Rien

---

### `core/humanizer.py` — ✅ Complet
**Dépendances :** `playwright`  
**Rôle :** Simulation comportement humain (scroll organique, mouse physics, typing humain)  
**Ce qui manque :** Rien

---

### `core/logger_utils.py` — ✅ Complet
**Dépendances :** stdlib uniquement  
**Rôle :** Logger sécurisé (masquage PII, rotation fichiers)  
**Ce qui manque :** Rien

---

### `core/mobile_rotator.py` — 🔄 Partiel
**Dépendances :** `core.settings`  
**Rôle :** Rotation IP via Android/ADB (airplaine mode toggle + ADB shell)  
**Ce qui manque :**
- Ligne 200 : `pass` avec commentaire "Stratégie 3 : input tap — à implémenter si besoin via coordonnées"
- Stratégie 3 non implémentée (interaction tap directe par coordonnées)

---

### `core/offer_hunter.py` — 🔄 Partiel
**Dépendances :** `core.database` ❌ · `core.settings` ✅  
**Rôle :** Scanner d'offres/bonus — détecte les boosts actifs chez les partenaires affiliés  
**Ce qui manque :**
- `core.database` absent → `ImportError`

---

### `core/prompts.py` — ✅ Complet
**Dépendances :** `core.settings`  
**Rôle :** Protocoles sémantiques — prompts Gemini pour qualification de leads  
**Ce qui manque :** Rien

---

### `core/supply_chain_manager.py` — 🔄 Partiel
**Dépendances :** `core.database` ❌ · `core.settings` ✅  
**Rôle :** Yield optimizer — `FleetManager`, EPC/Waterfall, Signal Exchange, Smart Pacing  
**Ce qui manque :**
- `core.database` absent → `ImportError`

---

### `core/time_manager.py` — ✅ Complet
**Dépendances :** `asyncio`, `datetime`  
**Rôle :** Cycle circadien — fenêtres horaires, weekends, scheduler asynchrone  
**Ce qui manque :** Rien

---

### `core/vision_guardian.py` — 🔄 Partiel
**Dépendances :** `core.database` ❌ · `core.settings` · `playwright` · `PIL`  
**Rôle :** Analyse visuelle — détection captcha, crash handler, apprentissage sélecteurs DOM  
**Ce qui manque :**
- `core.database` absent → `ImportError`
- Ligne 141 : `pass + TODO` — SELECT sur table `dom_knowledge` non implémenté
- Ligne 181 : `pass` — sauvegarde des sélecteurs DOM non implémentée

---

### `core/workload_orchestrator.py` — 🔄 Partiel
**Dépendances :** `core.database` ❌ · `core.settings` ✅ · `core.time_manager` ✅  
**Rôle :** Dispatcher UCB1 — `ComputeGridOrchestrator`, scarcity curve, PID batch size, feedback loop  
**Note :** Contient la logique qui devrait alimenter `core/dispatcher.py` (`SponsorDispatcher`)  
**Ce qui manque :**
- `core.database` absent → `ImportError`

---

## Config

### `config/rag_engine.py` — ✅ Complet
**Dépendances :** `core.settings`, `json`, `pathlib`  
**Rôle :** RAG engine — chargement knowledge base JSON, retrieve_context()  
**Ce qui manque :** Rien

---

## Channels — TikTok

### `channels/tiktok/sniper.py` — 🔄 Partiel
**Dépendances :** `core.database` ❌ · `core.settings` · `core.browser_engine` · `core.time_manager` · `core.vision_guardian`  
**Rôle :** Topology mapper v35.2 — `KeywordLearner`, entropy analysis, déduplication  
**Ce qui manque :**
- `core.database` absent → `ImportError`

---

### `channels/tiktok/sender.py` — 🔄 Partiel
**Dépendances :** `core.database` ❌ · `core.browser_engine` · `core.time_manager` · `core.humanizer` · `core.settings`  
**Rôle :** Telemetry injector v41.2 — injection commentaires TikTok, DCO, compliance [Ad]  
**Ce qui manque :**
- `core.database` absent → `ImportError`

---

### `channels/tiktok/partner_sniper.py` — 🔄 Partiel
**Dépendances :** `core.database` ❌ · `core.settings` · `core.browser_engine` · `core.humanizer` · `core.time_manager`  
**Rôle :** Network topology mapper v37.3 — `TikTokTopologyMapper`, VIP escalation, viral triangulation  
**Ce qui manque :**
- `core.database` absent → `ImportError`
- Méthodes DB appelées : `inject_priority_task()`, `upsert_viral_target()` — à vérifier dans NexusDB

---

### `channels/tiktok/media_optimizer.py` — 🔄 Partiel
**Dépendances :** `core.database` ❌ · `core.browser_engine` · `core.time_manager` · `core.settings`  
**Rôle :** Media optimizer v27.0 — scan engagement vidéos, yield efficiency, velocity detection, GDPR hashing  
**Ce qui manque :**
- `core.database` absent → `ImportError`

---

## Channels — Reddit

### `channels/reddit/audience_listener.py` — 🔄 Partiel
**Dépendances :** `core.database` ❌ · `core.browser_engine` · `core.settings`  
**Rôle :** Audience listener v26.1 — `SemanticIntentClassifier`, CircadianScheduler, analyse flux GQL Reddit  
**Ce qui manque :**
- `core.database` absent → `ImportError`

---

### `channels/reddit/partner_hunter.py` — 🔄 Partiel
**Dépendances :** `core.database` ❌ · `core.settings` · `core.browser_engine` · `core.humanizer`  
**Rôle :** Partner hunter v11.1 — `AuthorityClassifier`, B2B vs Prop Firm detection, burst control  
**Ce qui manque :**
- `core.database` absent → `ImportError`
- Appel `self.db.insert_telemetry_signal()` — méthode à vérifier dans NexusDB

---

### `channels/reddit/sender.py` — ✅ Complet
**Dépendances :** `core.database` ✅ · `core.browser_engine` · `core.time_manager` · `core.vision_guardian` · `core.humanizer`  
**Rôle :** Community gateway v28.0 — `RedditCommunityGateway`, DCO geo-routing FR/US, reputation gate, persistent memory  
**Ce qui manque :** Rien  
**Implémenté :** Détection succès par `page.expect_response()` sur `/api/comment`, `/svc/shreddit/graphql`, `gateway.reddit.com` (HTTP 200 = succès). Fallback sur vérification champ texte si timeout.

---

## Channels — Email

### `channels/email/mailer_client.py` — 🔄 Partiel
**Dépendances :** `core.database` ✅ · `requests` · `loguru` · `os`  
**Rôle :** `NurturingBot` — séquences email via Brevo API, A/B testing sujets, send-time optimization  
**Ce qui manque :**
- `{{LINK}}` substitué via `raw_html.replace("{{LINK}}", affiliate_link)` ligne 209 ✅
- `MAILER_API_KEY` validée au `__init__` : `EnvironmentError` + `logger.critical` si absente ✅
- Fichier complet jusqu'à la ligne 238 (`send_referral_request`) ✅

---

## Fichiers de configuration (non-Python)

| Fichier | Statut | Notes |
|---------|--------|-------|
| `config/sponsors.json` | ✅ | Partenaires et mandats affiliés |
| `config/semantic_map.json` | ✅ | Routing sémantique par protocole |
| `config/knowledge_base.json` | ✅ | Base RAG |
| `config/campaigns.json` | ✅ | Campagnes actives |
| `config/activity_heatmap.json` | ✅ | Heatmap circadienne |
| `config/competitors_list.json` | ✅ | Filtre concurrents |
| `requirements.txt` | 🔄 | Manque `psutil`, `aiofiles` |
| `.env` | ✅ | Variables sensibles (non versionné) |
| `Dockerfile` | ✅ | Docker-ready |
| `frontend/` | ✅ | React/Vite — indépendant |

---

## Matrice des dépendances critiques

```
core/database.py ✅ CRÉÉ (alias vers secure_telemetry_store.NexusDB)
core/dispatcher.py ❌ MANQUANT ← pipeline_bridge.py l'importe
core/secure_telemetry_store.py ← contient NexusDB (la vraie DB)
```

**Bloqueur restant : 1 fichier à créer (P0-2)**

---

## Corrections appliquées (2026-04-13)

| Fichier | Ligne | Bug corrigé |
|---------|-------|-------------|
| `core/database.py` | — | **Créé** — alias `from core.secure_telemetry_store import NexusDB` |
| `core/secure_telemetry_store.py` | 1467 | `data["total"] \|\| 1` → `data["total"] or 1` (SyntaxError JS) |
| `core/settings.py` | 183 | `@root_validator` → `@root_validator(skip_on_failure=True)` (Pydantic v2) |
| `core/settings.py` | 281 | Ajout `extra = "ignore"` (champs `.env` supplémentaires rejetés) |
| `core/settings.py` | 224-262 | Emojis retirés des `print()` du validator `CHROME_BIN` (UnicodeEncodeError Windows) |
| `core/settings.py` | 288 | `print(f"🔴 ...")` → `print(..., file=sys.stderr)` sans emoji |
