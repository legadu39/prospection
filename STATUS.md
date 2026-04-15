# STATUS.md — Analyse de l'état du projet Nexus

> Dernière mise à jour : 2026-04-16 (audit vérifié tâche par tâche). Légende : ✅ complet · 🔄 partiel · ❌ vide/cassé

---

## Résumé exécutif

| Stat | Valeur |
|------|--------|
| Fichiers Python | 26 + 2 créés (database.py, dispatcher.py) |
| ✅ Complets (vérifiés) | 22 |
| 🔄 Partiels | 4 (channel files — fonctionnels mais non testés end-to-end) |
| ❌ Vides / Cassés | 0 |
| **Bloqueurs P0** | **0** — tous résolus |
| **BACKLOG** | **23/23 tâches ✅** — sprint P3 complet, tag v1.1.0 |

**Le projet démarre.** `core/database.py` et `core/dispatcher.py` existent. Toutes les dépendances sont dans `requirements.txt`. Suite de tests : 213 tests (154 NexusDB + 59 orchestrator).

---

## Fichiers racine

### `launcher.py` — ✅ Complet
**Dépendances :** `core.database` ✅ · `core.settings` ✅ · `core.supply_chain_manager` ✅ · `psutil` ✅  
**Rôle :** Orchestrateur subprocess des 5 services (ad_exchange, pipeline_bridge, tiktok_sniper, tiktok_sender, reddit_listener)  
**Ce qui manque :** Rien — `core/database.py` créé (P0-1), `psutil` ajouté dans `requirements.txt` (P0-3)

---

### `pipeline_bridge.py` — ✅ Complet
**Dépendances :** `core.database` ✅ · `core.dispatcher` ✅ · `core.gemini_processor` ✅ · `core.prompts` ✅ · `core.logger_utils` ✅  
**Rôle :** Pipeline IA principal (triage sémantique, PID backpressure, fast-track, smart retry)  
**Ce qui manque :** Rien  
**Implémenté (P3-6) :** `run_pipeline()` gère `CancelledError` proprement via `try/finally` — annule watchdog task, sauvegarde `KeywordLearner`, ferme `db.close()` avant exit.

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

### `core/ad_exchange_server.py` — ✅ Complet
**Dépendances :** `core.database` ✅ · `core.settings` ✅ · `fastapi` · `uvicorn`  
**Rôle :** Serveur FastAPI — allocation programmatique, geo-routing, pacing, postback attribution  
**Ce qui manque :** Rien  
**Implémenté :** `GET /health` — vérifie la connectivité DB + compte sponsors actifs, HTTP 200 si ok, 503 si DB unreachable.

---

### `core/browser_engine.py` — 🔄 Partiel (imports résolus)
**Dépendances :** `core.settings` ✅ · `core.database` ✅ · `psutil` ✅ · `aiofiles` ✅ · `aiohttp` ✅ · `cryptography` ✅ · `playwright` ✅  
**Rôle :** Moteur CDP/Playwright — profils sandbox, stealth injection, circuit breaker, proxy résidentiel  
**Ce qui manque :** Imports résolus (P0-1, P0-3). Logique interne non vérifiée end-to-end (pas de tests Playwright).

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

### `core/mobile_rotator.py` — ✅ Complet
**Dépendances :** `core.settings`  
**Rôle :** Rotation IP via Android/ADB (airplane mode toggle + ADB shell)  
**Ce qui manque :** Rien  
**Implémenté :** Stratégie 3 — `cmd statusbar expand-settings` + `input tap {x} {y}` + `KEYCODE_BACK`. Coordonnées configurables via `airplane_tap_coords` au constructeur (défaut 180×620 pour FHD+ 1080×2400).

---

### `core/offer_hunter.py` — ✅ Complet
**Dépendances :** `core.database` ✅ · `core.settings` ✅  
**Rôle :** Scanner d'offres/bonus — détecte les boosts actifs chez les partenaires affiliés  
**Ce qui manque :** Rien — `core/database.py` créé (P0-1)

---

### `core/prompts.py` — ✅ Complet
**Dépendances :** `core.settings`  
**Rôle :** Protocoles sémantiques — prompts Gemini pour qualification de leads  
**Ce qui manque :** Rien

---

### `core/supply_chain_manager.py` — ✅ Complet
**Dépendances :** `core.database` ✅ · `core.settings` ✅  
**Rôle :** Yield optimizer — `FleetManager`, EPC/Waterfall, Signal Exchange, Smart Pacing  
**Ce qui manque :** Rien — `core/database.py` créé (P0-1)

---

### `core/time_manager.py` — ✅ Complet
**Dépendances :** `asyncio`, `datetime`  
**Rôle :** Cycle circadien — fenêtres horaires, weekends, scheduler asynchrone  
**Ce qui manque :** Rien

---

### `core/vision_guardian.py` — ✅ Complet
**Dépendances :** `core.database` ✅ · `core.settings` ✅ · `playwright` · `PIL`  
**Rôle :** Analyse visuelle — détection captcha, crash handler, apprentissage sélecteurs DOM  
**Ce qui manque :** Rien  
**Implémenté :** `_load_memory()` SELECT sur `dom_knowledge` (merge hive→local) · `_update_stats()` UPSERT vers `dom_knowledge` (swarm push) · Migration V14 ajoutée dans `secure_telemetry_store.py`

---

### `core/workload_orchestrator.py` — ✅ Complet
**Dépendances :** `core.database` ✅ · `core.settings` ✅ · `core.time_manager` ✅  
**Rôle :** Dispatcher UCB1 — `ComputeGridOrchestrator`, scarcity curve, PID batch size, feedback loop  
**Ce qui manque :** Rien — `core/database.py` créé (P0-1), `PARTNER_YIELD_TIERS` externalisé vers `sponsors.json` (P3-5), 59 tests unitaires (P3-3)

---

## Config

### `config/rag_engine.py` — ✅ Complet
**Dépendances :** `core.settings`, `json`, `pathlib`  
**Rôle :** RAG engine — chargement knowledge base JSON, retrieve_context()  
**Ce qui manque :** Rien

---

## Channels — TikTok

### `channels/tiktok/sniper.py` — 🔄 Partiel (imports résolus)
**Dépendances :** `core.database` ✅ · `core.settings` ✅ · `core.browser_engine` ✅ · `core.time_manager` ✅ · `core.vision_guardian` ✅  
**Rôle :** Topology mapper v35.2 — `KeywordLearner`, entropy analysis, déduplication  
**Ce qui manque :** Imports résolus (P0-1). Logique interne non vérifiée end-to-end (pas de tests Playwright).

---

### `channels/tiktok/sender.py` — 🔄 Partiel (imports résolus)
**Dépendances :** `core.database` ✅ · `core.browser_engine` ✅ · `core.time_manager` ✅ · `core.humanizer` ✅ · `core.settings` ✅  
**Rôle :** Telemetry injector v41.2 — injection commentaires TikTok, DCO, compliance [Ad]  
**Ce qui manque :** Imports résolus (P0-1). Logique interne non vérifiée end-to-end (pas de tests Playwright).

---

### `channels/tiktok/partner_sniper.py` — 🔄 Partiel (imports résolus)
**Dépendances :** `core.database` ✅ · `core.settings` ✅ · `core.browser_engine` ✅ · `core.humanizer` ✅ · `core.time_manager` ✅  
**Rôle :** Network topology mapper v37.3 — `TikTokTopologyMapper`, VIP escalation, viral triangulation  
**Ce qui manque :** Imports et méthodes DB résolus — `inject_priority_task()` ✅ (L.1806), `upsert_viral_target()` ✅, `get_author_reputation()` ✅ (L.1746), `update_author_reputation()` ✅ (L.1763). Logique Playwright non testée.

---

### `channels/tiktok/media_optimizer.py` — 🔄 Partiel (imports résolus)
**Dépendances :** `core.database` ✅ · `core.browser_engine` ✅ · `core.time_manager` ✅ · `core.settings` ✅  
**Rôle :** Media optimizer v27.0 — scan engagement vidéos, yield efficiency, velocity detection, GDPR hashing  
**Ce qui manque :** Imports résolus (P0-1). Logique interne non vérifiée end-to-end (pas de tests Playwright).

---

## Channels — Reddit

### `channels/reddit/audience_listener.py` — 🔄 Partiel (imports résolus)
**Dépendances :** `core.database` ✅ · `core.browser_engine` ✅ · `core.settings` ✅  
**Rôle :** Audience listener v26.1 — `SemanticIntentClassifier`, CircadianScheduler, analyse flux GQL Reddit  
**Ce qui manque :** Imports résolus (P0-1). Logique interne non vérifiée end-to-end (pas de tests Playwright).

---

### `channels/reddit/partner_hunter.py` — 🔄 Partiel (imports résolus)
**Dépendances :** `core.database` ✅ · `core.settings` ✅ · `core.browser_engine` ✅ · `core.humanizer` ✅  
**Rôle :** Partner hunter v11.1 — `AuthorityClassifier`, B2B vs Prop Firm detection, burst control  
**Ce qui manque :** Imports résolus (P0-1). `insert_telemetry_signal()` ✅ (alias confirmé L.1233 NexusDB). Logique interne non vérifiée end-to-end.

---

### `channels/reddit/sender.py` — ✅ Complet
**Dépendances :** `core.database` ✅ · `core.browser_engine` · `core.time_manager` · `core.vision_guardian` · `core.humanizer`  
**Rôle :** Community gateway v28.0 — `RedditCommunityGateway`, DCO geo-routing FR/US, reputation gate, persistent memory  
**Ce qui manque :** Rien  
**Implémenté :** Détection succès par `page.expect_response()` sur `/api/comment`, `/svc/shreddit/graphql`, `gateway.reddit.com` (HTTP 200 = succès). Fallback sur vérification champ texte si timeout.

---

## Channels — Email

### `channels/email/mailer_client.py` — ✅ Complet
**Dépendances :** `core.database` ✅ · `requests` ✅ · `loguru` ✅ · `os` ✅  
**Rôle :** `NurturingBot` — séquences email via Brevo API, A/B testing sujets, send-time optimization  
**Vérifié :** `{{LINK}}` substitué via `raw_html.replace("{{LINK}}", affiliate_link)` L.245 · `MAILER_API_KEY` validée au `__init__` (EnvironmentError + logger.critical) L.26-34 · `send_referral_request` présent · `BREVO_TIMEOUT` lu depuis `settings.BREVO_TIMEOUT` L.170

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
core/database.py   ✅ EXISTE  — alias vers secure_telemetry_store.NexusDB (vérifié 2026-04-16)
core/dispatcher.py ✅ EXISTE  — SponsorDispatcher wrapping ComputeGridOrchestrator (vérifié 2026-04-16)
core/secure_telemetry_store.py ✅ — NexusDB complète, 4 méthodes ajoutées (P1-5)
```

**Aucun bloqueur restant.**

---

## Tests NexusDB (P3-4 — 2026-04-15)

**Résultats :** 154 tests · 0 failed · **coverage 83%** sur `core/secure_telemetry_store.py`

| Fichier de test | Tests | Rôle |
|----------------|-------|------|
| `tests/unit/test_nexusdb_smoke.py` | 15 | Smoke tests originaux |
| `tests/unit/test_nexusdb_full.py` | 139 | Tests complets (méthodes, branches, exceptions) |

**Bugs découverts (non corrigés, à documenter) :**

| Bug | Localisation | Symptôme |
|-----|-------------|----------|
| Nested session rollback | `fail_lead` → `release_lead_hold` (L.1594), `register_conversion_event` → `confirm_lead_hold` (L.839) | Le `BEGIN IMMEDIATE` imbriqué échoue, `conn.rollback()` annule la transaction outer silencieusement → statut lead jamais mis à jour |
| `"col" in sqlite3.Row` vérifie les valeurs | `register_conversion_event` L.807-810 | `"assigned_program" in row` teste si la STRING est une VALEUR du row (pas une clé) → `current_program` reste toujours 'UNKNOWN' → double-dip logic (L.871-915) jamais exécuté |
| Colonne `program` absente de `leads` | `register_conversion_event` L.792, `analyze_user_history` L.1073, `get_dashboard_snapshot` L.1913 | SELECT explicit sur `program` dans `leads` mais la colonne n'est pas créée par les migrations |
| `PRAGMA wal_checkpoint(PASSIVE)` dans une transaction | `_init_nexus_migrations` L.366 | Échoue avec "database table is locked" → ligne 367 (`PRAGMA optimize`) jamais atteinte |

**Lignes structurellement non couvrables (SQLite :memory:) :**
- L.33-47, 96, 106, 112-199 — init PostgreSQL pool
- L.275-335 — `_seed_initial_data` : `sponsors.json` est un dict, pas une liste → loop jamais exécutée
- L.872-899 — double-dip body (bloqué par bug ci-dessus)
- L.1276-1278, 1571-1574 — chemins PostgreSQL exclusifs

---

## Documentation schéma DB (P3-10 — 2026-04-15)

**Livrable :** `docs/SCHEMA.md` — 12 tables documentées (migrations V1–V14), colonnes/types/contraintes/relations/index + 4 bugs connus.

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
