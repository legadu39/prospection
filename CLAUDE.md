# CLAUDE.md

## Stack technique

| Couche | Technologie | Fichier principal |
|--------|------------|-------------------|
| Orchestration services | Python subprocess | `launcher.py` |
| API HTTP | FastAPI + uvicorn | `core/ad_exchange_server.py` |
| Pipeline IA | AsyncIO | `pipeline_bridge.py` |
| Base de données | SQLite (dev) / PostgreSQL (prod) | `core/secure_telemetry_store.py` |
| Config | Pydantic v2 + python-dotenv | `core/settings.py` |
| IA / Qualification | Google Gemini 1.5 Flash | `core/gemini_processor.py` |
| Automatisation navigateur | Playwright + CDP | `core/browser_engine.py` |
| Email | Brevo API (requests) | `channels/email/mailer_client.py` |
| Logging | stdlib `logging` (partout) + `loguru` (mailer uniquement) | `core/logger_utils.py` |
| Frontend | React + Vite | `frontend/` |
| Déploiement | Docker | `Dockerfile` |

**Versions clés :** Python 3.10+ · Pydantic 2.5.2 · Playwright 1.40.0 · FastAPI 0.104.1 · google-generativeai 0.3.2

---

## Architecture

Le projet est une **plateforme d'acquisition et de routing de leads affiliés** (AdTech / Yield Management).

### Flux de données principal

```
Channels (acquisition)
  ├── channels/tiktok/sniper.py        → scan TikTok (recherche)
  ├── channels/tiktok/partner_sniper.py → topology mapper (profils)
  ├── channels/tiktok/media_optimizer.py → engagement vidéos
  ├── channels/reddit/audience_listener.py → flux GQL Reddit
  └── channels/reddit/partner_hunter.py → posts qualifiés Reddit
         │
         ▼ insert_raw_lead() / insert_telemetry_signal()
   NexusDB (core/secure_telemetry_store.py)
         │
         ▼ fetch_and_claim_leads()
   pipeline_bridge.py  ←→  Gemini IA (qualification sémantique)
         │
         ▼ run_dispatch_cycle()
   core/dispatcher.py → SponsorDispatcher → ComputeGridOrchestrator
         │
         ▼ atomic_dispatch_transaction()
   NexusDB (statut DISPATCHING)
         │
         ▼ get_next_lead_to_send_by_source()
   Channel senders (envoi)
  ├── channels/tiktok/sender.py         → injection commentaires TikTok
  ├── channels/reddit/sender.py         → réponses Reddit
  └── channels/email/mailer_client.py   → séquences Brevo
         │
         ▼ postback /click /postback
   core/ad_exchange_server.py           → attribution CPA
```

### Services orchestrés par `launcher.py`

1. `ad_exchange_server` — FastAPI sur port configuré dans settings
2. `pipeline_bridge` — boucle IA async
3. `tiktok_sniper` (sniper.py)
4. `tiktok_sender` (sender.py)
5. `reddit_listener` (audience_listener.py)

### Modules core partagés

- `NexusDB` (`core/secure_telemetry_store.py`) — **unique source de vérité DB**, importée via `core.database`
- `settings` (`core/settings.py`) — **unique source de vérité config** (chemins, ports, clés)
- `SandboxCDPProfile` + `StealthInjector` (`core/browser_engine.py`) — tous les bots Playwright passent par là
- `PhysicsHumanizer` (`core/humanizer.py`) — simulation comportement humain, obligatoire pour tous les senders
- `CircadianCycle` (`core/time_manager.py`) — fenêtres horaires, utilisé par tous les bots

### Geo-routing strict (Stratégie V3)

| Zone | Produits autorisés | Produits interdits |
|------|-------------------|--------------------|
| 🇺🇸 US / Global | Prop Firms (APEX, FTMO, Topstep) | — |
| 🇫🇷 FR / EU | SaaS (TradingView), Crypto PSAN (Ledger, Meria) | **Prop Firms INTERDITS** |
| Tout | — | Banque, crédit, prêt (exclus partout) |

---

## Conventions IMPÉRATIVES

**Imports DB :**
- Toujours `from core.database import NexusDB` — jamais `from core.secure_telemetry_store import NexusDB` directement
- `core/database.py` est l'alias officiel (1 ligne) vers `secure_telemetry_store`

Ne jamais créer de worktree Git (.claude/worktrees). Travailler toujours directement dans C:\Users\Mathieu\Desktop\prospection

**Privacy / RGPD :**
- Tout identifiant utilisateur (username, email, author_id) doit être **haché immédiatement** via `db._hash_identity(raw_id)` avant tout stockage ou log
- Ne jamais stocker de PII en clair dans la DB ni dans les logs

**Compliance légale :**
- Tout message public (commentaire TikTok, réponse Reddit, email) doit contenir une mention légale
  - EN : `(Ad. Not financial advice. Trading involves risk.)`
  - FR : `(Publicité. Ce site ne fournit pas de conseil en investissement financier. Les crypto-actifs sont risqués.)`

**Geo-routing :**
- Ne jamais router un lead FR/EU vers un Prop Firm
- Ne jamais router un lead US vers un contenu PSAN spécifique FR
- La détection géo se fait via `subreddit`, `intent`, ou headers HTTP selon le module

**Configuration :**
- Tous les chemins, ports, clés, flags passent par `settings` — jamais de valeurs hardcodées
- `PARTNER_YIELD_TIERS` est chargé depuis `config/sponsors.json["yield_tiers"]` (P3-5 ✅)

**Imports robustes :**
- Pattern standard pour les modules avec chemin relatif incertain :
  ```python
  try:
      from core.database import NexusDB
  except ImportError:
      sys.path.append(str(Path(__file__).resolve().parents[N]))
      from core.database import NexusDB
  ```

**Logging :**
- Utiliser `core/logger_utils.py` (`setup_secure_logger`) dans les modules core et pipeline
- Le logger masque automatiquement les PII — ne jamais bypasser

**Pas de `time.time()` et `datetime.utcnow()` mélangés :**
- Utiliser `time.time()` (epoch float) pour les timestamps stockés en DB (champ `updated_at`, `created_at`)
- Utiliser `datetime.utcnow()` uniquement pour affichage ou calculs relatifs

---

## Règle des tests

**213 tests existent** (`tests/unit/`) — NexusDB (154) + workload_orchestrator (59). Règles à respecter :

- Tester `NexusDB` avec SQLite **en mémoire uniquement** : `NexusDB(db_path=Path(":memory:"))`
- **Ne jamais mocker NexusDB** — les mocks ont déjà causé des divergences schema/prod par le passé
- Tester `infer_process_type()` et `_calculate_ucb1_score()` dans `workload_orchestrator.py` en priorité (logique financière critique)
- Pour les tests Playwright, utiliser un profil CDP dédié `test_profile` sans toucher aux profils de prod
- Les tests d'intégration doivent passer le flux complet : insert lead → statut `QUALIFIED` → dispatch → statut `DISPATCHING`

---

## Ce que tu dois faire après CHAQUE implémentation

1. **Vérifier l'import DB** : le fichier modifié utilise bien `from core.database import NexusDB` (pas `secure_telemetry_store`)
2. **Vérifier le geo-routing** : si le code route ou envoie du contenu, la règle FR ≠ Prop Firm est respectée
3. **Vérifier le hachage PII** : aucun `author`, `username`, `email` brut n'est stocké ou loggé
4. **Vérifier la mention légale** : tout message public contient la disclaimer (EN ou FR selon la cible)
5. **Mettre à jour `requirements.txt`** si un nouveau package a été utilisé
6. **Mettre à jour BACKLOG.md** : marquer la tâche accomplie, ajuster les tâches qui en dépendent
7. **Lire le fichier modifié** avant toute modification — ne jamais éditer à l'aveugle

---

## État du projet

> Dernière mise à jour : 2026-04-16 (audit complet — 23/23 tâches vérifiées dans le code)

**Le projet démarre.** Tous les bloqueurs P0 sont résolus. BACKLOG 23/23 ✅. Tag v1.1.0 publié.

| Composant | État vérifié |
|-----------|-------------|
| `core/database.py` | ✅ Existe — alias correct vers `secure_telemetry_store.NexusDB` |
| `core/dispatcher.py` | ✅ Existe — `SponsorDispatcher` wrappant `ComputeGridOrchestrator` |
| `requirements.txt` | ✅ Complet — psutil, aiofiles, pydantic-settings, fastapi, uvicorn présents |
| NexusDB (4 méthodes) | ✅ `inject_priority_task` · `get_author_reputation` · `update_author_reputation` · `get_sponsor_stats` |
| Tests | ✅ 213 tests — 154 NexusDB (83% coverage) + 59 workload_orchestrator |
| `docs/SCHEMA.md` | ✅ 12 tables documentées (migrations V1–V14) |

**Fichiers complets (vérifiés) :**
`check_links.py` · `core/database.py` · `core/dispatcher.py` · `core/gemini_processor.py` · `core/humanizer.py` · `core/logger_utils.py` · `core/offer_hunter.py` · `core/prompts.py` · `core/secure_telemetry_store.py` · `core/settings.py` · `core/supply_chain_manager.py` · `core/time_manager.py` · `core/vision_guardian.py` · `core/workload_orchestrator.py` · `core/ad_exchange_server.py` · `core/mobile_rotator.py` · `channels/email/mailer_client.py` · `channels/reddit/sender.py` · `config/rag_engine.py`

**Fichiers partiels (imports résolus, logique Playwright non testée end-to-end) :**
`core/browser_engine.py` · `channels/tiktok/sniper.py` · `channels/tiktok/sender.py` · `channels/tiktok/partner_sniper.py` · `channels/tiktok/media_optimizer.py` · `channels/reddit/audience_listener.py` · `channels/reddit/partner_hunter.py`

**Anomalie résiduelle :** `tests/conftest.py:28` importe `core.secure_telemetry_store` directement (violation convention P1-3 — non bloquant, tests seulement).
