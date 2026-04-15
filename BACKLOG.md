# BACKLOG.md — Matrice de priorité

> Dernière mise à jour : 2026-04-13  
> Taille : **S** = <1h · **M** = 1-4h · **L** = >4h  
> Statut : ⬜ à faire · 🔄 en cours · ✅ fait

---

## P0 — Bloquant (le projet ne démarre pas du tout)

> Critère strict : ImportError ou crash immédiat au démarrage d'au moins un service.

| ID | Statut | Tâche | Taille | Fichier(s) | Détail |
|----|--------|-------|--------|------------|--------|
| P0-1 | ✅ | Créer `core/database.py` (alias NexusDB) | **S** | 13 fichiers | `from core.database import NexusDB` échoue sur 13 fichiers. Fix en 1 ligne : `from core.secure_telemetry_store import NexusDB`. **Débloque tout le reste.** Fait le 2026-04-13. Bugs collatéraux résolus : SyntaxError `\|\|` dans `secure_telemetry_store.py:1467`, 3 corrections Pydantic v2 dans `settings.py`. |
| P0-2 | ✅ | Créer `core/dispatcher.py` (`SponsorDispatcher`) | **M** | `pipeline_bridge.py:35,62` | `SponsorDispatcher` inexistant → `ImportError` au démarrage de `pipeline_bridge`. Créer une classe `SponsorDispatcher(db)` wrappant `ComputeGridOrchestrator` de `workload_orchestrator.py` et exposant `process_dispatch_cycle()`. |
| P0-3 | ✅ | Compléter `requirements.txt` (packages manquants) | **S** | `requirements.txt` | Packages utilisés mais absents : `psutil` (`browser_engine.py`, `launcher.py`), `aiofiles` (`browser_engine.py`), `pydantic-settings` (`settings.py`), `fastapi`+`uvicorn` (`ad_exchange_server.py`). Sans eux → `ModuleNotFoundError` sur machine vierge. |
| P0-4 | ✅ | Auditer les méthodes NexusDB manquantes | **M** | `core/secure_telemetry_store.py` | Audit complet du fichier (1484 lignes lues). Résultat : `insert_telemetry_signal` ✅ (alias ligne 1233), `update_subreddit_stats` ✅, `get_subreddit_stats` ✅, `upsert_viral_target` ✅, `get_next_lead_to_send_by_source` ✅. **MANQUANTES : `inject_priority_task`, `get_author_reputation`, `update_author_reputation`, `get_sponsor_stats`** → voir P1-5 ci-dessous. |

---

## P1 — Important (le projet démarre mais une fonctionnalité core est cassée)

> Critère : la feature est utilisable en prod mais produit un résultat incorrect ou nul.

| ID | Statut | Tâche | Taille | Fichier(s) | Détail |
|----|--------|-------|--------|------------|--------|
| P1-1 | ✅ | Résoudre `{LINK}` dans le template email | **S** | `channels/email/mailer_client.py:116` | `href='{{LINK}}'` jamais substitué → bouton CTA cassé dans 100% des emails envoyés. Implémenter la substitution dynamique depuis `sponsors.json` selon `product_key` (PROP_FIRM / CRYPTO_WALLET). |
| P1-2 | ✅ | Compléter et vérifier la fin de `mailer_client.py` | **S** | `channels/email/mailer_client.py:160+` | Fichier lu jusqu'à la ligne 159 seulement. La méthode d'envoi principale et le point d'entrée `if __name__ == "__main__"` sont potentiellement absents ou incomplets. Lire les lignes 160+ et compléter. |
| P1-3 | ✅ | Normaliser tous les imports vers `core.database` | **S** | `pipeline_bridge.py:34`, `mailer_client.py:10` | Ces deux fichiers importent `core.secure_telemetry_store` directement, contredisant la convention. Homogénéiser pour que **tous** les fichiers passent par `core.database`. |
| P1-4 | ✅ | Valider `MAILER_API_KEY` au démarrage de `NurturingBot` | **S** | `channels/email/mailer_client.py:17` | `API_KEY = os.getenv("MAILER_API_KEY")` sans valeur par défaut ni validation. Si la variable est absente du `.env`, les requêtes Brevo retournent 401 silencieusement — aucun email envoyé, aucune erreur visible. Ajouter un check au `__init__` avec `logger.critical` + arrêt propre si clé absente. |
| P1-5 | ✅ | Implémenter 4 méthodes manquantes dans NexusDB | **M** | `core/secure_telemetry_store.py` | Découvert lors de l'audit P0-4. Ces méthodes sont appelées mais absentes : `inject_priority_task()` (`tiktok/partner_sniper.py:380`), `get_author_reputation()` (`tiktok/partner_sniper.py:335`), `update_author_reputation()` (`tiktok/partner_sniper.py:444`), `get_sponsor_stats()` (`supply_chain_manager.py:99`). → `AttributeError` au runtime sur les channels TikTok et le yield manager. |

---

## P2 — Utile (le projet fonctionne mais avec des bugs ou comportements incorrects)

> Critère : impact visible en production, mais le service continue de tourner.

| ID | Statut | Tâche | Taille | Fichier(s) | Détail |
|----|--------|-------|--------|------------|--------|
| P2-1 | ✅ | Implémenter la lecture `dom_knowledge` dans `vision_guardian.py` | **M** | `core/vision_guardian.py:139-142` | `pass + TODO` — le SELECT sur la table `dom_knowledge` n'est pas implémenté. VisionGuardian repart de zéro à chaque redémarrage, sans capitaliser sur les sélecteurs DOM qui ont fonctionné. |
| P2-2 | ✅ | Implémenter la sauvegarde sélecteur dans `vision_guardian.py` | **S** | `core/vision_guardian.py:179-181` | `pass` — les sélecteurs DOM découverts dynamiquement ne sont pas persistés en DB. Symétriquement critique avec P2-1. |
| P2-3 | ✅ | Compléter la stratégie 3 de `mobile_rotator.py` | **S** | `core/mobile_rotator.py:199-200` | `pass` explicite + commentaire "input tap à implémenter via coordonnées". Bloquant si la rotation IP 4G par ADB doit utiliser la stratégie tap direct. |
| P2-4 | ✅ | Fiabiliser la détection de succès dans `reddit/sender.py` | **S** | `channels/reddit/sender.py:508-510` | `if not input.is_visible() or input_value() == ""` est fragile : faux positifs si Reddit rafraîchit la page. Implémenter un listener sur la réponse réseau (HTTP 200 de l'API commentaire) pour une confirmation fiable. |

---

## P3 — Nice to have (amélioration, polish, dette technique)

> Critère : aucun impact fonctionnel immédiat.

| ID | Statut | Tâche | Taille | Fichier(s) | Détail |
|----|--------|-------|--------|------------|--------|
| P3-1 | ✅ | Ajouter `__init__.py` à tous les packages | **S** | `core/`, `channels/`, `channels/tiktok/`, `channels/reddit/`, `channels/email/`, `config/` | Absents. Rend les imports relatifs fragiles selon l'environnement Python. |
| P3-2 | ✅ | Endpoint `/health` dans `ad_exchange_server.py` | **S** | `core/ad_exchange_server.py` | Manquant pour healthcheck Docker/K8s. Retourner `{"status":"ok","db":"connected","sponsors_loaded":N}`. |
| P3-3 | ✅ | Tests unitaires `workload_orchestrator.py` | **M** | `core/workload_orchestrator.py` | 59 tests — UCB1, scarcity curve, PID, fuzzy matching. `infer_process_type()`, `_calculate_ucb1_score()`, `attempt_atomic_allocation()` couverts. |
| P3-4 | ✅ | Tests unitaires `secure_telemetry_store.py` (NexusDB) | **L** | `core/secure_telemetry_store.py` | 154 tests (smoke + full) — coverage 83% sur `secure_telemetry_store.py`. SQLite `:memory:` only. Bugs découverts : (1) `fail_lead`/`release_lead_hold` → nested session rollback silencieux, (2) `register_conversion_event` → même bug + `"col" in sqlite3.Row` vérifie les valeurs pas les clés (double-dip inaccessible), (3) colonne `program` absente de `leads` (ajout manuel en fixture). |
| P3-5 | ✅ | Externaliser `PARTNER_YIELD_TIERS` vers `config/sponsors.json` | **S** | `core/workload_orchestrator.py:75-101` | Tiers de payout hardcodés (APEX=150€, LEDGER=60€…). Doit vivre dans `sponsors.json` pour être ajustable sans redéploiement. |
| P3-6 | ✅ | Gérer `asyncio.CancelledError` dans `pipeline_bridge.py` | **S** | `pipeline_bridge.py:run_pipeline()` | Boucle principale ne catch pas `CancelledError` → shutdown brutal sans cleanup DB. |
| P3-7 | ✅ | Stratégie de rotation du `PRIVACY_SALT` | **M** | `core/secure_telemetry_store.py:69` | Sel de hachage RGPD statique. Prévoir rotation + migration des hashes si compromis. |
| P3-8 | ✅ | Normaliser `time.time()` vs `datetime.utcnow()` | **S** | Plusieurs fichiers | Mix des deux conventions pour horodater les leads. Utiliser `time.time()` (epoch float) partout en DB. |
| P3-9 | ✅ | Externaliser le timeout Brevo dans `settings` | **S** | `channels/email/mailer_client.py:142` | `timeout=10` hardcodé dans `requests.post`. Déplacer vers `settings.BREVO_TIMEOUT`. |
| P3-10 | ✅ | Documenter le schéma DB (tables et colonnes) | **M** | `core/secure_telemetry_store.py` | `docs/SCHEMA.md` généré le 2026-04-15 — 12 tables (V1–V14), colonnes/types/contraintes/index/relations + 4 bugs documentés. |

---

## Récapitulatif

| Priorité | Tâches | Dont ✅ | Effort restant | Débloque |
|----------|--------|---------|----------------|----------|
| **P0** | 4 | 4 | ~0h | Le projet démarre |
| **P1** | 5 | 5 | ~0h | Features core fonctionnelles ✅ |
| **P2** | 4 | 4 | ~0h | Comportement correct en prod ✅ |
| **P3** | 10 | 10 | ~0h | Dette technique et robustesse ✅ |
| **Total** | **23** | **23** | **~0h** | |

---

## Ordre d'exécution recommandé

```
Sprint 1 — Démarrage (P0, ~5h)
  P0-1  core/database.py          ✅ FAIT
  P0-4  audit méthodes NexusDB    ✅ FAIT (4 méthodes manquantes → P1-5)
  P0-3  requirements.txt          10 min   ← PROCHAINE TÂCHE
  P0-2  core/dispatcher.py        2h       ← après P0-3

Sprint 2 — Core features (P1, ~4h)
  P1-3  normaliser imports         30 min
  P1-4  valider MAILER_API_KEY     30 min
  P1-2  compléter mailer_client    1h
  P1-1  résoudre {LINK} email      1h

Sprint 3 — Bugs prod (P2, ~4h)
  P2-1 + P2-2  vision_guardian     2h  (faire les deux ensemble)
  P2-3  mobile_rotator             1h
  P2-4  reddit success detection   1h

Sprint 4 — Polish (P3, selon dispo)
  P3-1  __init__.py                5 min
  P3-2  /health endpoint           30 min
  P3-6  CancelledError             30 min
  P3-5  externaliser YIELD_TIERS   1h
  P3-8  normaliser timestamps      1h
  P3-3  tests workload_orch        2-3h
  P3-4  tests NexusDB              4-6h
  P3-7  PRIVACY_SALT rotation      2-3h
  P3-9  timeout Brevo              15 min
  P3-10 doc schéma DB              2h
```
