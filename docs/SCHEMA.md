# SCHEMA.md — Schéma NexusDB

> Source : `core/secure_telemetry_store.py` migrations V1–V14  
> Généré le 2026-04-15. DB : SQLite (dev) / PostgreSQL (prod).

---

## Table de bord des migrations

| Version | Contenu |
|---------|---------|
| V1 | `leads`, `sponsors`, `dispatch_logs` + index flux |
| V2 | Index perf `sponsors`, index `leads.ai_status` |
| V3 | `campaigns` |
| V6 | `viral_queue` |
| V7 | `accounts` |
| V8 | `conversions` |
| V9 | `subreddit_stats` |
| V10 | `leads.meta_analysis` (ALTER) |
| V11 | `author_reputation` |
| V12 | `sponsors.balance_available/reserved`, `dispatch_logs.cost_charged/status` (ALTER) |
| V13 | `ledger` |
| V14 | `dom_knowledge` |

> Versions V4 et V5 absentes des migrations (historique supprimé, schéma intact).

---

## Tables

### `_schema_version`

Table interne de tracking des migrations.

| Colonne | Type | Contraintes | Description |
|---------|------|-------------|-------------|
| `version` | INTEGER | PRIMARY KEY | Numéro de version appliquée |
| `updated_at` | TEXT | — | ISO timestamp de l'application |

---

### `leads`

Table principale des signaux télémétriques (alias sémantique : `telemetry_signals`).

| Colonne | Type | Contraintes | Description |
|---------|------|-------------|-------------|
| `id` | TEXT | PRIMARY KEY | UUID du lead |
| `source` | TEXT | — | Canal d'acquisition (`tiktok`, `reddit`, `email`…) |
| `author` | TEXT | — | Node ID **haché SHA-256** (jamais en clair — RGPD) |
| `url` | TEXT | — | URL source du signal |
| `comment_text` | TEXT | — | Payload technique (sanitisé) |
| `ai_status` | TEXT | DEFAULT `'PENDING'` | Statut qualification IA (`PENDING`, `QUALIFIED`, `REJECTED`) |
| `ai_confidence` | REAL | DEFAULT `0.0` | Score de confiance Gemini (0.0–1.0) |
| `ai_draft` | TEXT | — | Brouillon de réponse généré par IA |
| `ai_process_info` | TEXT | DEFAULT `'{}'` | JSON — métadonnées du pipeline IA |
| `assigned_sponsor_id` | TEXT | — | FK vers `sponsors.id` (NULL si non dispatché) |
| `assigned_program` | TEXT | — | Identifiant programme affilié |
| `assigned_ref_link` | TEXT | — | Lien affilié assigné |
| `draft_reply` | TEXT | — | Réponse finale prête à l'envoi |
| `status` | TEXT | DEFAULT `'NEW'` | Statut workflow : `NEW` → `QUALIFIED` → `DISPATCHING` → `SENT` / `FAILED` |
| `created_at` | REAL | — | Epoch float (`time.time()`) |
| `updated_at` | REAL | — | Epoch float (`time.time()`) |
| `meta_analysis` | TEXT/JSONB | DEFAULT `'{}'` | JSON — analyse sémantique enrichie (V10) |

**Index :**
- `idx_leads_flow` — `(status, source, created_at)`
- `idx_leads_ai` — `(ai_status)`

> **Bug connu :** La colonne `program` n'existe **pas** dans `leads`. Elle est requêtée explicitement dans `register_conversion_event` (L.792), `analyze_user_history` (L.1073) et `get_dashboard_snapshot` (L.1913) → `OperationalError` au runtime. Voir [section Bugs](#bugs-documentés).

---

### `sponsors`

Mandats affiliés (alias sémantique : `mandates`).

| Colonne | Type | Contraintes | Description |
|---------|------|-------------|-------------|
| `id` | TEXT | PRIMARY KEY | Identifiant sponsor (ex : `APEX`, `LEDGER`) |
| `label` | TEXT | — | Nom d'affichage |
| `program` | TEXT | — | Clé programme (`PROP_FIRM`, `CRYPTO_WALLET`, `SAAS`…) |
| `ref_link` | TEXT | — | Lien affilié de base |
| `ref_code` | TEXT | — | Code affilié |
| `priority` | INTEGER | DEFAULT `2` | Priorité de dispatch UCB1 (1=haute, 3=basse) |
| `monthly_limit_hard` | INTEGER | DEFAULT `3` | Cap mensuel de conversions vérifiées |
| `verified_count_month` | INTEGER | DEFAULT `0` | Compteur mensuel conversions vérifiées |
| `last_verification_date` | TEXT | — | ISO date dernière vérification |
| `conversion_rate_estimate` | TEXT | DEFAULT `'0.10'` | Taux de conversion estimé (stocké en TEXT) |
| `active` | INTEGER | DEFAULT `1` | Flag activité (0/1) |
| `balance_available` | REAL | DEFAULT `0.0` | Budget disponible en € (V12) |
| `balance_reserved` | REAL | DEFAULT `0.0` | Budget réservé en attente de confirmation (V12) |

**Index :**
- `idx_sponsors_perf` — `(program, active, priority)`

---

### `dispatch_logs`

Journal de chaque dispatch lead → sponsor.

| Colonne | Type | Contraintes | Description |
|---------|------|-------------|-------------|
| `id` | INTEGER (SQLite) / SERIAL (PG) | PRIMARY KEY AUTOINCREMENT | ID auto-incrémenté |
| `lead_id` | TEXT | FK → `leads.id` | Lead dispatché |
| `sponsor_id` | TEXT | FK → `sponsors.id` | Sponsor cible |
| `program` | TEXT | — | Programme affilié au moment du dispatch |
| `dispatched_at_ts` | REAL | — | Epoch float du dispatch |
| `dispatched_at_iso` | TEXT | — | ISO datetime du dispatch |
| `cost_charged` | REAL | DEFAULT `0.0` | Coût CPA débité (V12) |
| `status` | TEXT | DEFAULT `'COMPLETED'` | Statut du log (`COMPLETED`, `FAILED`, `PENDING`) (V12) |

**Index :**
- `idx_logs_calc` — `(sponsor_id, dispatched_at_iso)`

---

### `campaigns`

Campagnes d'acquisition actives avec budget boosté.

| Colonne | Type | Contraintes | Description |
|---------|------|-------------|-------------|
| `program` | TEXT | PRIMARY KEY | Clé programme (FK logique vers `sponsors.program`) |
| `amount` | INTEGER | — | Montant du boost en € |
| `is_boosted` | INTEGER | — | Flag boost actif (0/1) |
| `end_date` | TEXT | — | ISO date de fin de campagne |
| `url` | TEXT | — | URL de la campagne |
| `updated_at` | REAL | — | Epoch float dernière MAJ |

---

### `viral_queue`

File de leads viraux à traiter en priorité (TikTok cascade).

| Colonne | Type | Contraintes | Description |
|---------|------|-------------|-------------|
| `username` | TEXT | PRIMARY KEY | Username **haché** (RGPD) |
| `source` | TEXT | DEFAULT `'cascade'` | Source de détection |
| `priority` | INTEGER | DEFAULT `0` | Score de priorité (plus élevé = traité en premier) |
| `status` | TEXT | DEFAULT `'PENDING'` | Statut : `PENDING`, `PROCESSING`, `DONE` |
| `created_at` | REAL | — | Epoch float |
| `updated_at` | REAL | — | Epoch float |

**Index :**
- `idx_viral_queue` — `(status, priority DESC, created_at ASC)`

---

### `accounts`

Comptes bots gérés par le moteur CDP/Playwright.

| Colonne | Type | Contraintes | Description |
|---------|------|-------------|-------------|
| `id` | TEXT | PRIMARY KEY | Identifiant compte (haché ou alias interne) |
| `platform` | TEXT | DEFAULT `'tiktok'` | Plateforme cible (`tiktok`, `reddit`) |
| `status` | TEXT | DEFAULT `'ACTIVE'` | Statut : `ACTIVE`, `COOLDOWN`, `BANNED` |
| `proxy_url` | TEXT | — | URL proxy résidentiel assigné |
| `last_active_ts` | REAL | — | Epoch float dernière activité |
| `success_count` | INTEGER | DEFAULT `0` | Compteur actions réussies |
| `fail_count` | INTEGER | DEFAULT `0` | Compteur échecs |
| `meta_info` | TEXT | DEFAULT `'{}'` | JSON — métadonnées (CDP port, profil, user-agent…) |
| `updated_at` | REAL | — | Epoch float |

**Index :**
- `idx_accounts_status` — `(platform, status)`

---

### `conversions`

Événements de conversion CPA (postback affilié).

| Colonne | Type | Contraintes | Description |
|---------|------|-------------|-------------|
| `id` | TEXT | PRIMARY KEY | UUID conversion |
| `lead_id` | TEXT | FK → `leads.id` | Lead source |
| `sponsor_id` | TEXT | — | Sponsor crédité |
| `amount` | REAL | — | Montant CPA en devise |
| `currency` | TEXT | DEFAULT `'EUR'` | Devise (`EUR`, `USD`) |
| `converted_at` | REAL | — | Epoch float de la conversion |
| `meta_data` | TEXT | DEFAULT `'{}'` | JSON — données postback brutes |

**Index :**
- `idx_conversions_sponsor` — `(sponsor_id)`

---

### `subreddit_stats`

Statistiques de performance par subreddit (circuit breaker Reddit).

| Colonne | Type | Contraintes | Description |
|---------|------|-------------|-------------|
| `subreddit` | TEXT | PRIMARY KEY | Nom du subreddit |
| `success_count` | INTEGER | DEFAULT `0` | Nombre d'envois réussis |
| `fail_count` | INTEGER | DEFAULT `0` | Nombre d'échecs |
| `last_fail_ts` | REAL | — | Epoch float du dernier échec |
| `updated_at` | REAL | — | Epoch float dernière MAJ |

---

### `author_reputation`

Système de réputation par auteur et plateforme (anti-spam, VIP escalation).

| Colonne | Type | Contraintes | Description |
|---------|------|-------------|-------------|
| `author_id` | TEXT | PK composite | Identifiant auteur **haché SHA-256** |
| `platform` | TEXT | PK composite | Plateforme (`tiktok`, `reddit`) |
| `reputation_score` | INTEGER | DEFAULT `50` | Score 0–100 (50 = neutre) |
| `status` | TEXT | DEFAULT `'NEUTRAL'` | Statut : `NEUTRAL`, `TRUSTED`, `BANNED`, `VIP` |
| `expiry_ts` | REAL | DEFAULT `0` | Epoch float d'expiration du statut |
| `meta_data` | TEXT | DEFAULT `'{}'` | JSON — historique interactions |
| `updated_at` | REAL | — | Epoch float |

**Index :**
- `idx_reputation_status` — `(status, expiry_ts)`

---

### `ledger`

Grand livre financier — toutes les transactions budgétaires.

| Colonne | Type | Contraintes | Description |
|---------|------|-------------|-------------|
| `id` | INTEGER / SERIAL | PRIMARY KEY AUTOINCREMENT | ID auto-incrémenté |
| `transaction_date` | REAL | — | Epoch float de la transaction |
| `type` | TEXT | — | Type : `DEPOSIT`, `RESERVATION`, `CONSUMPTION`, `REFUND`, `BONUS` |
| `partner_id` | TEXT | FK → `sponsors.id` | Sponsor concerné |
| `amount` | REAL | — | Montant de la transaction |
| `currency` | TEXT | DEFAULT `'EUR'` | Devise |
| `reference_external` | TEXT | — | Référence postback / facture externe |
| `description` | TEXT | — | Description libre |
| `balance_after` | REAL | — | Solde `sponsors.balance_available` après opération |
| `created_at` | REAL | — | Epoch float |

**Index :**
- `idx_ledger_partner` — `(partner_id, transaction_date)`
- `idx_ledger_ref` — `(reference_external)`

---

### `dom_knowledge`

Base de connaissances CSS/XPath apprise par essai-erreur (swarm learning — VisionGuardian).

| Colonne | Type | Contraintes | Description |
|---------|------|-------------|-------------|
| `key_id` | TEXT | PK composite | Clé sémantique (ex : `reddit_comment_box`) |
| `selector` | TEXT | PK composite | Sélecteur CSS ou XPath |
| `success_count` | INTEGER | DEFAULT `0` | Nombre de succès avec ce sélecteur |
| `fail_count` | INTEGER | DEFAULT `0` | Nombre d'échecs |
| `weight` | REAL | DEFAULT `0.5` | Score de confiance UCB1 (0.0–1.0) |
| `updated_at` | REAL | — | Epoch float dernière MAJ |

**Index :**
- `idx_dom_key` — `(key_id, weight DESC)`

---

## Relations inter-tables

```
sponsors (id) ──< dispatch_logs (sponsor_id)
leads    (id) ──< dispatch_logs (lead_id)
leads    (id) ──< conversions   (lead_id)
sponsors (id) ──< ledger        (partner_id)

campaigns.program  →  sponsors.program  [FK logique, non contrainte]
viral_queue.username  →  leads.author   [FK logique, non contrainte]
```

---

## Bugs documentés (découverts suite P3-4)

Ces bugs existent dans `core/secure_telemetry_store.py` et ne sont **pas encore corrigés**.

### Bug 1 — Nested session rollback silencieux

**Localisation :** `fail_lead` → `release_lead_hold` (L.1594), `register_conversion_event` → `confirm_lead_hold` (L.839)

**Symptôme :** Le `BEGIN IMMEDIATE` imbriqué échoue. `conn.rollback()` annule la transaction *outer* silencieusement → le statut du lead n'est jamais mis à jour.

**Impact :** Leads bloqués en `DISPATCHING` indéfiniment (mitigé partiellement par `_reset_stuck_dispatches` au démarrage).

---

### Bug 2 — `"col" in sqlite3.Row` vérifie les valeurs, pas les clés

**Localisation :** `register_conversion_event` L.807–810

**Symptôme :** `"assigned_program" in row` teste si la chaîne est une *valeur* du row (pas un nom de colonne). `current_program` reste toujours `'UNKNOWN'` → la logique anti-double-dip (L.871–915) n'est jamais exécutée.

**Fix attendu :** Utiliser `row.keys()` ou accès direct `row["assigned_program"]` dans un `try/except KeyError`.

---

### Bug 3 — Colonne `program` absente de la table `leads`

**Localisation :** `register_conversion_event` L.792, `analyze_user_history` L.1073, `get_dashboard_snapshot` L.1913

**Symptôme :** `SELECT program FROM leads` → `sqlite3.OperationalError: no such column: program`. La colonne n'est créée par aucune migration (V1 à V14).

**Fix attendu :** Ajouter une migration V15 : `ALTER TABLE leads ADD COLUMN program TEXT DEFAULT NULL`.

---

### Bug 4 — `PRAGMA wal_checkpoint(PASSIVE)` dans une transaction active

**Localisation :** `_init_nexus_migrations` L.366

**Symptôme :** Le PRAGMA est exécuté à l'intérieur d'une transaction ouverte → `OperationalError: database table is locked`. La ligne suivante (`PRAGMA optimize`, L.367) n'est jamais atteinte.

**Fix attendu :** Déplacer les deux PRAGMAs *après* la fermeture de la transaction (`with self.session()` doit être terminé avant).
