# tests/conftest.py
"""
Fixtures partagées pour tous les tests Nexus.

Conventions (voir CLAUDE.md) :
- NexusDB toujours instancié avec SQLite :memory: — jamais mocké
- La fixture 'db' crée un schéma frais par test
- La fixture 'client' utilise httpx.AsyncClient avec app FastAPI + DB isolée
"""
import sys
import os
from pathlib import Path

# S'assure que la racine du projet est dans sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Variables d'environnement minimales pour que settings.py démarre sans .env réel
os.environ.setdefault("SECURITY_MASTER_KEY", "test-master-key-not-for-production")
os.environ.setdefault("POSTGRES_PASSWORD", "test")
os.environ.setdefault("USE_POSTGRES", "False")

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

from core.secure_telemetry_store import NexusDB


# ---------------------------------------------------------------------------
# Fixture : base de données SQLite en mémoire, schéma initialisé, isolée
# ---------------------------------------------------------------------------

@pytest.fixture()
def db():
    """
    Fournit une instance NexusDB pointant sur SQLite ':memory:'.
    Le schéma complet est appliqué (auto_migrate=True).
    Chaque test reçoit une DB vierge — aucune donnée ne persiste entre tests.
    """
    instance = NexusDB(db_path=Path(":memory:"), auto_migrate=True)
    yield instance
    instance.close()


# ---------------------------------------------------------------------------
# Fixture : AsyncClient httpx branché sur l'application FastAPI + DB isolée
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture()
async def client(db):
    """
    Fournit un httpx.AsyncClient prêt à appeler l'API FastAPI.
    La DB de l'application est remplacée par la fixture 'db' en mémoire.
    """
    # Import différé pour éviter le crash au démarrage si les dépendances
    # optionnelles (playwright, psutil…) sont absentes dans l'environnement CI
    from core.ad_exchange_server import app
    import core.ad_exchange_server as exchange_module

    # Substitution de la DB globale du module par la DB de test
    original_db = exchange_module.db
    exchange_module.db = db

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac

    # Restauration après le test
    exchange_module.db = original_db
