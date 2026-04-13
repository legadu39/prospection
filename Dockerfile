# Prosprection automatisée/Dockerfile
# --- STAGE 1 : BUILDER FRONTEND (Node.js) ---
FROM node:18-alpine as frontend_builder
WORKDIR /app_front

# Copie des fichiers de configuration NPM
COPY frontend/package*.json ./
# Installation propre des dépendances JS
RUN npm ci

# Copie du code source Frontend
COPY frontend/ ./
# Compilation (Vite -> dist/)
RUN npm run build


# --- STAGE 2 : RUNNER PYTHON (Production) ---
FROM python:3.11-slim-bookworm

# Variables d'environnement pour l'optimisation
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.7.1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    CHROME_BIN=/usr/bin/google-chrome

# Configuration système et dépendances
# Ajout de 'tini' pour la gestion des processus et outils système nécessaires
RUN apt-get update && apt-get install -y --no-install-recommends \
    tini \
    curl \
    gnupg \
    procps \
    git \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Installation de Google Chrome stable pour Playwright/Selenium
RUN curl -fsSL https://dl-ssl.google.com/linux/linux_signing_key.pub | \
    gpg --dearmor -o /usr/share/keyrings/google-chrome.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update && apt-get install -y --no-install-recommends \
    google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Création de l'utilisateur non-root pour la sécurité
RUN groupadd -r nexus && useradd -r -g nexus -m -d /app nexus

WORKDIR /app

# --- OPTIMISATION DU CACHE (SMART LAYERING) ---
# On installe Playwright et les navigateurs AVANT le reste des requirements.
RUN pip install --no-cache-dir playwright
RUN playwright install chromium && playwright install-deps chromium

# --- INSTALLATION DES DÉPENDANCES PROJET ---
COPY requirements.txt .
# Installation des dépendances + pydantic-settings/psycopg2 si manquants
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir pydantic-settings psycopg2-binary

# --- FUSION : Récupération du site compilé depuis le Stage 1 ---
# On copie le dossier 'dist' du builder vers 'static_site' dans l'image finale
COPY --from=frontend_builder /app_front/dist /app/static_site

# --- COPIE DU CODE SOURCE BACKEND ---
COPY . .

# Création des dossiers nécessaires et permissions
# FIX AUDIT : On s'assure que nexus a les droits sur tout /app pour éviter les problèmes de volume
RUN mkdir -p /app/logs /app/chrome_profiles /app/locks /app/import /app/config && \
    chown -R nexus:nexus /app

# Switch vers l'utilisateur non-root
USER nexus

# Point d'entrée avec Tini pour éviter les processus zombies
ENTRYPOINT ["/usr/bin/tini", "--"]

# Commande de démarrage par défaut
CMD ["python", "launcher.py"]