### Prosprection automatisée/config/rag_engine.py
# config/rag_engine.py - KNOWLEDGE RETRIEVER V3.0 (INTELLIGENT SCORING & FUZZY MATCHING)
# -*- coding: utf-8 -*-

import json
import logging
import unicodedata
import asyncio
import collections
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

# Configuration
BASE_DIR = Path(__file__).resolve().parents[1]
KB_FILE = BASE_DIR / "config" / "knowledge_base.json"
KB_BACKUP = BASE_DIR / "config" / "knowledge_base.bak"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RAGEngine")

class RAGEngine:
    """
    Moteur de récupération de contexte avancé.
    Optimisé avec scoring TF-IDF simplifié, recherche floue et mémoire de session.
    """
    
    def __init__(self):
        self.data: List[Dict] = []
        self.index: Dict[str, Set[int]] = {}  # Map mot-clé normalisé -> Indices des entrées
        self.term_frequencies: Dict[str, float] = {}  # Pour le scoring (rareté des mots)
        self.session_context: collections.deque = collections.deque(maxlen=5) # Mémoire des 5 derniers mots-clés
        self._initialized = False

    async def initialize(self):
        """Charge la KB et construit l'index avec calculs de fréquence."""
        if self._initialized: return
        await self._load_knowledge_base()
        self._initialized = True

    async def _load_knowledge_base(self):
        """Charge le JSON, gère la redondance et construit l'index pondéré."""
        raw_data = await asyncio.to_thread(self._read_json_sync_with_retry)
        self.data = raw_data
        
        self.index = {}
        all_keywords = []
        
        for idx, entry in enumerate(self.data):
            keywords = entry.get("keywords", [])
            for kw in keywords:
                norm_kw = self._normalize_search_term(kw)
                if norm_kw:
                    if norm_kw not in self.index:
                        self.index[norm_kw] = set()
                    self.index[norm_kw].add(idx)
                    all_keywords.append(norm_kw)
        
        # Calcul de la rareté des termes (Inverse Frequency simplifiée)
        total_kws = len(all_keywords)
        counts = collections.Counter(all_keywords)
        self.term_frequencies = {k: 1.0 / (v / total_kws) for k, v in counts.items()}
        
        if self.data:
            logger.info(f"📚 RAG Intelligent : {len(self.data)} entrées, {len(self.index)} mots-clés indexés.")

    def _read_json_sync_with_retry(self) -> List[Dict]:
        """Lecture avec stratégie de fallback (Résilience)."""
        target_files = [KB_FILE, KB_BACKUP]
        
        for file_path in target_files:
            if file_path.exists():
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            # Création automatique du backup si on lit le fichier principal
                            if file_path == KB_FILE:
                                self._create_backup_sync(data)
                            return data
                except Exception as e:
                    logger.error(f"❌ Erreur lecture {file_path}: {e}")
                    continue
        
        return self._create_default_kb_sync()

    def _create_backup_sync(self, data: List[Dict]):
        """Crée une copie de sécurité (Persistance)."""
        try:
            with open(KB_BACKUP, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception:
            pass

    def _create_default_kb_sync(self) -> List[Dict]:
        """Génère un template si absent."""
        # V3 STRATEGY UPDATE: CLEAN SHEET
        # Suppression des offres Boursorama/Banque
        # Ajout des offres Prop Firms & Crypto
        default_data = [
            {
                "keywords": ["apex", "prop firm", "funding", "trading", "capital"],
                "context": "Apex Trader Funding permet d'obtenir jusqu'à 300k$ de capital. Pas de risque personnel. Gardez 90% des profits. Code promo dans la bio pour le reset."
            },
            {
                "keywords": ["crypto", "ledger", "securite", "wallet", "hack"],
                "context": "Ne laissez pas vos cryptos sur les échanges. Utilisez un Ledger Nano X pour la sécurité maximale (Self-Custody). Lien officiel disponible."
            },
            {
                "keywords": ["tradingview", "chart", "analyse", "outils"],
                "context": "TradingView est l'outil indispensable pour l'analyse technique. Version Pro avec -60% via notre lien partenaire."
            }
        ]
        try:
            KB_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(KB_FILE, "w", encoding="utf-8") as f:
                json.dump(default_data, f, indent=2, ensure_ascii=False)
            return default_data
        except Exception:
            return []

    def _normalize_search_term(self, text: str) -> str:
        """Normalisation agressive (Accents, casse, espaces)."""
        if not text: return ""
        nfkd_form = unicodedata.normalize('NFKD', text)
        only_ascii = nfkd_form.encode('ASCII', 'ignore').decode('utf-8')
        return only_ascii.lower().strip()

    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        """Calcule la distance d'édition entre deux chaînes (Recherche floue)."""
        if len(s1) < len(s2): return self._levenshtein_distance(s2, s1)
        if not s2: return len(s1)
        previous_row = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        return previous_row[-1]

    def _clean_content_for_ai(self, text: str) -> str:
        """Nettoyage pour l'IA sans altérer le sens."""
        if not text: return ""
        return "".join(ch for ch in text if unicodedata.category(ch)[0] != "C" or ch in "\n\t")

    async def retrieve_context(self, user_text: str) -> str:
        """Récupération avec Intelligence Logique et Mémoire."""
        if not user_text: return ""
        if not self._initialized: await self.initialize()
        if not self.data: return ""

        return await asyncio.to_thread(self._retrieve_context_sync, user_text)

    def _retrieve_context_sync(self, user_text: str) -> str:
        normalized_input = self._normalize_search_term(user_text)
        tokens = normalized_input.split()
        
        # Scoring : Dict[index_du_contexte, score_total]
        scored_indices = collections.defaultdict(float)
        
        # 1. Analyse des jetons (Tokens) directs et flous
        for token in tokens:
            # Match Exact (Poids fort)
            if token in self.index:
                weight = self.term_frequencies.get(token, 1.0)
                for idx in self.index[token]:
                    scored_indices[idx] += 10.0 * weight
                self.session_context.append(token)
            
            # Match Flou (Heuristique de proximité)
            elif len(token) > 3:
                for kw, indices in self.index.items():
                    if abs(len(kw) - len(token)) <= 1:
                        dist = self._levenshtein_distance(token, kw)
                        if dist == 1: # Une seule erreur
                            weight = self.term_frequencies.get(kw, 1.0)
                            for idx in indices:
                                scored_indices[idx] += 5.0 * weight

        # 2. Utilisation de la mémoire de session si la requête est pauvre
        if not scored_indices and self.session_context:
            for old_token in self.session_context:
                if old_token in self.index:
                    for idx in self.index[old_token]:
                        scored_indices[idx] += 2.0 # Poids faible car c'est du rappel

        # 3. Recherche par sous-chaîne (Fallback thématique)
        for kw, indices in self.index.items():
            if kw in normalized_input and len(kw) > 3:
                for idx in indices:
                    scored_indices[idx] += 3.0

        if not scored_indices:
            return ""

        # Tri par score décroissant
        sorted_results = sorted(scored_indices.items(), key=lambda x: x[1], reverse=True)
        
        final_contexts = []
        for idx, score in sorted_results[:3]: # Top 3
            raw_ctx = self.data[idx].get("context", "")
            final_contexts.append(self._clean_content_for_ai(raw_ctx))

        return "\n".join(list(dict.fromkeys(final_contexts)))