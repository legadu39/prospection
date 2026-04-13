### check_links.py
import json
import sys
import os
import requests
from pathlib import Path
from urllib.parse import urlparse

# CONFIGURATION DU CHEMIN
CONFIG_PATH = Path("config/sponsors.json")

# --- INTELLIGENCE N°4 : AUDIT AUTOMATISÉ ---
def auto_audit_link(url: str, expected_code: str) -> tuple[bool, str]:
    """
    Vérifie programmatiquement si le lien redirige correctement et contient le code d'affiliation.
    V3 Update: Content Verification pour éviter les Soft 404.
    Retourne (Succès, Message).
    """
    if not url or not url.startswith("http"):
        return False, "URL Invalide"

    print(f"   🤖 Audit Automatique en cours pour : {url[:50]}...")
    
    try:
        # Simulation d'un navigateur standard pour éviter les blocages anti-bot basiques
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        # Suivi des redirections (allow_redirects=True)
        resp = requests.get(url, headers=headers, allow_redirects=True, timeout=10)
        final_url = resp.url
        history = [r.url for r in resp.history]
        page_content = resp.text.lower()
        
        # Analyse de la réponse finale
        if resp.status_code >= 400:
            return False, f"Erreur HTTP {resp.status_code}"

        # 0. V3 CONTENT VERIFICATION (Protection anti Soft-404)
        positive_signals = ["sign up", "register", "inscription", "créer un compte", "start", "join", "get started", "log in", "connexion"]
        negative_signals = ["not found", "404", "error", "page introuvable", "maintenance", "coming soon", "site en construction"]
        
        has_positive = any(sig in page_content for sig in positive_signals)
        has_negative = any(sig in page_content for sig in negative_signals)
        
        if has_negative and not has_positive:
            return False, "⚠️ SOFT 404 DÉTECTÉE : Page d'erreur ou maintenance partenaire."

        if not has_positive and len(page_content) < 500:
            return False, "⚠️ CONTENU SUSPECT : Page vide ou très courte."

        # 1. Vérification dans l'URL finale (Paramètres GET)
        if expected_code and str(expected_code).lower() in final_url.lower():
            return True, f"Code '{expected_code}' détecté dans l'URL finale."
            
        # 2. Vérification dans l'historique de redirection (Souvent le code est strippé à la fin)
        for hop in history:
            if expected_code and str(expected_code).lower() in hop.lower():
                return True, f"Code '{expected_code}' détecté dans la chaîne de redirection."

        # 3. Vérification des Cookies (Cas complexes)
        for cookie in resp.cookies:
            if expected_code and str(expected_code) in str(cookie.value):
                return True, f"Code '{expected_code}' détecté dans les Cookies."

        # Si on arrive ici, l'auto-détection a échoué mais le lien fonctionne
        return False, f"Lien accessible ({final_url}), mais code '{expected_code}' non visible."

    except requests.Timeout:
        return False, "Timeout (10s) - Le serveur est lent ou bloque."
    except requests.ConnectionError:
        return False, "Erreur de connexion (DNS/Réseau)."
    except Exception as e:
        return False, f"Erreur technique : {e}"

def check_cash_flow():
    print("\n🕵️  INITIALISATION DE L'AUDIT FINANCIER (NEXUS V3.0 - SMART MODE)")
    print("=" * 60)

    # 1. Vérification de l'existence du fichier
    if not CONFIG_PATH.exists():
        # Fallback pour test local
        fallback_path = Path("prospection/config/sponsors.json")
        if fallback_path.exists():
            target_path = fallback_path
        else:
            print(f"❌ CRITIQUE : Fichier de configuration introuvable !")
            print(f"   Attendu : {CONFIG_PATH.absolute()}")
            sys.exit(1)
    else:
        target_path = CONFIG_PATH

    print(f"📂 Fichier chargé : {target_path}")

    # 2. Chargement du JSON
    try:
        with open(target_path, 'r', encoding='utf-8') as f:
            sponsors = json.load(f)
            
        # Support pour la structure { "mandates": [...] } ou [...] directe
        if isinstance(sponsors, dict) and "mandates" in sponsors:
            # Si c'est un dict mandates, on peut avoir une liste ou un dict de dicts
            raw_mandates = sponsors["mandates"]
            if isinstance(raw_mandates, dict):
                sponsor_list = list(raw_mandates.values())
            else:
                sponsor_list = raw_mandates
        elif isinstance(sponsors, list):
            sponsor_list = sponsors
        else:
            sponsor_list = []
            
    except json.JSONDecodeError as e:
        print(f"❌ ERREUR SYNTAXE JSON : {e}")
        sys.exit(1)

    active_count = 0
    errors = 0
    warnings = 0

    print("\n💰 VÉRIFICATION DES MANDATS (FLUX D'ARGENT)")
    print("-" * 60)

    for s in sponsor_list:
        # On ne vérifie que les campagnes actives
        if not s.get('active'):
            continue
            
        active_count += 1
        program = s.get('program', s.get('sponsor_name', 'INCONNU')).upper()
        label = s.get('label', s.get('offer_type', 'Sans label'))
        ref_link = s.get('target_url') or s.get('ref_link', '')
        # Adaptation : Parfois ref_code n'est pas dans le JSON mais dans l'URL. 
        # On essaie d'extraire un paramètre "ref=" ou "aff=" si ref_code manquant.
        ref_code = s.get('ref_code', 'N/A')

        print(f"\n🔹 PROGRAMME : {program}")
        print(f"   Label     : {label}")
        print(f"   Lien REF  : {ref_link}")
        print(f"   Code REF  : {ref_code}")

        # Détection automatique des placeholders oubliés
        suspicious_terms = ["VOTRECODE", "VOTREREF", "exemple", "refnocode", "partner_link", "NEXUS_GLOBAL_DEFAULT"]
        is_suspicious = any(term in ref_link for term in suspicious_terms) or \
                        any(term in str(ref_code) for term in suspicious_terms)

        if is_suspicious:
            print("   ⚠️  ALERTE : Ce lien contient des termes par défaut !")
            warnings += 1
        
        # --- INTELLIGENCE : AUDIT AUTOMATIQUE ---
        # Si un ref_code est défini et n'est pas N/A, on tente l'auto-validation
        auto_valid = False
        if ref_code and ref_code != "N/A" and "http" in ref_link:
            success, msg = auto_audit_link(ref_link, ref_code)
            if success:
                print(f"   ✅ AUTO-VALIDATION RÉUSSIE : {msg}")
                auto_valid = True
            else:
                print(f"   🤔 Auto-audit incertain : {msg}")
        
        # Si auto-validé, on skip la question humaine sauf si suspect
        if auto_valid and not is_suspicious:
            continue

        # Validation humaine (Fallback)
        while True:
            response = input("   👉 Confirmez-vous que ce lien crédite VOTRE compte ? (y/n) : ").strip().lower()
            if response == 'y':
                print("   ✅ Validé manuellement.")
                break
            elif response == 'n':
                print("   ❌ MARQUÉ COMME ERREUR.")
                errors += 1
                break
            # Si autre touche, on repose la question

    print("\n" + "=" * 60)
    print("RÉSULTAT DE L'AUDIT DE DÉPLOIEMENT :")
    
    if errors > 0:
        print(f"🔴 ÉCHEC : {errors} liens d'affiliation ont été rejetés par l'opérateur.")
        print("   ACTION : Modifiez config/sponsors.json et relancez ce script.")
        sys.exit(1)
    
    if warnings > 0:
        print(f"🟠 ATTENTION : {warnings} liens suspects détectés mais validés manuellement.")
    
    if active_count == 0:
        print("🔴 ERREUR : Aucun programme actif trouvé !")
        sys.exit(1)

    print(f"🟢 SUCCÈS : {active_count} mandats vérifiés et prêts pour encaissement.")
    print("🚀 FEU VERT POUR DÉPLOIEMENT.")
    sys.exit(0)

if __name__ == "__main__":
    try:
        check_cash_flow()
    except KeyboardInterrupt:
        print("\n\n🛑 Audit interrompu par l'utilisateur.")
        sys.exit(1)