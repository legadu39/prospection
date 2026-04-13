import subprocess
import time
import requests
import sys
import random
import json
import re
from pathlib import Path
from collections import deque

class MobileRotator:
    def __init__(self, adb_path="adb", max_history=5):
        """
        Contrôleur ADB Intelligent & Résilient.
        Intègre maintenant :
        - Fallback automatique de stratégie (Android récent vs ancien).
        - Redondance des services de vérification IP.
        - Smart Polling (attente active) pour réduire la latence.
        - Mémoire des IPs (court terme) pour éviter les cycles courts.
        - Blacklist Volatile (long terme) pour hygiène prédictive.
        - Quality Gate: Test de latence pour le trading (Ping).
        """
        self.adb_path = adb_path
        
        # Intelligence N°3 : Gestion de Contexte (Mémoire à court terme)
        self.ip_history = deque(maxlen=max_history)
        
        # Intelligence N°4 : Redondance des Services
        self.ip_providers = [
            "https://api.ipify.org",
            "https://ifconfig.me/ip",
            "https://checkip.amazonaws.com",
            "https://icanhazip.com",
            "https://api.myip.com"
        ]
        
        # État interne pour la stratégie ADB (Optimisation)
        self.working_strategy = None 

        # Intelligence N°5 : Blacklist Volatile (Long Terme)
        self.bad_ip_file = Path("bad_ips.json")
        self.bad_ips = self._load_bad_ips()

        # Intelligence N°6 : Score d'Hygiène IP (Resilience)
        # On démarre à 100%. Chaque requête baisse le score.
        self.current_ip_health = 100.0
        self.decay_rate = 2.5 # Perte de santé par action majeure

        # Vérification de la connexion physique au démarrage
        self._check_device_connection()

    def _load_bad_ips(self):
        """Charge la blacklist persistante et nettoie les entrées expirées."""
        if not self.bad_ip_file.exists():
            return {}
        try:
            with open(self.bad_ip_file, "r") as f:
                data = json.load(f)
            
            # Nettoyage automatique à l'initialisation
            now = time.time()
            clean_data = {ip: exp for ip, exp in data.items() if exp > now}
            return clean_data
        except Exception:
            return {}

    def _save_bad_ips(self):
        """Sauvegarde la blacklist sur disque."""
        try:
            with open(self.bad_ip_file, "w") as f:
                json.dump(self.bad_ips, f)
        except Exception as e:
            print(f"⚠️ Erreur sauvegarde blacklist IP: {e}")

    def report_bad_ip(self, ip: str, duration: int = 14400):
        """
        Signale une IP comme 'Sale' (Captcha loop, ban, etc.).
        Bannie par défaut pour 4 heures (14400s).
        """
        if ip and len(ip) > 7:
            expiration = time.time() + duration
            self.bad_ips[ip] = expiration
            self._save_bad_ips()
            print(f"⛔ IP {ip} ajoutée à la Blacklist (Exp: {int(duration/60)}min).")

    def notify_request_made(self):
        """
        Intelligence: Call this method whenever a heavy request is made.
        Degrades IP health to simulate usage fatigue.
        """
        self.current_ip_health -= self.decay_rate
        # Add random noise to avoid robotic patterns
        self.current_ip_health -= random.uniform(0.1, 1.0)
        
        if self.current_ip_health < 50:
            print(f"📉 Santé IP en baisse: {int(self.current_ip_health)}%")

    def check_preventive_rotation(self) -> bool:
        """
        Vérifie si une rotation proactive est nécessaire basée sur le score d'hygiène.
        A appeler AVANT de lancer une opération critique.
        """
        if self.current_ip_health < 20.0:
            print("🛡️ [RESILIENCE] Rotation Préventive déclenchée (Hygiène IP critique).")
            return self.rotate_ip()
        return False

    def _run_cmd(self, command):
        """Exécute une commande ADB shell avec capture sécurisée."""
        full_cmd = f"{self.adb_path} shell {command}"
        try:
            result = subprocess.run(
                full_cmd, 
                shell=True, 
                capture_output=True, 
                text=True, 
                timeout=10
            )
            return result.stdout.strip()
        except subprocess.TimeoutExpired:
            print("⚠️ Timeout ADB.")
            return ""
        except Exception as e:
            print(f"⚠️ Erreur ADB: {e}")
            return ""

    def _check_device_connection(self):
        """Vérifie si un appareil est connecté via ADB."""
        try:
            result = subprocess.run(f"{self.adb_path} devices", shell=True, capture_output=True, text=True)
            if "device\n" not in result.stdout and "device\r" not in result.stdout:
                print("🚨 AUCUN APPAREIL DÉTECTÉ ! Vérifiez le câble USB et le mode Debug.")
                # On ne quitte pas, on espère un branchement à chaud
            else:
                print("📱 Appareil connecté et prêt.")
        except Exception:
            print("🚨 ADB introuvable dans le PATH.")

    def _check_latency(self) -> bool:
        """
        INTELLIGENCE : Quality Gate.
        Vérifie si l'IP, bien que valide, est assez rapide pour le trading/sniping.
        Seuil : < 300ms vers DNS Google.
        """
        try:
            # Ping simple vers Google DNS (universel)
            # -c 1 (count), -W 2 (timeout 2s)
            cmd = f"{self.adb_path} shell ping -c 1 -W 2 8.8.8.8"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if "0 packets received" in result.stdout:
                return False

            # Extraction du temps (time=XX ms)
            match = re.search(r"time=(\d+(\.\d+)?)", result.stdout)
            if match:
                ms = float(match.group(1))
                if ms > 300:
                    print(f"⚠️ [LATENCE] Ping trop élevé ({ms}ms). IP instable pour le trading.")
                    return False
                return True
            return True # Fallback si pas de regex match mais ping ok
        except Exception:
            return False

    def get_current_ip(self):
        """Récupère l'IP actuelle via API externe avec redondance."""
        # Mélange pour ne pas spammer toujours le même provider
        providers = self.ip_providers.copy()
        random.shuffle(providers)
        
        for url in providers:
            try:
                # Timeout court pour rapidité
                response = requests.get(url, timeout=3)
                if response.status_code == 200:
                    ip = response.text.strip()
                    # Validation basique format IP
                    if len(ip) >= 7 and "." in ip:
                        return ip
            except:
                continue
        return None

    def _toggle_airplane_mode(self, enable):
        """Active ou désactive le mode avion via ADB."""
        state = "1" if enable else "0"
        
        # Stratégie 1 : settings put (Android standard)
        if self.working_strategy is None or self.working_strategy == "settings":
            self._run_cmd(f"settings put global airplane_mode_on {state}")
            self._run_cmd(f"am broadcast -a android.intent.action.AIRPLANE_MODE --ez state {state.lower() == '1'}")
        
        # Stratégie 2 : cmd connectivity (Android récents / Root)
        if self.working_strategy is None or self.working_strategy == "cmd":
            cmd_state = "enable" if enable else "disable"
            self._run_cmd(f"cmd connectivity airplane-mode {cmd_state}")
            
        # Stratégie 3 : input tap (Bourrin - à implémenter si besoin via coordonnées)
        pass

    def rotate_ip(self):
        """
        Cycle complet de rotation d'IP.
        Retourne True si nouvelle IP obtenue, False sinon.
        """
        print("🔄 [ROTATION] Lancement du cycle de changement d'IP...")
        
        old_ip = self.get_current_ip()
        if not old_ip:
            print("⚠️ Impossible de récupérer l'IP actuelle. On tente quand même la rotation.")
            old_ip = "unknown"

        # Tentative de rotation
        max_retries = 3
        for i in range(max_retries):
            # 1. Mode Avion ON
            self._toggle_airplane_mode(True)
            time.sleep(1.5) # Laisser le temps au modem de couper
            
            # 2. Mode Avion OFF
            self._toggle_airplane_mode(False)
            
            # 3. Attente active (Smart Polling)
            # On check toutes les 1s si le réseau est revenu
            network_back = False
            for _ in range(15): # Max 15 secondes
                if self.get_current_ip(): # Si on a une IP, le réseau est là
                    network_back = True
                    break
                time.sleep(1)
            
            if not network_back:
                print("⚠️ Réseau lent à revenir. Nouvelle tentative...")
                continue

            # 4. Vérification de la nouvelle IP
            new_ip = self.get_current_ip()
            
            if not new_ip:
                print("⚠️ Pas d'IP visible malgré le réseau mobile.")
                continue
                
            if new_ip == old_ip:
                print(f"⚠️ [MÊME IP] L'opérateur a renvoyé la même IP ({new_ip}). Nouvelle tentative...")
                continue
                
            # Intelligence N°3 : Détection de Cycle (IP déjà vue récemment)
            if new_ip in self.ip_history:
                print(f"♻️ [CYCLE DÉTECTÉ] IP {new_ip} déjà utilisée récemment. On force une nouvelle rotation.")
                continue
            
            # Intelligence N°5 : Vérification Blacklist Long Terme
            if new_ip in self.bad_ips:
                if time.time() < self.bad_ips[new_ip]:
                    print(f"⛔ [BLACKLIST] IP {new_ip} connue comme 'Sale'. Rejet immédiat.")
                    continue
                else:
                    # Expiration du ban
                    del self.bad_ips[new_ip]
                    self._save_bad_ips()

            # Intelligence N°7 : Quality Gate (Latence)
            if not self._check_latency():
                print(f"🐢 [LATENCE] IP {new_ip} fonctionnelle mais trop lente. On la jette.")
                # On ajoute une punition courte (5 min) pour ne pas la reprendre tout de suite
                self.report_bad_ip(new_ip, duration=300) 
                continue

            # --- SUCCÈS ---
            self.ip_history.append(new_ip)
            # RESET HEALTH SCORE
            self.current_ip_health = 100.0
            print(f"✅ [SUCCÈS] Nouvelle IP Fraîche : {new_ip} (Santé: 100%)")
            return True

        print("🚨 [FATAL] Impossible d'obtenir une nouvelle IP unique et rapide après plusieurs essais.")
        return False

if __name__ == "__main__":
    rotator = MobileRotator()
    rotator.rotate_ip()