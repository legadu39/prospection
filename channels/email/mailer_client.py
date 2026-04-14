import os
import time
import random
import requests
from loguru import logger

try:
    from core.settings import settings
except ImportError:
    settings = None  # fallback: use hardcoded default

# Importation de l'architecture Core pour la persistance et le contexte
try:
    from core.database import NexusDB
except ImportError:
    # Fallback si exécuté hors du contexte global (pour tests unitaires isolés)
    logger.warning("NexusDB non trouve. Mode 'Stateless' active (pas de memoire).")
    NexusDB = None

# Configuration API
API_URL = "https://api.brevo.com/v3/smtp/email"


class NurturingBot:
    def __init__(self):
        api_key = os.getenv("MAILER_API_KEY", "").strip()
        if not api_key:
            logger.critical(
                "MAILER_API_KEY absente ou vide. "
                "Ajoutez MAILER_API_KEY=<votre_cle_brevo> dans le fichier .env. "
                "Arret de NurturingBot."
            )
            raise EnvironmentError(
                "MAILER_API_KEY manquante — NurturingBot ne peut pas demarrer sans cle Brevo."
            )
        self.headers = {
            "accept": "application/json",
            "api-key": api_key,
            "content-type": "application/json",
        }
        # Connexion à la mémoire du projet (State Management)
        self.db = NexusDB() if NexusDB else None

    def _check_pressure(self, user_email: str, hours: int = 24) -> bool:
        """
        Garde-fou (Contexte) : Vérifie si l'utilisateur a déjà été contacté récemment.
        Retourne True si on peut envoyer, False si on doit s'abstenir (Cooling off).
        """
        if not self.db:
            return True  # Pas de DB, on envoie par défaut

        try:
            # On utilise le hachage GDPR de NexusDB pour vérifier l'historique
            # sans stocker l'email en clair si ce n'est pas nécessaire ailleurs.
            author_hash = self.db._hash_identity(user_email)
            cutoff = time.time() - (hours * 3600)

            with self.db.session() as conn:
                # On regarde dans les logs de dispatch liés à cet "auteur" (lead)
                # Note: Cette requête suppose que le lien entre lead et email est maintenu via le hash auteur
                res = conn.execute(
                    """
                    SELECT count(*) as cnt 
                    FROM leads 
                    WHERE author = ? AND updated_at > ? AND status = 'SENT'
                """,
                    (author_hash, cutoff),
                ).fetchone()

                count = res["cnt"] if hasattr(res, "keys") else res[0]

                if count > 0:
                    logger.info(
                        f"⏳ Pression Marketing: {user_email} déjà contacté il y a moins de {hours}h. Skip."
                    )
                    return False
                return True
        except Exception as e:
            logger.warning(f"⚠️ Erreur vérification pression marketing: {e}. On autorise l'envoi.")
            return True

    def _predict_optimal_send_time(self, email: str) -> str:
        """
        Anticipation : Devine le meilleur moment pour envoyer selon le domaine.
        """
        domain = email.split("@")[-1].lower() if "@" in email else ""

        # Heuristique B2C (Emails gratuits) -> Mieux le soir ou pause déj
        free_providers = [
            "gmail.com",
            "yahoo.com",
            "hotmail.com",
            "outlook.com",
            "orange.fr",
            "wanadoo.fr",
        ]

        if domain in free_providers:
            return "B2C_PERSONAL"
        else:
            return "B2B_PROFESSIONAL"

    def _get_dynamic_template(self, product_key: str, next_offer: str, user_context: str) -> tuple:
        """
        Automatisation : Génère un contenu dynamique et A/B teste les objets.
        """
        # A/B Testing des objets
        subjects_pool = []
        if product_key == "PROP_FIRM":
            subjects_pool = [
                "🚀 Tu as ton compte Prop Firm ? Il te manque ça...",
                "⚠️ 80% des traders échouent à cause de ça",
                "L'outil secret pour valider ton challenge",
            ]
        elif product_key == "CRYPTO_WALLET":
            subjects_pool = [
                "🔒 Ta Ledger est sécurisée ?",
                "Ne fais jamais ça avec ta clé privée...",
                "Sécurité Crypto : Le détail oublié",
            ]
        else:
            subjects_pool = [f"Une opportunité pour toi concernant {next_offer}"]

        subject = random.choice(subjects_pool)

        # Personnalisation du ton selon le contexte
        greeting = "Salut !" if user_context == "B2C_PERSONAL" else "Bonjour,"
        tone_style = (
            "color: #4CAF50;" if user_context == "B2C_PERSONAL" else "color: #2E86C1;"
        )  # Vert vs Bleu Pro

        html_template = f"""
        <html><body style="font-family: Arial, sans-serif; line-height: 1.6;">
            <h2 style="{tone_style}">{greeting} Bravo pour ton passage à l'action !</h2>
            <p>J'ai vu que tu t'intéressais à <strong>{product_key}</strong>.</p>
            
            <div style="background-color: #f8f9fa; padding: 15px; border-left: 4px solid #ccc; margin: 20px 0;">
                <p><strong>Le constat :</strong> Beaucoup sous-estiment l'importance de l'outillage.</p>
                <p>Pour passer au niveau supérieur, voici ce que les pros utilisent : <strong>{next_offer}</strong>.</p>
            </div>
            
            <center>
                <a href='{{LINK}}' style='background-color: #4CAF50; color: white; padding: 12px 24px; text-decoration: none; border-radius: 4px; font-weight: bold;'>👉 Découvrir l'offre exclusive</a>
            </center>
            
            <p style="font-size: 0.8em; color: #666; margin-top: 30px;">
                PS: C'est un conseil basé sur notre analyse technique, pas un conseil financier.
            </p>
        </body></html>
        """
        return subject, html_template

    def _dispatch_email(self, to_email, subject, html_content, tags=None):
        """
        Résilience : Envoi avec Retry Policy (Exponential Backoff) et Logging DB.
        """
        payload = {
            "sender": {"name": "Nexus Assistant", "email": "contact@ton-domaine.com"},
            "to": [{"email": to_email}],
            "subject": subject,
            "htmlContent": html_content,
            "tags": tags or ["automation"],
        }

        max_retries = 3
        base_delay = 2  # Secondes

        for attempt in range(max_retries):
            try:
                brevo_timeout = settings.BREVO_TIMEOUT if settings is not None else 10
                response = requests.post(
                    API_URL, json=payload, headers=self.headers, timeout=brevo_timeout
                )

                # Succès (200-299)
                if 200 <= response.status_code < 300:
                    logger.success(f"📧 Mail envoyé à {to_email} (Tentative {attempt+1})")

                    # Persistance de l'événement dans NexusDB
                    if self.db:
                        try:
                            # On loggue l'action pour le context future (Anti-Spam)
                            # On utilise l'ID haché pour rester cohérent avec la table leads
                            self.db.insert_raw_lead(
                                {
                                    "id": f"MAIL_{int(time.time())}_{attempt}",
                                    "author": to_email,  # Sera haché par insert_raw_lead
                                    "source": "EMAIL_OUTBOUND",
                                    "comment_text": f"Subject: {subject}",
                                    "status": "SENT",
                                }
                            )
                        except Exception as db_e:
                            logger.error(f"⚠️ Echec log DB: {db_e}")

                    return True

                # Erreur Client (4xx) - Inutile de réessayer
                if 400 <= response.status_code < 500:
                    logger.error(f"❌ Erreur Client (Pas de retry): {response.text}")
                    return False

                # Erreur Serveur (5xx) - On lève une exception pour déclencher le retry
                response.raise_for_status()

            except Exception as e:
                wait_time = base_delay * (2**attempt)
                logger.warning(
                    f"⚠️ Échec envoi (Tentative {attempt+1}/{max_retries}). Pause {wait_time}s... Erreur: {e}"
                )
                time.sleep(wait_time)

        logger.error(f"❌ Abandon définitif pour {to_email}")
        return False

    def send_cross_sell_sequence(self, user_email, first_product_bought):
        """
        Déclenche la séquence intelligente : Analyse -> Prédiction -> Construction -> Envoi
        """
        # 1. Gestion de Contexte (Anti-Spam)
        if not self._check_pressure(user_email):
            return

        # 2. Logique de recommandation
        next_offer = ""
        affiliate_link = ""

        if first_product_bought == "PROP_FIRM":
            next_offer = "TradingView Pro"
            affiliate_link = "https://fr.tradingview.com/pricing/?share_your_id=ton_id"
        elif first_product_bought == "CRYPTO_WALLET":
            next_offer = "NordVPN"
            affiliate_link = "https://nordvpn.com/ton_lien"
        else:
            logger.info(f"ℹ️ Pas de séquence définie pour {first_product_bought}")
            return

        # 3. Anticipation (Profilage)
        user_profile = self._predict_optimal_send_time(user_email)

        # 4. Automatisation (Template Dynamique)
        subject, raw_html = self._get_dynamic_template(
            first_product_bought, next_offer, user_profile
        )
        final_html = raw_html.replace("{{LINK}}", affiliate_link)

        # 5. Envoi Résilient
        self._dispatch_email(
            user_email, subject, final_html, tags=["cross-sell", first_product_bought]
        )

    def send_referral_request(self, user_email, user_referral_code):
        """
        Le script pour transformer le client en ambassadeur (Viralité)
        """
        # 1. Gestion de Contexte
        if not self._check_pressure(user_email, hours=48):  # Moins agressif sur le referral
            return

        subject = "🎁 Un cadeau pour toi (et tes amis)"

        # Template simple (gardé statique pour l'instant car performant)
        html_content = f"""
        <html><body>
            <p>Salut !</p>
            <p>Tu veux obtenir notre PDF 'Les 10 secrets du Scalping' (Valeur 49€) gratuitement ?</p>
            <p>C'est simple : Partage ton lien personnel ci-dessous à 1 ami trader.</p>
            <div style='border: 1px dashed black; padding: 10px; background: #f9f9f9; margin: 15px 0;'>
                <strong>Ton lien magique :</strong><br>
                <a href="https://nexus-global.com/ref/{user_referral_code}">https://nexus-global.com/ref/{user_referral_code}</a>
            </div>
            <p>Dès qu'il clique, tu reçois le PDF.</p>
        </body></html>
        """

        self._dispatch_email(user_email, subject, html_content, tags=["referral"])
