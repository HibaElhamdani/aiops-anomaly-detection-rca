"""
Generateur de charge Locust pour MaBanque API (Couche 1 AIOps).
Simule des clients bancaires concurrents sur les endpoints metier.

- Users (nombre d'utilisateurs)  : regle au lancement (UI ou CLI)
- Spawn Rate (vitesse creation) : regle au lancement (UI ou CLI)
- Wait Time (pause entre clics) : defini dans ce fichier
- Ordre et poids des endpoints  : definis dans ce fichier
"""

from locust import HttpUser, task, between
import random


class BanqueUser(HttpUser):
    """
    Un client bancaire virtuel.
    Chaque instance de cette classe = 1 utilisateur simule.
    """

    # Pause aleatoire entre 1 et 5 secondes apres chaque action.
    # -> between(0.1, 0.5) = charge tres agressive (robot)
    # -> between(3, 8)     = charge legere (humain lent)
    wait_time = between(1, 5)

    # Adresse de l'API cible (conteneur FastAPI publie sur le port 8000).
    # Peut etre surcharge au lancement :
    #   locust -f locustfile.py --host http://localhost:8000
    host = "http://localhost:8000"

    def on_start(self):
        """
        Execute UNE SEULE FOIS au demarrage de chaque utilisateur.
        Ordre impose : le client commence toujours par se connecter.
        """
        payload = {
            "username": f"client_{random.randint(1, 50)}",
            "password": "password123",
        }
        with self.client.post(
            "/login",
            json=payload,
            name="POST /login",
            catch_response=True,
        ) as reponse:
            # On marque succes meme si le login mock echoue,
            # pour ne pas bloquer le scenario de charge.
            reponse.success()

    @task(5)
    def consulter_solde(self):
        """
        Tache frequente (poids = 5).
        Consultation du solde.
        Impact metriques : CPU leger, reseau modere.
        """
        self.client.get("/solde", name="GET /solde")

    @task(3)
    def effectuer_virement(self):
        """
        Tache moyenne (poids = 3).
        Effectue un virement bancaire.
        Impact metriques : ecriture BDD -> disk_write_bytes_sec, CPU.
        """
        payload = {
            "montant": round(random.uniform(10.0, 500.0), 2),
            "destinataire": f"IBAN_FR{random.randint(1000, 9999)}",
            "libelle": "Virement Locust AIOps",
        }
        self.client.post(
            "/virement",
            json=payload,
            name="POST /virement",
        )

    @task(2)
    def voir_historique(self):
        """
        Tache plus rare (poids = 2).
        Consultation de l'historique des operations.
        Impact metriques : lecture BDD -> disk_read_bytes_sec, latence.
        """
        self.client.get("/historique", name="GET /historique")

    @task(1)
    def relogin_periodique(self):
        """
        Tache rare (poids = 1).
        Re-connexion ponctuelle pour varier le trafic d'authentification.
        """
        payload = {
            "username": f"client_{random.randint(1, 50)}",
            "password": "password123",
        }
        self.client.post(
            "/login",
            json=payload,
            name="POST /login (refresh)",
        )


class ChaosAdminUser(HttpUser):
    """
    Utilisateur special qui injecte du chaos PENDANT la charge.
    Sert a creer des scenarios de panne realistes pour le RCA.

    Attention :
    - wait_time long (10-20 s) pour ne pas spammer /chaos
    - a activer seulement lors des tests d'anomalies
    - en usage normal, ne lancer que BanqueUser
    """

    wait_time = between(10, 20)
    host = "http://localhost:8000"

    @task
    def injecter_chaos(self):
        """
        Declenche un mode chaos aleatoire via POST /chaos.
        Combine trafic Locust + panne = scenario AIOps complet.
        """
        type_chaos = random.choice([
            "error_500",     # erreurs HTTP 500 en rafale
            "slow",          # latence artificielle
            "timeout",       # blocage long (gateway timeout)
            "memory_leak",   # fuite memoire progressive
        ])
        payload = {"type": type_chaos}
        self.client.post(
            "/chaos",
            json=payload,
            name=f"POST /chaos [{type_chaos}]",
        )


# =============================================================================
# AIDE MEMOIRE - LANCEMENT
# =============================================================================
#
# 1) Installation (une seule fois) :
#       cd locust
#       pip install -r requirements.txt
#
# 2) Lancement avec interface web :
#       locust -f locustfile.py --host http://localhost:8000
#       Puis ouvrir http://localhost:8089
#       Regler : Number of users + Spawn rate -> Start swarming
#
# 3) Lancement en ligne de commande (sans UI) :
#       locust -f locustfile.py --host http://localhost:8000 \
#              --users 30 --spawn-rate 5 --run-time 3m --headless
#
# 4) Ce qui se regle OU :
#       AU LANCEMENT (UI/CLI)     |  DANS CE FICHIER
#       --------------------------+---------------------------
#       Users  = nb clients       |  wait_time = between(...)
#       Spawn  = vitesse creation |  @task(poids) + ordre
#                                 |  on_start() = 1er endpoint
#
# 5) Endpoints exclus (ne jamais les appeler ici) :
#       /health, /docs, /openapi.json, /redoc
#       (meme regle que le middleware de logs FastAPI)
#
# 6) Adapter les JSON (username, montant, type...) si services.py
#    attend des noms de champs differents.
# =============================================================================