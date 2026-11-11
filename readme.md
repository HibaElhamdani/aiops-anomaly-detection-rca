# AIOps : supervision quasi temps réel d'une application bancaire

Pipeline de données qui surveille une application bancaire et son infrastructure, détecte les anomalies en quasi temps réel et les stocke dans un Data Lakehouse (architecture Médaillon).

## Problématique

Une application bancaire doit rester disponible en permanence. Si elle tombe en panne ou devient très lente, les clients ne peuvent plus effectuer leurs opérations. Ce projet détecte rapidement ces situations en analysant en continu les **logs de l'application** et les **métriques de l'infrastructure**.



## Les composants

| Composant | Rôle |
|---|---|
| **FastAPI** | Application bancaire : connexion, consultation du solde, historique des transactions. Chaque requête génère un log (méthode, chemin, code de statut, latence, message d'erreur). |
| **SQLite** | Base de données du prototype : tables `clients`, `comptes`, `transactions`. Sans serveur, portable et légère. |
| **Locust** | Simulation du trafic : nombre d'utilisateurs configurable et poids par endpoint pour faire monter la latence sur certains d'entre eux. |
| **psutil** | Collecte des métriques de l'infrastructure (voir ci-dessous). |
| **Kafka** | Transport des données avec deux topics : `aiops-logs` et `aiops-metrics`. |
| **Spark Structured Streaming** | Traitement en continu, calcul des statistiques par fenêtre et détection des anomalies. |
| **Delta Lake** | Stockage ACID en trois couches : Bronze, Silver, Gold. |
| **Docker Compose** | Lancement de toute la stack avec une seule commande. |

### Métriques collectées (8)

CPU (%), RAM (%), utilisation du disque (%), réseau envoyé (Mo), réseau reçu (Mo), lecture disque (Mo), écriture disque (Mo), nombre de processus.

## Traitement Spark

1. **Lecture** des deux topics Kafka en continu.
2. **Bronze** : sauvegarde du message JSON brut, sans transformation (audit).
3. **Silver** : application d'un schéma (colonnes typées, vrai timestamp).
4. **Fenêtre glissante** de 5 minutes, recalculée toutes les 10 secondes, avec un watermark d'1 minute pour accepter les données en retard.
5. **Calcul** par métrique : moyenne, écart-type, maximum, puis Z-score.
6. **Décision** : règles d'anomalie (ci-dessous).
7. **Stockage** : Silver pour les calculs, Gold pour les incidents.

### Détection d'anomalies sur les métriques

Z-score du pic de la fenêtre : `Z = (max − moyenne) / écart-type` (mis à 0 si l'écart-type est nul).

| Métrique | Anomalie si |
|---|---|
| CPU | (Z > 3 **et** pic > 70 %) **ou** moyenne > 85 % |
| RAM | (Z > 3 **et** pic > 80 %) **ou** moyenne > 90 % |
| Disque | (Z > 3 **et** pic > 85 %) **ou** moyenne > 90 % |
| Réseau, lecture/écriture disque, processus | Z > 3 **et** pic > 2 × la moyenne |

Une colonne `is_anomaly` indique si au moins une métrique est en anomalie. Les seuils servent à limiter les fausses alertes d'un Z-score seul.

### Détection d'erreurs sur les logs

Un log est une erreur (`is_error`) si le **code de statut ≥ 400** ou si la **latence > 1000 ms**. Les erreurs sont copiées dans la couche Gold (incidents).

## Structure du projet

```
AIOPS/
├── apps/
│   ├── fastapi_app/          # application bancaire (FastAPI + SQLite)
│   ├── metrics_agent/        # collecte des métriques (psutil) vers Kafka
│   └── monitoring_exporter/  # export pour la visualisation
├── locust/                   # scénarios de charge
├── spark_processor/          # job Spark Structured Streaming
├── docker-compose.yml
└── README.md
```

## Lancer le projet

Prérequis : Docker et Docker Compose.

```bash
git clone https://github.com/HibaElhamdani/aiops-anomaly-detection-rca.git
cd aiops-anomaly-detection-rca
docker compose up -d
```

Pour arrêter :

```bash
docker compose down
```

Les données Delta Lake sont écrites dans le dossier `storage/` (ignoré par Git).



## Technologies

Python, FastAPI, SQLite, Locust, psutil, Apache Kafka, Apache Spark (Structured Streaming), Delta Lake, Docker, Docker Compose.

## Auteure

**Hiba El Hamdani** : élève-ingénieure en Informatique et Ingénierie des Données (IID), ENSA Khouribga.