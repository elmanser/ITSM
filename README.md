# ITSM Intelligence Platform
### Plateforme Intelligente de Gestion des Incidents — PFE GTR 5ème Année
**ENSA Fès · 2025–2026**

---

## Table des Matières

1. [Vue d'ensemble](#1-vue-densemble)
2. [Architecture Système](#2-architecture-système)
3. [Stack Technologique](#3-stack-technologique)
4. [Structure du Projet](#4-structure-du-projet)
5. [Démarrage Rapide](#5-démarrage-rapide)
6. [Services et Accès](#6-services-et-accès)
7. [Pipeline Streaming Temps Réel](#7-pipeline-streaming-temps-réel)
8. [Pipeline Batch (Lambda)](#8-pipeline-batch-lambda)
9. [Modèle Machine Learning](#9-modèle-machine-learning)
10. [API REST FastAPI](#10-api-rest-fastapi)
11. [Dashboard Streamlit](#11-dashboard-streamlit)
12. [Monitoring — Prometheus & Grafana](#12-monitoring--prometheus--grafana)
13. [Orchestration — Apache Airflow](#13-orchestration--apache-airflow)
14. [Base de Données — Schéma DW](#14-base-de-données--schéma-dw)
15. [Tests Unitaires](#15-tests-unitaires)
16. [Configuration GLPI (Optionnel)](#16-configuration-glpi-optionnel)
17. [Ce qui manque / Améliorations futures](#17-ce-qui-manque--améliorations-futures)

---

## 1. Vue d'ensemble

Ce projet implémente une **plateforme AIOps complète** pour la gestion intelligente des tickets ITSM. Il combine :

- **Architecture Lambda hybride** : pipeline temps réel (Kafka) + pipeline batch (Airflow + ETL)
- **Machine Learning** : classification automatique de la priorité des tickets + prédiction du MTTR
- **Monitoring professionnel** : Prometheus + Grafana avec dashboard auto-provisionné
- **Interface avancée** : Dashboard Streamlit glassmorphique multi-pages

### Flux général
```
GLPI REST API (ou Mock Producer)
        │
        ▼ (poll toutes les 30s)
  Kafka Producer ──────────────► Kafka Topic [itsm.tickets.raw]
                                          │
                                          ▼
                                  Kafka Consumer
                                    (enrichissement)
                                          │
                                          ▼
                             ┌─── PostgreSQL Data Warehouse ───┐
                             │   (schéma étoile : fact_tickets) │
                             └───────────────┬─────────────────┘
                                             │
                    ┌────────────────────────┼─────────────────────┐
                    ▼                        ▼                     ▼
              FastAPI /predict        Streamlit              Airflow
              (ML + rate limit)       Dashboard              DAGs ETL/ML
                    ▲                        │
                    │                        │
             ML Model Bundle         Prometheus
          (RandomForest/XGBoost)     + Grafana
```

---

## 2. Architecture Système

### Architecture Lambda (Hybride)

| Couche | Composant | Rôle |
|--------|-----------|------|
| **Ingestion temps réel** | GLPI → Kafka Producer | Poll REST API toutes les 30s, publie dans Kafka |
| **Transport** | Apache Kafka | File de messages durables, découplage producteur/consommateur |
| **Consommation** | Kafka Consumer | Enrichissement (MTTR, SLA), écriture DW |
| **Batch** | Airflow + ETL Python | ETL quotidien, recalcul KPIs, qualité données |
| **Stockage** | PostgreSQL Data Warehouse | Schéma étoile (dimensions + faits) |
| **ML** | Scikit-learn / XGBoost | Classification priorité + régression MTTR |
| **Serving** | FastAPI | API REST prédiction, rate-limited, Prometheus |
| **Visualisation** | Streamlit | Dashboard 6 pages, glassmorphisme |
| **Monitoring** | Prometheus + Grafana | Métriques API, latence, usage mémoire/CPU |
| **Orchestration** | Apache Airflow | DAGs ETL quotidien + retraining ML hebdomadaire |

### Décisions d'architecture

- **Kafka** plutôt qu'un appel direct DB : découplage, durabilité, replay possible
- **Schéma étoile** : optimisé pour les requêtes analytiques ITSM
- **FastAPI** : async, validation Pydantic, intégration Prometheus native
- **Blended training** : données synthétiques corrélées + données réelles pour le ML
- **Containerisation complète** : 13 services Docker Compose, réseau isolé

---

## 3. Stack Technologique

| Catégorie | Technologie | Version |
|-----------|-------------|---------|
| **Messaging** | Apache Kafka | 7.4 (Confluent) |
| **Base de données** | PostgreSQL | 15 |
| **ITSM** | GLPI | 10.x |
| **ML** | Scikit-learn, XGBoost | 1.4 / 2.0 |
| **API** | FastAPI + Uvicorn | 0.109 / 0.27 |
| **Dashboard** | Streamlit | 1.32 |
| **Orchestration** | Apache Airflow | 2.x |
| **Monitoring** | Prometheus + Grafana | latest |
| **Conteneurisation** | Docker + Compose | 24+ |
| **Langage** | Python | 3.11 |
| **Rate Limiting** | slowapi | 0.1.9 |
| **Instrumentation** | prometheus-fastapi-instrumentator | 6.1.0 |

---

## 4. Structure du Projet

```
ITSM PROJECT/
│
├── docker-compose.yml              # Orchestration des 13 services
├── .env                            # Variables d'environnement (credentials)
├── README.md                       # Cette documentation
│
├── streaming/
│   ├── pipeline/
│   │   ├── producer/
│   │   │   ├── glpi_producer.py    # Producteur Kafka (GLPI → Kafka)
│   │   │   ├── Dockerfile
│   │   │   └── requirements.txt
│   │   └── consumer/
│   │       ├── kafka_consumer.py   # Consommateur Kafka (Kafka → PostgreSQL DW)
│   │       ├── Dockerfile
│   │       └── requirements.txt
│   │
│   ├── api/
│   │   ├── main.py                 # FastAPI : /predict /health /metadata /retrain
│   │   ├── retrain.py              # Retraining ML en arrière-plan
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   │
│   ├── ml/
│   │   ├── train.py                # Pipeline ML v2 (feature eng. + CV)
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   │
│   ├── dashboard/
│   │   ├── app.py                  # Dashboard Streamlit 6 pages
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   │
│   ├── db/
│   │   └── init.sql                # Schéma PostgreSQL DW (DDL complet)
│   │
│   └── scripts/
│       ├── test_glpi_api.py        # Test connexion GLPI API
│       └── mock_kafka_producer.py  # Producteur mock standalone
│
├── batch/
│   └── src/
│       └── batch/
│           ├── run_batch_pipeline.py   # Orchestrateur batch principal
│           ├── ingestion.py            # Chargement données sources
│           ├── cleaning.py             # Nettoyage et déduplication
│           ├── transformation.py       # Transformations métier
│           ├── load_to_postgres.py     # Écriture DW
│           ├── kpi_calculation.py      # Calcul KPIs ITSM
│           ├── quality_checks.py       # Contrôles qualité données
│           └── ml_dataset_builder.py   # Construction dataset ML
│
├── airflow/
│   └── dags/
│       ├── dag_etl.py              # ETL quotidien (00:30 UTC)
│       ├── dag_api_ingest.py       # Ingestion API (toutes les 5min)
│       ├── batch_itsm_pipeline_dag.py  # Pipeline batch complet
│       └── dag_ml_retrain.py       # Retraining ML (Dim. 02:00 UTC)
│
├── monitoring/
│   ├── prometheus.yml              # Config scraping Prometheus
│   └── grafana/
│       ├── provisioning/
│       │   ├── datasources/
│       │   │   └── datasource.yml  # Auto-provision datasource Prometheus
│       │   └── dashboards/
│       │       └── dashboard.yml   # Auto-provision dashboard ITSM
│       └── dashboards/
│           └── itsm_platform.json  # Dashboard Grafana 15 panels
│
└── tests/
    ├── conftest.py                 # Fixtures pytest + sys.path
    ├── requirements-test.txt
    └── unit/
        ├── test_consumer.py        # Tests consommateur Kafka
        ├── test_producer.py        # Tests producteur GLPI
        ├── test_ml.py              # Tests pipeline ML
        └── test_api.py             # Tests endpoints FastAPI
```

---

## 5. Démarrage Rapide

### Prérequis

- Docker Desktop ≥ 24 avec Docker Compose V2
- 8 GB RAM minimum (recommandé 12 GB)
- Ports libres : 8080, 8090, 8501, 8000, 9090, 3000, 5432, 8081

### Lancement complet

```bash
# 1. Cloner / accéder au répertoire
cd "ITSM PROJECT"

# 2. Démarrer tous les services
docker compose up -d

# 3. Attendre ~2 minutes que tous les services soient healthy
docker compose ps

# 4. Entraîner le modèle ML (première fois)
docker compose --profile training run --rm ml-trainer

# 5. Accéder aux interfaces (voir section 6)
```

### Ordre de démarrage automatique
```
ZooKeeper → Kafka → MariaDB → PostgreSQL (healthy)
         → GLPI → Kafka-UI
         → Producer → Consumer → API → Dashboard
         → Airflow → Prometheus → Grafana
```

### Arrêt propre

```bash
# Arrêter sans supprimer les volumes
docker compose stop

# Arrêter et supprimer tout (y compris données)
docker compose down -v
```

---

## 6. Services et Accès

| Service | URL | Identifiants | Description |
|---------|-----|--------------|-------------|
| **Streamlit Dashboard** | http://localhost:8501 | — | Dashboard principal 6 pages |
| **FastAPI (Swagger)** | http://localhost:8000/docs | — | API ML avec documentation interactive |
| **FastAPI (Métriques)** | http://localhost:8000/metrics | — | Métriques Prometheus |
| **GLPI** | http://localhost:8080 | `glpi` / `glpi` | Plateforme ITSM |
| **Kafka UI** | http://localhost:8090 | — | Interface monitoring Kafka |
| **Apache Airflow** | http://localhost:8081 | `admin` / `admin` | Orchestrateur DAGs |
| **Prometheus** | http://localhost:9090 | — | Base de métriques |
| **Grafana** | http://localhost:3000 | `admin` / `itsm_grafana_2026` | Dashboards monitoring |
| **PostgreSQL DW** | localhost:5432 | `itsm` / `itsm_dw_secret_2026` | Data Warehouse |
| **MariaDB (GLPI DB)** | — (interne) | `glpi` / `glpi_secret_2026` | Base GLPI |

---

## 7. Pipeline Streaming Temps Réel

### Producteur (`streaming/pipeline/producer/glpi_producer.py`)

**Mode GLPI** (si tokens configurés) :
- Poll l'API REST GLPI toutes les 30 secondes
- Récupère les nouveaux tickets (pagination par ID)
- Publie dans le topic Kafka `itsm.tickets.raw`

**Mode Mock** (sans tokens — par défaut) :
- Génère 1 à 4 tickets synthétiques toutes les 30 secondes
- Urgence et impact **corrélés** avec la priorité (pour meilleur signal ML) :

| Priorité GLPI | Urgence typique | Impact typique | MTTR moyen |
|---------------|-----------------|----------------|------------|
| 1 — Very High | 4–5 | 4–5 | ~3h |
| 2 — High | 3–4 | 3–4 | ~7h |
| 3 — Medium | 2–3 | 2–3 | ~20h |
| 4 — Low | 1–2 | 1–2 | ~50h |
| 5 — Very Low | 1 | 1 | ~100h |

- SLA réaliste : 70% tickets résolus dans les délais, 30% en dépassement

### Message Kafka (format JSON)
```json
{
  "event_type": "ticket_ingested",
  "source": "glpi_api",
  "ingested_at": "2026-05-17T18:30:00Z",
  "data": {
    "id": 2156,
    "name": "[MOCK] Incident #2156 — NETWORK",
    "priority": 1,
    "status": 5,
    "urgency": 5,
    "impact": 5,
    "date_creation": "2026-05-16 14:22:00",
    "solvedate": "2026-05-16 17:10:00",
    "itilcategories_id": "network",
    "_users_id_requester": "user_7",
    "_groups_id_assign": "group_2"
  }
}
```

### Consommateur (`streaming/pipeline/consumer/kafka_consumer.py`)

Traitement par message :
1. **Désérialisation** JSON Kafka
2. **Enrichissement** :
   - Calcul MTTR (date_résolution - date_création) en heures
   - Évaluation SLA (MTTR ≤ limite par priorité)
   - Normalisation label de priorité (code 1→5 → Very High→Very Low)
3. **Résolution dimensions** :
   - `get_or_create_category()` — crée si n'existe pas dans `dim_category`
   - `get_or_create_group()` — crée si n'existe pas dans `dim_group`
4. **Upsert PostgreSQL** :
   - `fact_tickets` — `ON CONFLICT (glpi_ticket_id) DO UPDATE`
   - `fact_ticket_sla` — délai SLA et deadline calculés

### Limites SLA par priorité

| Priorité | Délai SLA |
|----------|-----------|
| Very High | 4 heures |
| High | 8 heures |
| Medium | 24 heures |
| Low | 72 heures |
| Very Low | 168 heures (7 jours) |

---

## 8. Pipeline Batch (Lambda)

### DAGs Airflow

#### `dag_etl.py` — ETL quotidien
- **Schedule** : `30 0 * * *` (00h30 UTC chaque jour)
- Étapes : extraction CSV/API → nettoyage → transformation → chargement DW → calcul KPIs

#### `dag_api_ingest.py` — Ingestion API
- **Schedule** : `*/5 * * * *` (toutes les 5 minutes)
- Interroge l'API FastAPI pour synchroniser les prédictions

#### `dag_ml_retrain.py` — Retraining ML hebdomadaire
- **Schedule** : `0 2 * * 0` (dimanche 02h00 UTC)
- Étapes :
  1. `check_api_health` — vérification disponibilité API
  2. `trigger_model_retrain` — POST `/retrain` → retraining asynchrone

#### `batch_itsm_pipeline_dag.py` — Pipeline batch complet
- Orchestre l'ensemble du pipeline batch en séquence
- Inclus : ingestion → qualité → transformation → ML dataset

---

## 9. Modèle Machine Learning

### Pipeline d'entraînement (`streaming/ml/train.py`)

#### Données
- **Source primaire** : PostgreSQL DW (`fact_tickets` + dimensions)
- **Données synthétiques** : 12 000 tickets corrélés générés si DW < 500 lignes
- **Stratégie blend** : données réelles + synthétiques (ratio 1:1 minimum)

#### Feature Engineering (17 features)

| Feature | Type | Description |
|---------|------|-------------|
| `urgency` | Numérique | Niveau d'urgence (1–5) |
| `impact` | Numérique | Niveau d'impact (1–5) |
| `urgency_x_impact` | **Engineered** | Interaction urgence × impact |
| `urgency_sq` | **Engineered** | Urgence au carré (non-linéarité) |
| `impact_sq` | **Engineered** | Impact au carré |
| `severity_score` | **Engineered** | Urgence + Impact |
| `hour_of_day` | Temporel | Heure de création (0–23) |
| `day_of_week` | Temporel | Jour de la semaine (0–6) |
| `month` | Temporel | Mois (1–12) |
| `is_business_hours` | **Engineered** | 1 si entre 8h et 18h |
| `is_weekend` | **Engineered** | 1 si samedi ou dimanche |
| `quarter` | **Engineered** | Trimestre (1–4) |
| `category_type_*` | Catégoriel (one-hot) | network, hardware, software, security, access |

#### Modèles candidats et sélection

4 modèles testés avec **StratifiedKFold (5 folds)** :

| Modèle | Avantages |
|--------|-----------|
| `LogisticRegression` (Pipeline + StandardScaler) | Baseline interprétable |
| `RandomForest` (300 arbres, profondeur 18) | Robuste, pas de normalisation requise |
| `GradientBoosting` (200 estimateurs, lr=0.08) | Performances élevées sur données tabulaires |
| `XGBoost` (300 est., reg_alpha=0.1) | État de l'art, régularisation L1 |

**Sélection** : meilleur CV-F1 pondéré sur 5 folds

#### Métriques actuelles du modèle

| Métrique | Valeur |
|----------|--------|
| Algorithme | RandomForest |
| F1-Score pondéré | **69.8%** |
| Accuracy | **69.7%** |
| Balanced Accuracy | **72.5%** |
| CV-F1 (5-fold) | **69.5% ± 0.86%** |
| MAE MTTR | **15.4h** |
| Features | **17** |

#### Régresseur MTTR

`GradientBoostingRegressor` entraîné en parallèle sur les mêmes features pour prédire la durée de résolution (en heures).

#### Bundle modèle sauvegardé (`model.joblib`)
```python
{
    "model":          <classificateur priorité>,
    "mttr_model":     <régresseur MTTR>,
    "label_encoder":  <LabelEncoder 5 classes>,
    "feature_cols":   [liste des 17 features],
    "metrics":        {f1, accuracy, cv_f1_mean, cv_f1_std, mae_mttr, ...},
    "trained_at":     "2026-05-17T18:00:43"
}
```

### Retraining manuel

```bash
# Via Docker (recommandé)
docker compose --profile training run --rm ml-trainer

# Via API (asynchrone, non-bloquant)
curl -X POST http://localhost:8000/retrain
```

---

## 10. API REST FastAPI

### Endpoints

#### `GET /health`
Vérification de disponibilité de l'API.
```json
{
  "status": "ok",
  "timestamp": "2026-05-17T18:30:00Z",
  "model_version": "2026-05-17T18:00:43"
}
```

#### `POST /predict`
Prédiction de la priorité d'un ticket ITSM.

**Rate limit** : 60 requêtes/minute par IP

**Payload** :
```json
{
  "urgency": 4,
  "impact": 5,
  "hour_of_day": 9,
  "day_of_week": 1,
  "month": 5,
  "category_type": "network"
}
```

**Réponse** :
```json
{
  "predicted_label": "Very High",
  "predicted_index": 3,
  "confidence": 0.9963,
  "probabilities": {
    "Very High": 0.9963,
    "High": 0.0037,
    "Medium": 0.0,
    "Low": 0.0,
    "Very Low": 0.0
  },
  "predicted_mttr_hours": 3.72
}
```

#### `GET /metadata`
Informations sur le modèle chargé (algorithme, features, métriques).

#### `POST /retrain`
Déclenche un retraining asynchrone en arrière-plan.
```json
{
  "status": "retraining_started",
  "timestamp": "2026-05-17T18:31:00Z"
}
```

#### `GET /metrics`
Métriques Prometheus (format text/plain) :
- `http_requests_total` — compteur par endpoint et code HTTP
- `http_request_duration_seconds` — histogram des latences
- `process_resident_memory_bytes` — mémoire RSS
- `process_cpu_seconds_total` — temps CPU

### Sécurité API

- **Rate limiting** : slowapi, 60 req/min par IP sur `/predict`, 200 req/min global
- **Validation** : Pydantic v2, validation stricte des types et plages (urgency 1–5, etc.)
- **CORS** : non configuré (à ajouter en production)

---

## 11. Dashboard Streamlit

6 pages accessibles via la barre latérale :

### Page 1 — KPIs Stratégiques
- 6 cartes KPI animées (total tickets, ouverts, résolus, MTTR moyen, taux SLA, critiques)
- Graphique en anneau : répartition par priorité
- Graphique en barres empilées : tickets par statut × priorité
- Diagramme Sankey : flux Source → Priorité → Statut
- Tableau Top 10 catégories (tickets, MTTR moyen, % SLA)

### Page 2 — Live Stream ⟳ (auto-refresh 15s)
- Badge LIVE animé + compteur de cycle
- 4 KPIs temps réel (total streaming, dernière heure, 5 min, critiques ouverts)
- Graphique aire : ingestion des 30 dernières minutes par minute
- Tableau des 30 derniers tickets colorés par priorité

### Page 3 — Tendances Temporelles
- Sélecteur de granularité : Jour / Semaine / Heure
- Colonne gauche : volume par période (aire), MTTR + moyenne mobile
- Colonne droite : évolution SLA%, volume cumulé
- Heatmap jour × heure (concentration des incidents)
- Évolution mix de priorités par semaine (barres empilées)

### Page 4 — Analyse des SLA
- Gauge global SLA (0–100%, seuil 80%)
- Barres % SLA par priorité
- Scatter plot : MTTR vs SLA% vs Volume (bubble chart)
- SLA% par groupe de support (barres horizontales)
- Box plot distribution MTTR par priorité
- Tableau des dépassements SLA (delay_hours)

### Page 5 — Moteur Prédictif IA
- Formulaire de saisie : urgence, impact, heure, jour, mois, catégorie
- Carte résultat : priorité prédite + confidence chip coloré + MTTR estimé
- Graphique distribution des probabilités (barres horizontales)
- Historique des prédictions de session (10 dernières)
- Tickets historiques similaires (urgence ± 1, impact ± 1)

### Page 6 — Performance ML
- 6 cartes métriques : algorithme, F1, CV-F1, Accuracy, Balanced Acc., MAE MTTR
- Gauge F1-Score avec delta vs objectif 75%
- Gauge Accuracy
- Liste des features avec type (Numérique / Engineered / Temporel / Catégoriel)
- Pie chart répartition des types de features
- Tableau historique des modèles entraînés (depuis DW)
- Bouton de retraining manuel

---

## 12. Monitoring — Prometheus & Grafana

### Prometheus (`http://localhost:9090`)

Cibles surveillées :
- `itsm-api` : http://api:8000/metrics (scraping toutes les 10s)
- `prometheus` : auto-monitoring

Requêtes utiles :
```promql
# Taux de requêtes total
sum(rate(http_requests_total[1m]))

# Latence p95
histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[5m])) by (le))

# Taux d'erreur 5xx
sum(rate(http_requests_total{status_code=~"5.."}[5m])) / sum(rate(http_requests_total[5m]))

# Mémoire RSS en MB
process_resident_memory_bytes / 1024 / 1024
```

### Grafana (`http://localhost:3000`)
- **Login** : `admin` / `itsm_grafana_2026`
- Dashboard auto-provisionné : **"ITSM Intelligence Platform"**

**15 panels organisés en 3 sections :**

| Section | Panels |
|---------|--------|
| 🚀 API Performance | Request Rate · Latency p95 · Error Rate 5xx · Memory RSS |
| | HTTP Requests by Handler (timeseries) · Latency p50/p95/p99 (timeseries) |
| 💻 System Resources | CPU Usage · Memory RSS vs Virtual · GC Collections |
| 🤖 ML Predictions | Predictions Rate · Prediction Latency · Status Code Distribution |

**Auto-refresh** : toutes les 10 secondes, fenêtre par défaut : 1 heure

---

## 13. Orchestration — Apache Airflow

### Accès : `http://localhost:8081` — `admin` / `admin`

### DAGs configurés

#### `dag_etl` — Quotidien 00h30
```
extract_data → clean_data → transform_data → load_to_dw → compute_kpis
```

#### `dag_api_ingest` — Toutes les 5 minutes
```
ingest_from_api
```

#### `dag_ml_retrain` — Dimanche 02h00
```
check_api_health → trigger_model_retrain
```

Le retraining appelle `POST http://api:8000/retrain` qui lance `retrain.py` en tâche de fond FastAPI. Le nouveau modèle est sauvegardé et l'API redémarre automatiquement.

#### `batch_itsm_pipeline_dag` — Manuel / planifiable
```
ingest → validate → clean → transform → load → build_ml_dataset
```

---

## 14. Base de Données — Schéma DW

### Tables de dimensions

| Table | Colonnes clés | Description |
|-------|---------------|-------------|
| `dim_priority` | `code` (1–5), `label` (Very High…Very Low) | Niveaux de priorité GLPI |
| `dim_status` | `code` (1–6), `label` | Statuts tickets (New, Processing, Solved, Closed…) |
| `dim_category` | `name`, `itil_type` | Catégories ITIL des incidents |
| `dim_group` | `name`, `team_type` | Groupes de support |
| `dim_user` | `name`, `email` | Utilisateurs GLPI |
| `dim_date` | `date`, `year`, `month`, `week`, `day_of_week` | Calendrier pour analyses |

### Table de faits principale

```sql
fact_tickets (
    ticket_id          SERIAL PRIMARY KEY,
    glpi_ticket_id     INT UNIQUE,          -- ID GLPI original
    priority_id        → dim_priority,
    status_id          → dim_status,
    category_id        → dim_category,
    group_id           → dim_group,
    user_id            → dim_user,
    urgency            SMALLINT,            -- 1–5
    impact             SMALLINT,            -- 1–5
    date_creation      TIMESTAMP,
    date_resolution    TIMESTAMP,
    mttr_hours         DECIMAL,             -- Temps de résolution en heures
    sla_respected      BOOLEAN,
    source             VARCHAR,             -- 'glpi_api' | 'csv_batch'
    ingested_at        TIMESTAMP
)
```

### Tables de faits secondaires

```sql
fact_ticket_sla (
    ticket_id      → fact_tickets,
    sla_deadline   TIMESTAMP,              -- Date limite SLA
    resolution_date TIMESTAMP,
    sla_respected  BOOLEAN,
    delay_hours    DECIMAL,                -- Retard si dépassement (négatif si en avance)
    mttr_hours     DECIMAL
)

fact_ticket_events (
    event_id     SERIAL,
    ticket_id    → fact_tickets,
    event_type   VARCHAR,
    event_date   TIMESTAMP
)

ml_model_registry (
    id           SERIAL,
    algorithm    VARCHAR,                  -- RandomForest | XGBoost | …
    f1_score     DECIMAL,
    accuracy     DECIMAL,
    mae_mttr     DECIMAL,
    trained_at   TIMESTAMP
)

ml_predictions (
    prediction_id  SERIAL,
    ticket_id      → fact_tickets,
    predicted_label VARCHAR,
    confidence      DECIMAL,
    predicted_at    TIMESTAMP
)
```

---

## 15. Tests Unitaires

### Lancer les tests

```bash
# Depuis la racine du projet
cd tests
pip install -r requirements-test.txt
pytest unit/ -v

# Avec rapport de couverture
pytest unit/ -v --cov=../streaming --cov-report=html
```

### Tests disponibles

#### `test_consumer.py` — Consumer Kafka
- `test_parse_glpi_datetime` : formats valides, ISO, NULL, None, invalide
- `test_enrich_ticket` : calcul MTTR, logique SLA, mapping priorité, champs requis
- `test_sla_limits` : constantes limites SLA par priorité

#### `test_producer.py` — Producteur GLPI
- `test_weekend_detection` : samedi/dimanche/lundi/vendredi
- `test_mock_ticket_structure` : structure JSON, `solvedate` après `date_creation`

#### `test_ml.py` — Pipeline ML
- `test_generate_synthetic_dataset` : forme, colonnes, plages de valeurs, reproductibilité
- `test_prepare_features` : absence de NaN, colonnes features, label encoder

#### `test_api.py` — API FastAPI
- Tests `/health`, `/predict` (valide/invalide), `/metadata`
- `test_build_feature_vector` : vecteur de features engineered
- Mocking de `os.path.exists` et `joblib.load` pour isolation

---

## 16. Configuration GLPI (Optionnel)

Par défaut, le producteur fonctionne en **mode mock** (données synthétiques). Pour connecter GLPI réel :

### Étape 1 — Créer un client API dans GLPI
1. Aller sur http://localhost:8080
2. **Configuration → Général → API** (ou Setup → General → API)
3. Activer l'API REST
4. **Ajouter un client API** → Nom: "ITSM Producer" → Régénérer les tokens
5. Copier l'**App-Token**

### Étape 2 — Obtenir le User-Token
1. Connexion avec `glpi` / `glpi`
2. **Préférences** (icône utilisateur) → onglet **API**
3. Cliquer **Régénérer** → copier le User-Token

### Étape 3 — Configurer `.env`
```env
GLPI_APP_TOKEN=votre_app_token_ici
GLPI_USER_TOKEN=votre_user_token_ici
```

### Étape 4 — Redémarrer le producteur
```bash
docker compose restart producer
```

Le producteur basculera automatiquement du mode mock vers GLPI réel.

---

## 17. Ce qui manque / Améliorations futures

### Manquant actuellement

| Élément | Priorité | Description |
|---------|----------|-------------|
| **Tokens GLPI** | Haute | `GLPI_APP_TOKEN` et `GLPI_USER_TOKEN` dans `.env` vides → mode mock uniquement |
| **Retraining automatique actif** | Moyenne | Le DAG `dag_ml_retrain` existe mais Airflow doit être activé manuellement |
| **Authentification dashboard** | Basse | Streamlit sans login — à sécuriser en prod |
| **HTTPS / TLS** | Basse | Tout en HTTP — à ajouter avec un reverse proxy (Nginx/Traefik) |
| **Alerting Grafana** | Basse | Dashboard sans alertes email/Slack configurées |

### Améliorations possibles

| Amélioration | Impact |
|--------------|--------|
| SHAP / LIME pour explicabilité ML | Comprendre pourquoi le modèle prédit une priorité |
| Détection d'anomalies (Isolation Forest) | Alertes automatiques sur tickets inhabituels |
| NLP sur le champ `description` (TF-IDF / BERT) | Meilleure discrimination entre catégories |
| A/B testing de modèles | Comparer deux versions du modèle en prod |
| WebSocket sur dashboard | Streaming vraiment temps réel sans polling |
| PostgreSQL → TimescaleDB | Meilleures performances pour séries temporelles |
| MLflow | Tracking d'expériences ML, versioning des modèles |

---

## Variables d'Environnement — Référence complète

```env
# MariaDB (GLPI)
MARIADB_ROOT_PASSWORD=root_secret_2026
MARIADB_DATABASE=glpi
MARIADB_USER=glpi
MARIADB_PASSWORD=glpi_secret_2026

# PostgreSQL Data Warehouse
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=itsm_dw
POSTGRES_USER=itsm
POSTGRES_PASSWORD=itsm_dw_secret_2026

# Kafka
KAFKA_BROKER=kafka:29092
KAFKA_TOPIC_RAW=itsm.tickets.raw
KAFKA_GROUP_ID=itsm-consumer-group

# GLPI API (laisser vide pour mode mock)
GLPI_APP_TOKEN=
GLPI_USER_TOKEN=

# Airflow
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW__CORE__FERNET_KEY=81HqDtbqAywKSOumSha3BhWNOdQ26slT6K0YaZeZyPs=

# ML
ML_MODEL_PATH=/app/models/model.joblib

# Grafana
GF_SECURITY_ADMIN_PASSWORD=itsm_grafana_2026
```

---

## Commandes utiles

```bash
# Voir les logs d'un service
docker logs itsm-producer --tail=20 -f
docker logs itsm-consumer --tail=20 -f
docker logs itsm-api --tail=20

# Accéder au PostgreSQL DW
docker exec -it itsm-postgres psql -U itsm -d itsm_dw

# Requêtes DW utiles
# Nombre de tickets par priorité
SELECT dp.label, COUNT(*) FROM fact_tickets ft
JOIN dim_priority dp ON ft.priority_id=dp.priority_id
GROUP BY dp.label, dp.code ORDER BY dp.code;

# Taux SLA global
SELECT ROUND(100.0 * SUM(CASE WHEN sla_respected THEN 1 ELSE 0 END)
       / NULLIF(COUNT(*), 0), 1) AS sla_pct
FROM fact_tickets WHERE sla_respected IS NOT NULL;

# Réinitialiser le consumer Kafka (replay complet)
docker compose stop consumer
docker exec itsm-kafka sh -c "/usr/bin/kafka-consumer-groups \
  --bootstrap-server localhost:29092 \
  --group itsm-consumer-group \
  --reset-offsets --to-earliest \
  --topic itsm.tickets.raw --execute"
docker compose start consumer

# Lancer un retraining immédiat
docker compose --profile training run --rm ml-trainer

# Tester l'API de prédiction
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"urgency":5,"impact":5,"hour_of_day":9,"day_of_week":1,"month":5,"category_type":"security"}'

# Vérifier l'état de Prometheus
curl -s http://localhost:9090/api/v1/targets | \
  python -c "import json,sys;d=json.load(sys.stdin);[print(t['labels']['job'],t['health']) for t in d['data']['activeTargets']]"
```

---

*Documentation générée le 17 Mai 2026 — ITSM Intelligence Platform v3*
*ENSA Fès · Département Génie des Télécommunications et Réseaux*
