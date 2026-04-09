# TP Jour 2 — Pipeline d'ingestion de logs e-commerce avec Airflow & HDFS

> **Formation** : M2 Data Engineering — IPSSI Montpellier  
> **Thème** : Orchestration Apache Airflow + stockage distribué HDFS  
> **Branche** : `mohamed_dev_airflow`

---

## Objectif

Concevoir et déployer un pipeline de données complet qui :
1. Génère des **logs Apache** simulant un site e-commerce
2. Les ingère dans **HDFS** (Hadoop Distributed File System)
3. Les **analyse** (status codes, taux d'erreur, top URLs)
4. **Branche** le flux selon le taux d'erreur (alerte ou archivage)
5. **Archive** les fichiers traités dans une zone dédiée

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    Docker Compose (6 services)            │
│                                                          │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │  PostgreSQL  │  │   Airflow    │  │   Airflow    │   │
│  │  (port 5432) │  │  Webserver   │  │  Scheduler   │   │
│  │  Métadonnées │  │  (port 8080) │  │  Exécution   │   │
│  └─────────────┘  └──────────────┘  └──────┬───────┘   │
│                                             │            │
│                                      /shared (volume)    │
│                                             │            │
│  ┌──────────────┐  ┌──────────────┐         │           │
│  │   NameNode   │  │   DataNode   │◄────────┘           │
│  │  (port 9870) │  │  (stockage)  │   WebHDFS API       │
│  └──────────────┘  └──────────────┘                     │
└──────────────────────────────────────────────────────────┘
```

**Zones HDFS** :
- `/data/ecommerce/logs/raw/` — fichiers bruts ingérés
- `/data/ecommerce/logs/processed/` — fichiers archivés après traitement
- `/data/ecommerce/logs/weekly/` — fichiers compactés (hebdomadaire)

---

## Structure du projet

```
TP_2_jour2/
├── README.md                          ← ce fichier
├── PLAN.md                            ← plan de travail par phases
└── ecommerce-logs-pipeline/
    ├── docker-compose.yaml            ← stack Airflow + HDFS (6 services)
    ├── hadoop.env                     ← configuration Hadoop
    ├── .env                           ← UID Airflow
    ├── REPONSES.md                    ← réponses aux questions de réflexion
    ├── EXPLICATIONS.md                ← document complet d'explication + Q&A
    ├── dags/
    │   ├── logs_ecommerce_dag.py      ← DAG principal (8 tâches)
    │   ├── dag_branchement.py         ← exercice BranchPythonOperator
    │   ├── dag_xcom.py                ← exercice XCom
    │   ├── dag_sensor.py              ← exercice FileSensor
    │   └── logs_compaction_dag.py     ← compaction hebdomadaire (bonus)
    ├── plugins/
    │   └── hdfs_sensor.py             ← sensor HDFS custom (bonus)
    ├── scripts/
    │   └── generer_logs.py            ← générateur de logs Apache
    └── logs/                          ← logs d'exécution Airflow
```

---

## DAGs

### DAG principal — `logs_ecommerce_dag` (8 tâches)

Pipeline complet d'ingestion et d'analyse de logs :

```
generer_logs → uploader_vers_hdfs → hdfs_file_sensor → analyser_logs
                                                            │
                                                  brancher_selon_taux_erreur
                                                       ┌────┴────┐
                                                       ▼         ▼
                                                   alerter   archiver_ok
                                                       └────┬────┘
                                                            ▼
                                                    archiver_logs_hdfs
```

| Tâche | Opérateur | Description |
|---|---|---|
| `generer_logs_journaliers` | PythonOperator | Génère 1000 lignes de logs Apache |
| `uploader_vers_hdfs` | PythonOperator | Upload vers HDFS via WebHDFS |
| `hdfs_file_sensor` | PythonOperator | Vérifie la présence dans HDFS |
| `analyser_logs_hdfs` | PythonOperator | Calcule status codes, top URLs, taux d'erreur |
| `brancher_selon_taux_erreur` | BranchPythonOperator | Alerte si taux > 5%, sinon OK |
| `alerter_equipe_ops` | PythonOperator | Notification équipe opérations |
| `archiver_rapport_ok` | PythonOperator | Log si taux normal |
| `archiver_logs_hdfs` | PythonOperator | Déplace `raw/` → `processed/` |

### Exercices

| DAG | Tâches | Concept Airflow |
|---|---|---|
| `dag_branchement` | 4 | `BranchPythonOperator`, `TriggerRule` |
| `dag_xcom` | 3 | Communication inter-tâches via XCom |
| `dag_sensor` | 2 | `FileSensor` en mode `reschedule` |
| `logs_compaction_dag` | 4 | TaskFlow API (`@task`), compaction HDFS |

---

## Lancement

### Prérequis

- Docker & Docker Compose v2
- Port 8080 disponible (Airflow UI)

### Démarrage

```bash
cd TP_2_jour2/ecommerce-logs-pipeline

# Lancer la stack
docker compose up -d

# Vérifier que les 5 services tournent (airflow-init s'arrête après init)
docker compose ps

# Créer les répertoires HDFS
docker exec namenode hdfs dfs -mkdir -p /data/ecommerce/logs/raw
docker exec namenode hdfs dfs -mkdir -p /data/ecommerce/logs/processed
docker exec namenode hdfs dfs -mkdir -p /data/ecommerce/logs/weekly
docker exec namenode hdfs dfs -chmod -R 777 /data/ecommerce/logs
```

### Accès

| Service | URL |
|---|---|
| Airflow UI | http://localhost:8080 (admin / admin) |
| HDFS NameNode UI | http://localhost:9870 |

### Exécution des DAGs

```bash
# Activer et déclencher le DAG principal
docker exec ecommerce-logs-pipeline-airflow-scheduler-1 airflow dags unpause logs_ecommerce_dag
docker exec ecommerce-logs-pipeline-airflow-scheduler-1 airflow dags trigger logs_ecommerce_dag

# Exercices
docker exec ecommerce-logs-pipeline-airflow-scheduler-1 airflow dags trigger dag_branchement
docker exec ecommerce-logs-pipeline-airflow-scheduler-1 airflow dags trigger dag_xcom
```

### Arrêt

```bash
docker compose down
```

---

## Technologies

| Composant | Version | Rôle |
|---|---|---|
| Apache Airflow | 2.8.0 | Orchestration (LocalExecutor) |
| Apache Hadoop HDFS | 3.2.1 | Stockage distribué |
| PostgreSQL | 13 | Base de métadonnées Airflow |
| Docker Compose | v2 | Conteneurisation |
| WebHDFS | REST API | Communication Airflow ↔ HDFS |
| Python | 3.8 | Scripts et logique des DAGs |

---

## Documentation complémentaire

- [`REPONSES.md`](ecommerce-logs-pipeline/REPONSES.md) — Réponses aux 4 questions de réflexion (HDFS vs local, NameNode HA, modes sensor, réplication)
- [`EXPLICATIONS.md`](ecommerce-logs-pipeline/EXPLICATIONS.md) — Document complet : architecture, explication de chaque tâche, toutes les questions/réponses possibles
- [`PLAN.md`](PLAN.md) — Plan de travail par phases avec suivi d'avancement
