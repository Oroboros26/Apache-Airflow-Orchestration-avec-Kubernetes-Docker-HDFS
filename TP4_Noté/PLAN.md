# 📋 Plan de Travail — TP Noté Jour 4 : Data Platform Santé Publique ARS Occitanie

> **Formation** : Data Engineering — Apache Airflow — IPSSI Montpellier  
> **Branche** : `mohamed_dev_airflow`  
> **Date** : 13 Avril 2026  
> **Durée** : 4 heures

---

## 📚 Vue d'ensemble

TP noté individuel — conception et déploiement d'une **data platform de surveillance épidémiologique** pour l'ARS Occitanie. Le pipeline collecte les données IAS® (Indicateurs Avancés Sanitaires) depuis data.gouv.fr, calcule des indicateurs statistiques (z-score, R0, classification), et génère des rapports d'alerte.

| Composant | Technologie |
|---|---|
| Orchestration | Apache Airflow 2.8 (CeleryExecutor) |
| Broker | Redis |
| BDD Airflow | PostgreSQL 13 |
| BDD ARS | PostgreSQL 13 (ars_epidemio) |
| Stockage | Volume Docker nommé (ars-data) |
| Données | IAS® Grippe + Gastro-entérite (data.gouv.fr) |

---

## 🗂️ Structure du projet

```
TP4_Noté/
├── PLAN.md                                    ← Ce fichier
├── README.md                                  ← Documentation du projet
├── REPONSES.md                                ← Réponses aux questions
├── EXPLICATIONS.md                            ← Explications techniques
├── TP_Jour4_NOTE_Sante_Publique_ARS.pdf       ← Sujet du TP
└── ars-epidemio/                              ← TP PRINCIPAL
    ├── docker-compose.yaml                    ← Stack Airflow CeleryExecutor
    ├── .env                                   ← AIRFLOW_UID
    ├── .env.example                           ← Template .env
    ├── dags/
    │   ├── ars_epidemio_dag.py                ← DAG principal (10 tâches)
    │   └── sql/
    │       └── init_ars_epidemio.sql          ← Schéma PostgreSQL + données référence
    ├── scripts/
    │   ├── collecte_ias.py                    ← Collecte données IAS® data.gouv.fr
    │   └── calcul_indicateurs.py              ← Calcul z-score, R0, classification
    ├── output/                                ← Rapports JSON générés
    ├── screenshots/                           ← Captures d'écran
    ├── logs/
    └── plugins/
```

---

## 🚀 Étapes du TP

### Étape 1 — Setup Docker Compose ✅
- [x] Créer `docker-compose.yaml` (8 services : postgres, postgres-ars, redis, webserver, scheduler, worker, flower, init)
- [x] Créer `.env` avec `AIRFLOW_UID=50000`
- [x] Stack CeleryExecutor avec Redis comme broker

### Étape 2 — Configuration Airflow
- [ ] Connexion `postgres_ars` (Admin > Connections)
- [ ] Variables Airflow (seuils, départements, syndromes)
- [ ] Vérification depuis PythonOperator

### Étape 3 — Initialisation PostgreSQL ✅
- [x] Créer `init_ars_epidemio.sql` (5 tables + données référence)
- [x] Tables : syndromes, departements, donnees_hebdomadaires, indicateurs_epidemiques, rapports_ars
- [x] 13 départements Occitanie + 5 syndromes

### Étape 4 — Collecte données IAS® ✅
- [x] Script `collecte_ias.py` : téléchargement CSV data.gouv.fr
- [x] Parsing CSV (séparateur `;`, décimal `,`)
- [x] Agrégation hebdomadaire de Loc_Reg76
- [x] PythonOperator dans le DAG

### Étape 5 — Archivage local ✅
- [x] Partitionnement `/data/ars/raw/<annee>/<semaine>/`
- [x] Vérification d'existence et taille

### Étape 6 — Calcul indicateurs ✅
- [x] Script `calcul_indicateurs.py` : z-score, classification, R0
- [x] Seuils IAS (MIN_Saison / MAX_Saison)
- [x] Seuils z-score (1.5 / 3.0)
- [x] Classification finale : max(statut_ias, statut_zscore)

### Étape 7 — Insertion PostgreSQL ✅
- [x] INSERT donnees_hebdomadaires ON CONFLICT DO UPDATE
- [x] INSERT indicateurs_epidemiques ON CONFLICT DO UPDATE

### Étape 8 — BranchPythonOperator ✅
- [x] Évaluation situation : URGENCE → alerte / ALERTE → bulletin / NORMAL → confirmation

### Étape 9 — Rapport hebdomadaire ✅
- [x] Rapport JSON structuré
- [x] Sauvegarde volume Docker + PostgreSQL
- [x] TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS

### Étape 10 — Monitoring et validation
- [ ] Démarrer la stack `docker compose up -d`
- [ ] Vérifier tous les services (docker compose ps)
- [ ] Accéder Airflow UI (http://localhost:8080) et Flower (http://localhost:5555)
- [ ] Déclencher un DAG Run
- [ ] Vérifier les données PostgreSQL
- [ ] Captures d'écran

---

## 📊 Architecture du DAG

```
[init_base_donnees] (PostgresOperator)
       │
       ▼
[TaskGroup: collecte]
  └── collecter_donnees_sursaud (PythonOperator)
       │
       ▼
[TaskGroup: persistance_brute]
  ├── archiver_local (PythonOperator)
  └── verifier_archive (PythonOperator)
       │
       ▼
[TaskGroup: traitement]
  └── calculer_indicateurs_epidemiques (PythonOperator)
       │
       ▼
[TaskGroup: persistance_operationnelle]
  └── inserer_donnees_postgres (PythonOperator)
       │
       ▼
[evaluer_situation_epidemique] (BranchPythonOperator)
       │
  ┌────┼──────────┐
  ▼    ▼          ▼
[alerte] [bulletin] [normale]
  │    │          │
  └────┴──────────┘
       │
       ▼
[generer_rapport_hebdomadaire] (TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
```
