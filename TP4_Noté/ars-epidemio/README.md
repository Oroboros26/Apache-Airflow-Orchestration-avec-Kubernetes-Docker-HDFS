# TP Noté Airflow — Data Platform Santé Publique ARS Occitanie

## Auteur
- **Nom** : Sahbi Mohamed 
- **Prénom** : Mohamed
- **Formation** : Master 2 Data Engineering — IPSSI Montpellier
- **Date** : 13 Avril 2026

---

## Prérequis

- Docker Desktop >= 4.0
- Docker Compose >= 2.0
- Python >= 3.11 (pour les tests locaux)
- Accès Internet (téléchargement des données IAS® depuis data.gouv.fr)

---

## Instructions de déploiement

### 1. Démarrage de la stack

```bash
cd TP4_Noté/ars-epidemio/

# Créer le fichier .env
echo "AIRFLOW_UID=$(id -u)" > .env

# Démarrer tous les services
docker compose up -d

# Vérifier que tous les services sont Running
docker compose ps

# Résultat attendu : 7 services running
# postgres, postgres-ars, redis, airflow-webserver,
# airflow-scheduler, airflow-worker, flower
```

### 2. Configuration des connexions et variables Airflow

Accéder à l'UI Airflow : http://localhost:8080 (admin / admin)

#### Connexions (Admin > Connections)

| Conn Id | Type | Host | Port | Schema | Login | Password |
|---|---|---|---|---|---|---|
| `postgres_ars` | Postgres | postgres-ars | 5432 | ars_epidemio | postgres | postgres |

#### Variables (Admin > Variables)

| Clé | Valeur |
|---|---|
| `semaines_historique` | `12` |
| `seuil_alerte_incidence` | `150` |
| `seuil_urgence_incidence` | `500` |
| `seuil_alerte_zscore` | `1.5` |
| `seuil_urgence_zscore` | `3.0` |
| `departements_occitanie` | `["09","11","12","30","31","32","34","46","48","65","66","81","82"]` |
| `syndromes_surveilles` | `["GRIPPE","GEA","SG","BRONCHIO","COVID19"]` |
| `archive_base_path` | `/data/ars` |

### 3. Démarrage du pipeline

1. Activer le DAG `ars_epidemio_dag` dans l'UI Airflow
2. Déclencher un DAG Run manuel via le bouton "Trigger DAG"
3. Surveiller l'exécution dans la vue Graph

---

## Architecture des données

### Partitionnement des fichiers

Les données brutes sont archivées dans un volume Docker nommé (`ars-data`) avec un partitionnement temporel :

```
/data/ars/
├── raw/
│   └── <annee>/
│       └── <semaine>/
│           └── sursaud_YYYY-SXX.json
├── indicateurs/
│   └── indicateurs_YYYY-SXX.json
└── rapports/
    └── <annee>/
        └── <semaine>/
            └── rapport_YYYY-SXX.json
```

### Schéma PostgreSQL (base `ars_epidemio`)

| Table | Description |
|---|---|
| `syndromes` | Référentiel des 5 syndromes surveillés (GRIPPE, GEA, SG, BRONCHIO, COVID19) |
| `departements` | 13 départements Occitanie avec population et chef-lieu |
| `donnees_hebdomadaires` | Données IAS® agrégées par semaine ISO et syndrome |
| `indicateurs_epidemiques` | Z-score, R0, statut NORMAL/ALERTE/URGENCE par semaine |
| `rapports_ars` | Rapports JSON générés avec situation globale |

Toutes les tables incluent `created_at` et `updated_at` pour la traçabilité (conformité RGPD).

---

## Décisions techniques

1. **CeleryExecutor** : Choisi pour la scalabilité — les workers Celery permettent le parallélisme des tâches de calcul intensif (z-score, R0)
2. **Deux PostgreSQL séparés** : La base Airflow (métadonnées) est isolée de la base ARS (données de santé) — principe de cloisonnement RGPD
3. **Volume Docker nommé** : Persistance des données brutes et rapports indépendamment du cycle de vie des conteneurs — archivage 5 ans
4. **ON CONFLICT DO UPDATE** : Idempotence garantie — le pipeline peut être relancé sans créer de doublons
5. **catchup=True** : Permet de rejouer les semaines passées — fonctionnalité clé d'Airflow
6. **max_active_runs=1** : Évite les conditions de course sur les données partagées
7. **TaskGroups** : Organisation logique du pipeline en phases (collecte, persistance, traitement, etc.)
8. **BranchPythonOperator** : Routage conditionnel selon la sévérité (NORMAL → ALERTE → URGENCE)

---

## Difficultés rencontrées et solutions

1. **Format CSV français** : Les données IAS® utilisent `;` comme séparateur et `,` comme décimal — résolu avec un parsing personnalisé qui convertit les virgules décimales en points
2. **Valeurs NA** : Le dataset contient des `NA` pour les données manquantes (réseau Sentinelles) — gestion explicite avec conversion en `None`
3. **Agrégation hebdomadaire** : Les données sont quotidiennes mais le pipeline traite par semaine ISO — agrégation par moyenne de `Loc_Reg76`
4. **Z-score avec historique incomplet** : Certaines saisons ont des données manquantes — le calcul exige un minimum de 3 saisons valides
5. **Trigger rule du rapport** : Le rapport doit s'exécuter quelle que soit la branche empruntée — résolu avec `TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS`

---

## Interfaces

- **Airflow UI** : http://localhost:8080 (admin / admin)
- **Flower** : http://localhost:5555
- **PostgreSQL ARS** : localhost:5433 (postgres / postgres / ars_epidemio)
