# 📋 Plan de Travail — TP Jour 2

> **Formation** : Data Engineering — Apache Airflow — IPSSI Montpellier  
> **Branche** : `mohamed_dev_airflow`  
> **Date** : 9 Avril 2026  
> **Durée estimée** : ~4-5h (TP principal 3h + exercices supplémentaires)

---

## 📚 Vue d'ensemble

Le Jour 2 comporte **deux documents** :

| Document | Contenu |
|---|---|
| `apache_airflow_k8s_docker_hdfs_Jour2.pdf` | **Cours/Slides** — DAGs, Opérateurs, XCom, Sensors, Hooks, Pipeline ETL + 3 exercices pratiques |
| `TP_Jour2_Logs_Ecommerce_HDFS.pdf` | **TP Principal** — Pipeline d'ingestion de logs e-commerce vers HDFS (8 étapes) + 3 exercices supplémentaires |

---

## 🗂️ Structure cible du projet

```
TP_2_jour2/
├── PLAN.md                          ← Ce fichier
├── REPONSES.md                      ← Réponses aux questions de réflexion (Q1-Q4)
├── ecommerce-logs-pipeline/         ← TP PRINCIPAL (Pipeline HDFS)
│   ├── docker-compose.yaml
│   ├── hadoop.env
│   ├── .env
│   ├── dags/
│   │   ├── logs_ecommerce_dag.py        ← DAG principal (8 tâches)
│   │   ├── logs_compaction_dag.py       ← Exercice Supp. 2
│   │   ├── dag_branchement.py           ← Exercice Slides 1
│   │   ├── dag_xcom.py                  ← Exercice Slides 2
│   │   └── dag_sensor.py               ← Exercice Slides 3
│   ├── logs/
│   ├── plugins/
│   │   └── hdfs_sensor.py               ← Exercice Supp. 1
│   └── scripts/
│       └── generer_logs.py              ← Script génération logs Apache
├── TP_Jour2_Logs_Ecommerce_HDFS.pdf
└── apache_airflow_k8s_docker_hdfs_Jour2.pdf
```

---

## 🚀 Phase 1 — Infrastructure Docker (Airflow + Hadoop HDFS)

### Tâche 1.1 — Créer la structure du projet
- [ ] Créer le répertoire `ecommerce-logs-pipeline/` avec ses sous-dossiers (`dags/`, `logs/`, `plugins/`, `scripts/`)

### Tâche 1.2 — Fichiers de configuration
- [ ] Créer `hadoop.env` (config HDFS : fs.defaultFS, webhdfs, replication=1, etc.)
- [ ] Créer `.env` avec `AIRFLOW_UID=50000`
- [ ] Créer `docker-compose.yaml` avec les services :
  - `postgres` (base de données Airflow)
  - `airflow-webserver` (port 8080)
  - `airflow-scheduler`
  - `airflow-init` (initialisation DB + user admin)
  - `namenode` (HDFS NameNode, ports 9870 + 9000)
  - `datanode` (HDFS DataNode)

### Tâche 1.3 — Démarrage et vérification
- [ ] `docker compose up airflow-init` → initialiser DB Airflow
- [ ] `docker compose up -d` → démarrer tout le stack
- [ ] Vérifier tous les conteneurs (`docker compose ps`)
- [ ] Vérifier HDFS : `docker exec namenode hdfs dfsadmin -report` → 1 Live DataNode
- [ ] Accéder aux UIs web :
  - Airflow : http://localhost:8080 (admin/admin)
  - HDFS NameNode : http://localhost:9870

### Tâche 1.4 — Créer les répertoires HDFS
- [ ] `docker exec namenode hdfs dfs -mkdir -p /data/ecommerce/logs/raw`
- [ ] `docker exec namenode hdfs dfs -mkdir -p /data/ecommerce/logs/processed`
- [ ] `docker exec namenode hdfs dfs -chmod -R 777 /data/`
- [ ] Vérifier : `docker exec namenode hdfs dfs -ls -R /data/`

### Tâche 1.5 — Configurer la connexion Airflow → HDFS
- [ ] Admin → Connections → Ajouter :
  - Connection Id : `hdfs_default`
  - Connection Type : `HDFS`
  - Host : `namenode`
  - Port : `9870`
  - Login : `root`

---

## 🔧 Phase 2 — Script de génération de logs (Partie 4)

### Tâche 2.1 — Créer `scripts/generer_logs.py`
- [ ] Implémenter le générateur de logs Apache Combined Log Format
- [ ] IPs réalistes, URLs e-commerce (produits, panier, checkout, erreurs 4xx/5xx)
- [ ] User-Agents variés (Chrome, Safari, bots, etc.)
- [ ] Usage : `python3 generer_logs.py <date> <nb_lignes> <fichier_sortie>`
- [ ] Tester : `python3 scripts/generer_logs.py 2024-03-15 100 /tmp/test_access.log`

---

## 🔄 Phase 3 — DAG Principal : `logs_ecommerce_dag.py` (8 tâches)

### Tâche 3.1 — Squelette du DAG
- [ ] Créer le DAG `logs_ecommerce_dag` avec schedule `0 2 * * *` (chaque nuit à 2h)
- [ ] Configurer `default_args` (owner, retries, start_date, etc.)
- [ ] `catchup=False`, tags appropriés

### Tâche 3.2 — `generer_logs_journaliers` (PythonOperator)
- [ ] Utiliser `subprocess.run()` pour appeler `generer_logs.py`
- [ ] Utiliser `context["ds"]` pour la date (idempotence)
- [ ] Générer 1000 lignes → `/tmp/access_<YYYY-MM-DD>.log`
- [ ] Retourner le chemin du fichier (XCom automatique)

### Tâche 3.3 — `uploader_vers_hdfs` (BashOperator)
- [ ] Copier le fichier local vers HDFS via `hdfs dfs -put`
- [ ] **Résoudre le problème de volumes** : le fichier `/tmp/` du scheduler n'est pas accessible depuis namenode
- [ ] Solution : `docker cp` scheduler→namenode puis `hdfs dfs -put` depuis namenode

### Tâche 3.4 — `hdfs_file_sensor` (HdfsSensor ou BashOperator fallback)
- [ ] Attendre la présence du fichier dans HDFS `/data/ecommerce/logs/raw/access_{{ ds }}.log`
- [ ] `poke_interval=30`, `timeout=300`
- [ ] Alternative BashOperator : `hdfs dfs -test -e <path>`

### Tâche 3.5 — `analyser_logs_hdfs` (BashOperator)
- [ ] Lire le fichier HDFS via `hdfs dfs -cat`
- [ ] Compter les requêtes par status code
- [ ] Top 5 URLs les plus visitées
- [ ] Calculer le taux d'erreur (4xx + 5xx / total)
- [ ] Sauvegarder le taux dans `/tmp/taux_erreur_<date>.txt`

### Tâche 3.6 — `brancher_selon_taux_erreur` (BranchPythonOperator)
- [ ] Lire le fichier taux d'erreur
- [ ] Si taux > 5% → retourner `"alerter_equipe_ops"`
- [ ] Si taux ≤ 5% → retourner `"archiver_rapport_ok"`

### Tâche 3.7 — Branches : `alerter_equipe_ops` + `archiver_rapport_ok`
- [ ] `alerter_equipe_ops` : log WARNING avec message d'alerte
- [ ] `archiver_rapport_ok` : log INFO confirmation nominal

### Tâche 3.8 — `archiver_logs_hdfs` (BashOperator)
- [ ] `hdfs dfs -mv` de raw/ → processed/
- [ ] `trigger_rule="none_failed_min_one_success"` (s'exécute quelle que soit la branche)

### Tâche 3.9 — Chaîner les dépendances
- [ ] `generer → upload → sensor → analyser → branch → [alerte, rapport_ok] → archiver`
- [ ] Tester le DAG complet depuis l'UI Airflow

---

## 📝 Phase 4 — Questions de réflexion (`REPONSES.md`)

- [ ] **Q1** — HDFS vs système de fichiers local (3 avantages concrets pour 50Go/jour)
- [ ] **Q2** — NameNode SPOF : impact panne, HDFS HA, rôle du Journal Node
- [ ] **Q3** — HdfsSensor poke vs reschedule : impact workers, scénario de blocage
- [ ] **Q4** — Réplication HDFS facteur 3 : écriture d'un bloc 128Mo, cohérence lectures concurrentes

---

## 🎯 Phase 5 — Exercices du cours (Slides — `apache_airflow_k8s_docker_hdfs_Jour2.pdf`)

### Exercice Slides 1 — DAG avec branchement (`dag_branchement.py`)
- [ ] Générer un nombre aléatoire
- [ ] Si pair → tâche "pair", si impair → tâche "impair"
- [ ] Converger vers une tâche finale
- [ ] **Bonus** : `TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS` sur la tâche finale

### Exercice Slides 2 — Communication XCom (`dag_xcom.py`)
- [ ] Tâche 1 : générer liste de 5 nombres → return (auto XCom)
- [ ] Tâche 2 : xcom_pull la liste → calculer la somme
- [ ] Tâche 3 : afficher le résultat final

### Exercice Slides 3 — Sensor personnalisé (`dag_sensor.py`)
- [ ] FileSensor qui attend `/tmp/go.txt`
- [ ] Mode `reschedule`
- [ ] Déclencher la suite du workflow quand le fichier apparaît

---

## ⭐ Phase 6 — Exercices Supplémentaires (TP HDFS)

### Exercice Supp. 1 — HdfsSensor custom (⭐⭐)
- [ ] Créer `plugins/hdfs_sensor.py` avec classe `HdfsFileSensor`
- [ ] Méthode `poke()` via WebHDFS `op=GETFILESTATUS`
- [ ] Intégrer dans le DAG entre `stocker_hdfs` et `analyser_logs`
- [ ] Tester : fichier présent (immédiat) + fichier absent (2 poke cycles)
- [ ] Comparer modes poke vs reschedule

### Exercice Supp. 2 — Compaction hebdomadaire (⭐⭐⭐)
- [ ] Créer `dags/logs_compaction_dag.py` (schedule : lundi 3h)
- [ ] `lister_fichiers_semaine()` via WebHDFS LISTSTATUS
- [ ] `fusionner_fichiers()` → `/logs/ecommerce/weekly/YYYY-WXX.log`
- [ ] `valider_compaction()` → vérifier nb lignes
- [ ] `supprimer_fichiers_journaliers()` → cleanup
- [ ] **Bonus** : ExternalTaskSensor pour dépendance inter-DAG

### Exercice Supp. 3 — Retry exponentiel (⭐⭐)
- [ ] Configurer retry exponentiel (4 retries, 30s initial, max 5min)
- [ ] Créer `tache_instable` : échoue 2 premières tentatives, réussit à la 3e
- [ ] Observer dans l'UI : `up_for_retry → up_for_retry → success`
- [ ] **Bonus** : `on_retry_callback` pour logger tentative + délai

---

## 📸 Phase 7 — Captures d'écran (Livrables)

- [ ] `docker compose ps` → tous conteneurs running/healthy
- [ ] `hdfs dfsadmin -report` → 1 Live DataNode
- [ ] Web UI HDFS → Browse Directory `/data/ecommerce/logs/raw/`
- [ ] Airflow UI → Vue Graph du DAG (8 tâches)
- [ ] Airflow UI → Exécution complète (branche verte + branche grisée)
- [ ] Logs `analyser_logs_hdfs` → status codes + Top 5 URLs
- [ ] Web UI HDFS → fichier dans `/data/ecommerce/logs/processed/`

---

## 📌 Ordre de travail recommandé

| Priorité | Phase | Description | Statut |
|:---:|:---:|---|:---:|
| 1 | Phase 1 | Infrastructure Docker (Airflow + HDFS) | ✅ |
| 2 | Phase 2 | Script `generer_logs.py` | ✅ |
| 3 | Phase 3 | DAG principal (8 tâches) | ✅ |
| 4 | Phase 5 | Exercices slides (branchement, XCom, sensor) | ✅ |
| 5 | Phase 4 | Questions de réflexion `REPONSES.md` | ✅ |
| 6 | Phase 6 | Exercices supplémentaires (⭐⭐ et ⭐⭐⭐) | ✅ |
| 7 | Phase 7 | Captures d'écran | ⬜ |

---

## 🛠️ Technologies utilisées

| Outil | Version | Rôle |
|---|---|---|
| Apache Airflow | 2.8 | Orchestration du pipeline |
| Apache Hadoop (HDFS) | 3.2.1 | Stockage distribué des logs |
| Docker Compose | v2 | Déploiement local du stack |
| PostgreSQL | 13 | Base de données Airflow |
| Python | 3.x | Scripts, opérateurs, logique métier |

---

> **Prêt à commencer !** Lancer Phase 1 → Tâche 1.1
