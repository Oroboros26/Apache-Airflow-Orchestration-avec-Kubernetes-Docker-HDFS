# 📘 TP Jour 2 — Explication Complète et Réponses

> **Formation** : Data Engineering — Apache Airflow  
> **École** : IPSSI Montpellier — M2  
> **Thème** : Ingestion et analyse de logs Apache dans HDFS via Airflow  
> **Branche Git** : `mohamed_dev_airflow`

---

## 📌 Table des matières

1. [Ce que nous avons fait](#1--ce-que-nous-avons-fait)
2. [Architecture mise en place](#2--architecture-mise-en-place)
3. [Le DAG principal — Explication détaillée](#3--le-dag-principal--explication-détaillée)
4. [Les exercices réalisés](#4--les-exercices-réalisés)
5. [Questions de réflexion (Q1–Q4)](#5--questions-de-réflexion-q1q4)
6. [Questions Checkpoint Jour 2](#6--questions-checkpoint-jour-2)
7. [Questions supplémentaires possibles](#7--questions-supplémentaires-possibles)

---

## 1 — Ce que nous avons fait

### Résumé global

Nous avons construit un **pipeline d'ingestion de logs e-commerce** qui simule le contexte d'une marketplace (type Cdiscount). Le pipeline :

1. **Génère** des logs Apache réalistes (1000 lignes/jour)
2. **Uploade** ces logs vers HDFS (Hadoop Distributed File System)
3. **Vérifie** la présence du fichier dans HDFS
4. **Analyse** les logs (status codes, top URLs, taux d'erreur)
5. **Branche** selon le taux d'erreur (alerte si > 5%)
6. **Archive** en déplaçant le fichier de la zone `raw/` vers `processed/`

### Fichiers créés

```
ecommerce-logs-pipeline/
├── docker-compose.yaml          # Stack complète : Airflow + HDFS
├── hadoop.env                   # Configuration Hadoop partagée
├── .env                         # UID Airflow
├── REPONSES.md                  # Réponses aux 4 questions
├── dags/
│   ├── logs_ecommerce_dag.py    # DAG principal (8 tâches)
│   ├── dag_branchement.py       # Exercice 1 : pair/impair
│   ├── dag_xcom.py              # Exercice 2 : communication XCom
│   ├── dag_sensor.py            # Exercice 3 : FileSensor
│   └── logs_compaction_dag.py   # Exercice Supp. 2 : compaction
├── plugins/
│   └── hdfs_sensor.py           # Exercice Supp. 1 : sensor custom
├── scripts/
│   └── generer_logs.py          # Générateur de logs Apache
└── logs/                        # Logs Airflow
```

### Technologies utilisées

| Outil | Version | Rôle |
|---|---|---|
| Apache Airflow | 2.8 | Orchestration du pipeline |
| Apache Hadoop HDFS | 3.2.1 | Stockage distribué |
| Docker Compose | v2 | Conteneurisation |
| PostgreSQL | 13 | Base de données Airflow |
| WebHDFS API | REST | Communication Airflow ↔ HDFS |

---

## 2 — Architecture mise en place

### Architecture Lambda simplifiée

```
Sources              Ingestion           Stockage          Traitement        Consommation
─────────           ─────────           ─────────          ──────────       ────────────
Serveurs web   →    Airflow DAG    →    HDFS (raw)    →    Analyse     →    Alertes Ops
(logs Apache)       (ce TP)             (brut)             (dans DAG)       Rapports
```

### Infrastructure Docker (5 conteneurs)

```
┌──────────────────────────────────────────────────────────┐
│                    Docker Network                         │
│                                                          │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │  PostgreSQL  │  │   Airflow    │  │   Airflow    │   │
│  │   (port 5432)│  │  Webserver   │  │  Scheduler   │   │
│  │   Base de    │  │  (port 8080) │  │  (exécute    │   │
│  │   données    │  │  Interface   │  │  les tâches) │   │
│  └─────────────┘  └──────────────┘  └──────┬───────┘   │
│                                             │            │
│                                      /shared/ (volume)   │
│                                             │            │
│  ┌──────────────┐  ┌──────────────┐         │           │
│  │   NameNode   │  │   DataNode   │         │           │
│  │  (port 9870) │  │  (stockage)  │←────────┘           │
│  │  Maître HDFS │  │  Données     │  WebHDFS API        │
│  └──────────────┘  └──────────────┘                     │
└──────────────────────────────────────────────────────────┘
```

### Zones HDFS

```
/data/ecommerce/logs/
├── raw/          ← Zone d'atterrissage (fichiers bruts)
├── processed/    ← Zone après traitement (fichiers archivés)
└── weekly/       ← Zone compaction hebdomadaire
```

**Règle d'or** : On ne supprime **jamais** les données de la zone `raw`. Si un traitement a un bug, on peut toujours le rejouer depuis les données brutes.

### Pourquoi WebHDFS au lieu de `docker exec` ?

Le TP original suggère d'utiliser `docker exec namenode hdfs dfs ...` depuis les conteneurs Airflow. Cela pose un problème : **le binaire `docker` n'est pas disponible à l'intérieur des conteneurs Airflow**. 

Notre solution : utiliser l'**API REST WebHDFS** (port 9870) qui est accessible via le réseau Docker interne. C'est d'ailleurs la méthode recommandée en production.

| Opération | Endpoint WebHDFS |
|---|---|
| Créer un fichier | `PUT /webhdfs/v1{path}?op=CREATE` |
| Lire un fichier | `GET /webhdfs/v1{path}?op=OPEN` |
| Vérifier existence | `GET /webhdfs/v1{path}?op=GETFILESTATUS` |
| Renommer/Déplacer | `PUT /webhdfs/v1{path}?op=RENAME&destination=...` |
| Lister un répertoire | `GET /webhdfs/v1{path}?op=LISTSTATUS` |
| Supprimer | `DELETE /webhdfs/v1{path}?op=DELETE` |

---

## 3 — Le DAG principal — Explication détaillée

### Schéma du pipeline (8 tâches)

```
generer_logs_journaliers     ← PythonOperator : génère 1000 lignes de logs
        │
        ▼
uploader_vers_hdfs           ← PythonOperator : upload vers HDFS via WebHDFS
        │
        ▼
hdfs_file_sensor             ← PythonOperator : vérifie que le fichier existe
        │
        ▼
analyser_logs_hdfs           ← PythonOperator : analyse status codes + taux d'erreur
        │
        ▼
brancher_selon_taux_erreur   ← BranchPythonOperator : décide selon taux > 5%
        │
   ┌────┴────┐
   ▼         ▼
alerter    archiver           ← Une branche s'exécute, l'autre est "skipped"
equipe_ops rapport_ok
   │         │
   └────┬────┘
        ▼
archiver_logs_hdfs           ← PythonOperator : déplace raw → processed
                               trigger_rule="none_failed_min_one_success"
```

### Explication de chaque tâche

#### Tâche 1 : `generer_logs_journaliers`
- **Type** : `PythonOperator`
- **Action** : Appelle le script `generer_logs.py` via `subprocess.run()`
- **Utilise** `context["ds"]` (data stamp) au lieu de `date.today()` pour garantir l'**idempotence**
- **Retourne** le chemin du fichier → automatiquement stocké dans **XCom** (clé `return_value`)

#### Tâche 2 : `uploader_vers_hdfs`
- **Type** : `PythonOperator`
- **Action** : Upload le fichier vers HDFS via l'API WebHDFS en 2 étapes :
  1. `PUT` vers le NameNode → reçoit une redirection 307
  2. `PUT` vers le DataNode avec les données

#### Tâche 3 : `hdfs_file_sensor`
- **Type** : `PythonOperator`
- **Action** : Vérifie que le fichier existe dans HDFS (`GETFILESTATUS`)
- **Rôle** : Sécurité — ne pas analyser un fichier pas encore totalement écrit

#### Tâche 4 : `analyser_logs_hdfs`
- **Type** : `PythonOperator`
- **Action** : Lit le fichier depuis HDFS, calcule :
  - Distribution des **status codes** (200, 404, 500, etc.)
  - **Top 5 URLs** les plus visitées
  - **Taux d'erreur** = (requêtes 4xx + 5xx) / total × 100
- **Sauvegarde** le taux dans un fichier pour la tâche suivante

#### Tâche 5 : `brancher_selon_taux_erreur`
- **Type** : `BranchPythonOperator`
- **Logique** : Si taux d'erreur > 5% → retourne `"alerter_equipe_ops"`, sinon → retourne `"archiver_rapport_ok"`
- **Principe** : Le BranchPythonOperator **doit retourner un `task_id`** (ou liste de `task_id`). Les branches non retournées sont mises en état `skipped`.

#### Tâche 6 : `archiver_logs_hdfs`
- **Type** : `PythonOperator`
- **Action** : Déplace le fichier de `raw/` vers `processed/` via WebHDFS `RENAME`
- **`trigger_rule="none_failed_min_one_success"`** : S'exécute quelle que soit la branche choisie (il suffit qu'un parent ait réussi et qu'aucun n'ait échoué)

### Résultat de l'exécution

```
generer_logs_journaliers   : ✅ success
uploader_vers_hdfs         : ✅ success
hdfs_file_sensor           : ✅ success
analyser_logs_hdfs         : ✅ success
brancher_selon_taux_erreur : ✅ success  → a choisi "alerter_equipe_ops"
alerter_equipe_ops         : ✅ success
archiver_rapport_ok        : ⏭️ skipped  (branche non prise)
archiver_logs_hdfs         : ✅ success  (s'exécute grâce au trigger_rule)
```

---

## 4 — Les exercices réalisés

### Exercice 1 — DAG avec branchement (`dag_branchement.py`)

**Objectif** : Générer un nombre aléatoire, exécuter une branche pair ou impair, converger vers une tâche finale.

**Solution** :
- `generer_nombre` : génère un entier aléatoire, le retourne (→ XCom)
- `choisir_branche` : `BranchPythonOperator` qui lit le nombre via `xcom_pull` et retourne `"traitement_pair"` ou `"traitement_impair"`
- `tache_finale` : utilise `TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS` pour s'exécuter dans tous les cas

**Pourquoi `NONE_FAILED_MIN_ONE_SUCCESS` ?** Sans cette trigger rule, la tâche finale ne s'exécuterait pas car l'une des deux branches est toujours `skipped`. Le trigger rule par défaut (`ALL_SUCCESS`) exige que **tous** les parents soient en `success`.

**Statut** : ✅ Exécuté avec succès

---

### Exercice 2 — Communication XCom (`dag_xcom.py`)

**Objectif** : Démontrer l'échange de données entre tâches via XCom.

**Solution** :
- `generer_liste` : retourne une liste de 5 nombres → automatiquement poussé dans XCom
- `calculer_somme` : `ti.xcom_pull(task_ids="generer_liste")` → récupère la liste, calcule la somme
- `afficher_resultat` : récupère les deux valeurs et affiche

**Point clé** : Tout `return` d'un `PythonOperator` est automatiquement stocké dans XCom avec la clé `return_value`. Pas besoin d'appeler `xcom_push` explicitement.

**Statut** : ✅ Exécuté avec succès

---

### Exercice 3 — FileSensor (`dag_sensor.py`)

**Objectif** : Attendre l'apparition du fichier `/tmp/go.txt` avant de continuer.

**Solution** :
- `FileSensor` avec `mode="reschedule"` et `poke_interval=10`
- Une tâche de traitement qui s'exécute après détection

**Pourquoi `mode='reschedule'`** : Libère le slot du worker entre chaque vérification, contrairement à `mode='poke'` qui bloque le worker en continu.

---

### Exercice Supp. 1 — HdfsSensor custom (`plugins/hdfs_sensor.py`)

**Objectif** : Créer un sensor personnalisé qui utilise l'API WebHDFS.

**Solution** : Classe `HdfsFileSensor` héritant de `BaseSensorOperator` avec une méthode `poke()` qui fait un `GET` sur `/webhdfs/v1{path}?op=GETFILESTATUS`.

---

### Exercice Supp. 2 — Compaction hebdomadaire (`logs_compaction_dag.py`)

**Objectif** : Résoudre le problème des "small files" HDFS en compactant 7 fichiers journaliers en 1 fichier hebdomadaire.

**Solution** : DAG avec 4 tâches utilisant le **TaskFlow API** (`@task`) :
1. `lister_fichiers_semaine` — LISTSTATUS via WebHDFS
2. `fusionner_fichiers` — Lecture + concaténation + CREATE
3. `valider_compaction` — Vérification du nombre de lignes
4. `supprimer_fichiers_journaliers` — DELETE via WebHDFS

**Pourquoi les small files sont un problème** : Le NameNode stocke les métadonnées de **chaque fichier** en mémoire RAM. Des milliers de petits fichiers saturent la mémoire du NameNode, alors qu'un seul gros fichier n'occupe qu'une entrée.

---

## 5 — Questions de réflexion (Q1–Q4)

### Q1 — HDFS vs système de fichiers local

**Question** : Pourquoi ne pas stocker les logs sur le disque local ou un NFS ? Listez 3 avantages concrets de HDFS pour 50 Go/jour.

**Réponse** :

1. **Scalabilité horizontale** : HDFS distribue les données sur N DataNodes. À 50 Go/jour (~18 To/an), il suffit d'ajouter des machines. Un disque local atteint vite sa limite ; un NFS centralise tout sur un seul serveur.

2. **Tolérance aux pannes (réplication)** : Chaque bloc est répliqué sur 3 DataNodes. Si un serveur tombe → les données restent accessibles depuis les répliques. Un disque local sans RAID = 0 redondance.

3. **Localité des données** : Spark/Hive/MapReduce exécutent les calculs **sur les nœuds qui stockent les données**, évitant les transferts réseau massifs. Avec un NFS, les 50 Go transitent par le réseau à chaque traitement.

---

### Q2 — NameNode SPOF

**Question** : Si le NameNode tombe, que se passe-t-il ? Quels mécanismes HA existent ?

**Réponse** :

**Impact d'une panne** :
- **DataNodes** : continuent de fonctionner, mais ne reçoivent plus d'instructions. Les données physiques restent intactes.
- **Clients** : **toutes les opérations HDFS sont impossibles**. Le NameNode est le seul point d'entrée pour résoudre chemin de fichier → blocs sur DataNodes.

**Mécanismes HA** :
- **NameNode HA** : 2 NameNodes — un **Active** et un **Standby**. Failover automatique via **ZooKeeper**.
- **Journal Nodes** : Cluster de 3+ nœuds qui stockent les **edit logs** (journal des modifications). Le Standby les lit en continu pour rester synchronisé.
- **ZKFC** (ZooKeeper Failover Controller) : Détecte la panne du NameNode Active et déclenche le basculement vers le Standby.

---

### Q3 — HdfsSensor : mode poke vs reschedule

**Question** : Comparez les deux modes. Quand utiliser l'un ou l'autre ? Impact sur les workers ?

**Réponse** :

| Aspect | Mode `poke` | Mode `reschedule` |
|---|---|---|
| Worker | **Bloqué** en continu | **Libéré** entre chaque vérification |
| Slot | Occupé pendant toute l'attente | Disponible pour d'autres tâches |
| Adapté pour | Attentes courtes (< 5 min) | Attentes longues ou incertaines |
| Overhead | Aucun (boucle simple) | Léger (re-scheduling à chaque poke) |

**Scénario de blocage** : Avec 8 workers Celery et 8 sensors en mode `poke` attendant 30 min chacun → **deadlock** : aucun slot libre pour les autres tâches. En `reschedule`, les slots se libèrent entre chaque poke.

**Recommandation** : Toujours `reschedule` en production avec CeleryExecutor pour les attentes > 1 minute.

---

### Q4 — Réplication HDFS (facteur 3)

**Question** : Écriture d'un bloc de 128 Mo avec facteur 3 : combien de copies, sur combien de DataNodes, dans quel ordre ? Cohérence en lecture concurrente ?

**Réponse** :

**Processus d'écriture en pipeline** :
1. Le client contacte le NameNode → celui-ci choisit **3 DataNodes** (en respectant le rack-awareness)
2. Le client envoie le bloc au **DataNode 1**
3. DataNode 1 le retransmet au **DataNode 2** (en parallèle de la réception)
4. DataNode 2 le retransmet au **DataNode 3**
5. Les **ACK** remontent : DN3 → DN2 → DN1 → Client
6. Le client confirme au NameNode

**Résultat** : **3 copies** du bloc de 128 Mo sur **3 DataNodes distincts** (si possible sur des racks différents).

**Cohérence en lecture** :
- HDFS suit le modèle **write-once-read-many**
- Un fichier en cours d'écriture n'est **visible** qu'après `hflush()` ou `close()`
- Après fermeture → **cohérence forte** : tous les lecteurs voient la même version
- **Pas de lecture partielle** d'un bloc non finalisé

---

## 6 — Questions Checkpoint Jour 2

### C1 — Différence entre Operator et Task Instance ?

- **Operator** = un **template** (une classe Python) qui définit **quoi faire**. Exemples : `PythonOperator`, `BashOperator`, `BranchPythonOperator`.
- **Task Instance** = une **exécution concrète** d'un Operator pour une date donnée (`execution_date`). C'est l'Operator + un contexte d'exécution (date, tentative n°, etc.)

**Analogie** : L'Operator est la *recette de cuisine*, la Task Instance est le *plat préparé un jour donné*.

```python
# Ceci est un Operator (template)
t = PythonOperator(task_id="ma_tache", python_callable=ma_fonction)

# Quand Airflow l'exécute pour le 2026-04-09, il crée une Task Instance :
# TaskInstance(dag_id="mon_dag", task_id="ma_tache", execution_date="2026-04-09")
```

---

### C2 — Quand utiliser BranchPythonOperator ?

**Quand** : Lorsque le flux d'exécution doit **varier dynamiquement** selon une condition calculée à l'exécution.

**Exemples** :
- Taux d'erreur > seuil → alerter vs archiver (notre TP)
- Qualité des données : valides → transformer, invalides → notifier
- Jour de la semaine : lundi → rapport hebdo, autre jour → rapport quotidien

**Comment** : La fonction retourne le **`task_id`** (ou liste) de la branche à exécuter. Les autres branches sont automatiquement mises en état `skipped`.

**Attention** : La tâche de convergence (après les branches) doit utiliser `trigger_rule="none_failed_min_one_success"` sinon elle ne s'exécutera pas.

---

### C3 — Bon usage de XCom ?

**À faire** :
- Passer des **petites données** : chemins de fichiers, IDs, compteurs, statuts
- Utiliser `return` dans un PythonOperator (auto-push avec clé `return_value`)
- Récupérer avec `ti.xcom_pull(task_ids="source_task")`

**À ne PAS faire** :
- Ne **jamais** passer de DataFrames, fichiers volumineux ou gros objets via XCom
- Ne **jamais** stocker de mots de passe ou secrets (visibles dans l'UI)
- Limite de taille : PostgreSQL ~1 Go, MySQL ~64 Ko

**Bon pattern** : Stocker les **données** sur S3/GCS/HDFS et passer l'**URI/chemin** via XCom.

```python
# ✅ Bon : passer un chemin
return "/data/ecommerce/logs/raw/access_2026-04-09.log"

# ❌ Mauvais : passer le contenu du fichier
with open("fichier.log") as f:
    return f.read()  # TROP GROS !
```

---

### C4 — Mode reschedule : avantages ?

**Avantages** :
1. **Économie de workers** : le slot est libéré entre chaque poke, permettant à d'autres tâches de s'exécuter
2. **Pas de deadlock** : impossible de bloquer tous les workers avec des sensors en attente
3. **Meilleure utilisation des ressources** : surtout avec CeleryExecutor où les slots sont limités

**Inconvénient** : Léger overhead de re-scheduling (le scheduler doit re-planifier la tâche à chaque intervalle).

**Quand utiliser `poke`** : Seulement pour des attentes très courtes (< 1-2 minutes) où le coût de re-scheduling n'est pas justifié.

---

## 7 — Questions supplémentaires possibles

### Qu'est-ce qu'un DAG ?

Un **DAG** (Directed Acyclic Graph = Graphe Acyclique Dirigé) est un ensemble de tâches avec des dépendances. "Acyclique" signifie **pas de boucle** : une tâche ne peut pas dépendre d'elle-même (directement ou indirectement).

Dans Airflow, un DAG définit :
- **Quelles tâches** exécuter
- **Dans quel ordre** (dépendances via `>>` ou `<<`)
- **Quand** (schedule avec cron ou timedelta)

---

### Qu'est-ce que `catchup` ?

`catchup=True` (défaut) : Airflow exécute **toutes les runs manquées** entre `start_date` et maintenant. Si `start_date=2025-01-01` et schedule `@daily`, au démarrage Airflow crée ~460 runs.

`catchup=False` : Airflow ne lance que la **dernière run** manquée. C'est ce qu'on utilise pour ce TP.

---

### Pourquoi `context["ds"]` au lieu de `date.today()` ?

Pour garantir l'**idempotence**. Si Airflow doit **rejouer** la tâche du 10 mars (après un échec par exemple), `context["ds"]` renvoie `"2026-03-10"` même si on est le 15 avril. Avec `date.today()`, on aurait `"2026-04-15"` → les mauvais logs seraient générés.

---

### Qu'est-ce que l'idempotence ?

Une tâche est **idempotente** si l'exécuter 1 fois ou N fois produit le **même résultat**. C'est crucial pour les rejeux :
- Utiliser `context["ds"]` pour les dates
- Utiliser `overwrite=true` dans WebHDFS pour ne pas échouer si le fichier existe déjà
- Ne pas incrémenter de compteurs globaux

---

### Expliquez le `trigger_rule` utilisé

Par défaut, une tâche ne s'exécute que si **tous ses parents sont en `success`** (`ALL_SUCCESS`).

Avec un `BranchPythonOperator`, une branche est toujours `skipped`. La tâche finale a donc un parent `success` et un parent `skipped` → `ALL_SUCCESS` échoue.

Solution : `trigger_rule="none_failed_min_one_success"` → la tâche s'exécute si **aucun parent n'a échoué** ET **au moins un a réussi**.

Autres trigger rules utiles :
| Rule | Condition |
|---|---|
| `ALL_SUCCESS` | Tous les parents en success |
| `ONE_SUCCESS` | Au moins un parent en success |
| `ALL_DONE` | Tous les parents terminés (peu importe l'état) |
| `NONE_FAILED` | Aucun parent en échec (skipped OK) |
| `ALL_FAILED` | Tous les parents en échec |

---

### Qu'est-ce que WebHDFS ?

L'**API REST** de HDFS, accessible sur le port **9870** du NameNode. Elle permet d'interagir avec HDFS via des requêtes HTTP standard (GET, PUT, DELETE) sans avoir besoin du client Java/CLI hdfs.

Avantages :
- Pas besoin d'installer le client Hadoop
- Compatible avec n'importe quel langage (Python, curl, etc.)
- Fonctionne à travers les réseaux Docker (les conteneurs Airflow accèdent directement au NameNode)

---

### Pourquoi un volume partagé entre Airflow et namenode ?

Les conteneurs Docker ont des **systèmes de fichiers isolés**. Le fichier `/tmp/access_2026-04-09.log` créé dans le conteneur airflow-scheduler **n'existe pas** dans le conteneur namenode.

Solution : un **volume Docker** (`shared-data`) monté sur `/shared/` dans les deux conteneurs. Le fichier écrit par Airflow est immédiatement visible par namenode.

---

### Comment fonctionne l'écriture WebHDFS en 2 étapes ?

```
1. Client → NameNode : PUT /webhdfs/v1/chemin?op=CREATE
   ← NameNode répond 307 (redirect) avec l'URL du DataNode

2. Client → DataNode : PUT <redirect_url> + données
   ← DataNode répond 201 (created)
```

Le NameNode ne stocke **jamais** les données. Il ne fait que diriger le client vers le bon DataNode.

---

### Qu'est-ce qu'un Sensor ? Donnez des exemples.

Un **Sensor** est un opérateur spécial qui **attend** qu'une condition soit vraie avant de continuer. Il appelle la méthode `poke()` à intervalles réguliers (`poke_interval`).

| Sensor | Attend quoi |
|---|---|
| `FileSensor` | L'apparition d'un fichier sur le système de fichiers |
| `HdfsSensor` | La présence d'un fichier dans HDFS |
| `HttpSensor` | Qu'un endpoint HTTP retourne un code 200 |
| `ExternalTaskSensor` | La fin d'une tâche dans un autre DAG |
| `S3KeySensor` | La présence d'un objet dans un bucket S3 |

---

### Qu'est-ce qu'un Hook ? Différence avec un Operator ?

- **Hook** : Gère la **connexion** à un système externe (DB, API, Cloud). Encapsule les credentials via les Connections Airflow. N'exécute pas de logique métier.
- **Operator** : Exécute une **action** en utilisant un Hook pour se connecter.

Exemples :
```python
# Hook : gère la connexion
hook = PostgresHook(postgres_conn_id="ma_base")
records = hook.get_records("SELECT * FROM users")

# Operator : utilise le hook en interne
task = PostgresOperator(sql="INSERT INTO ...", postgres_conn_id="ma_base")
```

---

### Quelle combinaison retries pour une tâche SLA critique ?

Pour une tâche HDFS critique :
```python
default_args = {
    "retries": 4,                              # 4 tentatives
    "retry_delay": timedelta(seconds=30),       # Délai initial 30s
    "retry_exponential_backoff": True,          # 30s → 60s → 120s → 240s
    "max_retry_delay": timedelta(minutes=10),   # Plafond à 10 min
    "execution_timeout": timedelta(minutes=30), # Timeout global
}
```

**Raisonnement** :
- **4 retries** : suffisant pour survivre à une panne temporaire (redémarrage HDFS, latence réseau)
- **Exponentiel** : évite de surcharger un système déjà en difficulté
- **Plafond 10 min** : ne pas attendre indéfiniment entre les tentatives
- **Timeout 30 min** : empêche une tâche zombi de bloquer un slot indéfiniment

---

### Expliquez le format Combined Log Format

```
92.184.12.44 - - [09/Apr/2026:14:23:45 +0100] "GET /produit/smartphone HTTP/1.1" 200 48200 "https://google.fr" "Mozilla/5.0..."
│              │   │                              │                                 │   │      │                    │
│              │   │                              │                                 │   │      │                    └─ User-Agent
│              │   │                              │                                 │   │      └─ Referrer
│              │   │                              │                                 │   └─ Taille réponse (octets)
│              │   │                              │                                 └─ Status code HTTP
│              │   │                              └─ Requête (méthode + URL + protocole)
│              │   └─ Timestamp
│              └─ Identité (toujours "-")
└─ Adresse IP du client
```

**Status codes** :
- **2xx** : Succès (200 OK)
- **4xx** : Erreur client (404 Not Found, 403 Forbidden)
- **5xx** : Erreur serveur (500 Internal Server Error, 503 Service Unavailable)

---

*TP Jour 2 — Formation Data Engineering — IPSSI Montpellier — 2025-2026*
