# RÉPONSES — TP Jour 2 : Logs E-Commerce avec HDFS

---

## Q1 — HDFS vs système de fichiers local

**Pourquoi ne pas simplement stocker les logs sur le disque local du serveur Airflow ou sur un NFS ?**

3 avantages concrets de HDFS pour un cas d'usage de 50 Go/jour de logs :

1. **Scalabilité horizontale (Distribution)** : HDFS distribue les données sur plusieurs DataNodes. À 50 Go/jour, soit ~18 To/an, un seul serveur atteindrait vite sa limite de stockage. Avec HDFS, il suffit d'ajouter des DataNodes pour augmenter la capacité sans migration de données ni arrêt de service. Un NFS reste un point unique de stockage.

2. **Tolérance aux pannes (Réplication)** : HDFS réplique chaque bloc de données sur 3 DataNodes par défaut (facteur de réplication = 3). Si un disque ou un serveur tombe, les données restent accessibles depuis les répliques. Un disque local ou NFS sans RAID offre 0 redondance — une panne = perte de données.

3. **Localité des données (Data Locality)** : HDFS permet aux frameworks de traitement (Spark, Hive, MapReduce) d'exécuter les calculs directement sur les nœuds qui stockent les données. Pour 50 Go/jour de logs, cela évite de transférer les fichiers sur le réseau avant traitement. Un NFS centralise tout le I/O sur un seul serveur, créant un goulot d'étranglement réseau.

---

## Q2 — NameNode, point de défaillance unique (SPOF)

**Si le NameNode tombe, que se passe-t-il ?**

- **Pour les DataNodes** : Ils continuent à fonctionner, mais ne peuvent plus recevoir de nouvelles instructions d'écriture/lecture. Ils envoient des heartbeats au NameNode — sans réponse, ils entrent en mode dégradé. Les données stockées sur les DataNodes restent intactes physiquement.
- **Pour les clients** : Toutes les opérations HDFS (lecture, écriture, listing) sont **impossibles**. Le NameNode est le seul point d'entrée pour résoudre les chemins de fichiers vers les blocs sur les DataNodes. Le cluster entier est **inaccessible**.

**Mécanismes Hadoop HA (High Availability) :**

- **HDFS NameNode HA** : Déploiement de 2 NameNodes — un **Active** et un **Standby**. Le Standby maintient une copie synchronisée des métadonnées et peut prendre le relais instantanément en cas de panne (failover automatique via ZooKeeper).
- **Journal Nodes** : Cluster de 3+ nœuds qui stockent les **edit logs** (journal des modifications du namespace). Le NameNode Active écrit ses modifications dans les Journal Nodes. Le NameNode Standby les lit en continu pour rester synchronisé. Cela remplace le mécanisme NFS partagé utilisé dans les anciennes versions.
- **ZooKeeper** : Coordonne l'élection du NameNode Active et détecte les pannes via le **ZKFC** (ZooKeeper Failover Controller) qui tourne sur chaque NameNode.

---

## Q3 — HdfsSensor : mode poke vs reschedule

**Comparaison :**

| Critère | Mode `poke` | Mode `reschedule` |
|---|---|---|
| Comportement | Le worker reste occupé en continu, vérifie à chaque `poke_interval` | Le worker est libéré entre chaque vérification |
| Slot worker | **Bloqué** pendant toute la durée du sensor | **Libéré** entre deux pokes |
| Ressources | Consomme 1 slot worker en permanence | Économe — le slot est disponible pour d'autres tâches |
| Cas d'usage | Attente courte (< 5 min) | Attente longue ou indéterminée |

**Scénario de blocage :** Avec un CeleryExecutor configuré avec 8 workers et 8 sensors en mode `poke` qui attendent chacun un fichier pendant 30 minutes, **tous les slots sont occupés** par des tâches qui ne font rien. Aucune autre tâche du scheduler ne peut être exécutée → le pipeline entier est **bloqué/deadlocked**. En mode `reschedule`, les slots seraient libérés entre chaque poke et d'autres tâches pourraient s'exécuter.

**Recommandation production :** Toujours utiliser `mode="reschedule"` avec CeleryExecutor pour les attentes > 1 minute.

---

## Q4 — Réplication HDFS et cohérence des données

**Écriture d'un bloc de 128 Mo avec facteur de réplication 3 :**

1. Le client contacte le **NameNode** pour signaler une écriture
2. Le NameNode sélectionne **3 DataNodes** (en tenant compte du topology awareness : rack différent si possible)
3. L'écriture se fait en **pipeline** :
   - Le client envoie le bloc au **DataNode 1**
   - DataNode 1 le transmet simultanément au **DataNode 2**
   - DataNode 2 le transmet au **DataNode 3**
4. Les **3 copies** sont écrites en parallèle (pipeline), pas séquentiellement
5. Un **accusé de réception (ACK)** remonte la chaîne : DN3 → DN2 → DN1 → Client
6. Le client confirme au NameNode que l'écriture est terminée

**Résultat** : 3 copies du bloc de 128 Mo sont écrites sur 3 DataNodes distincts.

**Cohérence lors d'une lecture concurrente :**

- HDFS garantit un modèle **write-once-read-many** : un fichier en cours d'écriture n'est **visible** pour les lecteurs qu'après l'appel `hflush()` ou `close()` par l'écrivain.
- Avant la fermeture, seuls les blocs complètement écrits et répliqués sont visibles.
- HDFS offre une **cohérence forte** (strong consistency) : une fois le fichier fermé, tous les lecteurs voient la même version complète du fichier.
- Il n'y a **pas de lecture partielle** d'un bloc en cours d'écriture (sauf usage explicite de `hflush`).

---

## Captures d'écran

*(À compléter après exécution du pipeline)*

1. `docker compose ps` — tous les conteneurs running/healthy
2. `hdfs dfsadmin -report` — 1 Live DataNode
3. Web UI HDFS (http://localhost:9870) — Browse Directory `/data/ecommerce/logs/raw/`
4. Airflow UI — Vue Graph du DAG `logs_ecommerce_dag` (8 tâches)
5. Airflow UI — Exécution complète avec les 2 branches visibles
6. Logs de la tâche `analyser_logs_hdfs` — status codes + Top 5 URLs
7. Web UI HDFS — fichier dans `/data/ecommerce/logs/processed/`
