# RÉPONSES — TP Noté Jour 4 : Data Platform Santé Publique ARS Occitanie

---

## Q1 — Pourquoi CeleryExecutor plutôt que LocalExecutor ?

**3 avantages concrets de CeleryExecutor pour ce cas d'usage :**

### 1. Parallélisme réel des tâches

Avec **LocalExecutor**, les tâches s'exécutent dans des sous-processus du scheduler — limité par les ressources d'un seul conteneur. Avec **CeleryExecutor**, chaque tâche est sérialisée et envoyée dans la file Redis, puis consommée par un **worker Celery dédié**. Plusieurs tâches peuvent s'exécuter simultanément sur des workers distincts.

Pour l'ARS, c'est crucial quand le pipeline doit traiter simultanément les données Grippe et GEA, ou quand le catchup rattrape plusieurs semaines.

### 2. Scalabilité horizontale

```bash
# Passer de 1 à 3 workers sans modifier le DAG
docker compose up -d --scale airflow-worker=3
```

En cas de pic épidémique nécessitant des calculs plus fréquents (passage de hebdomadaire à quotidien), on ajoute des workers sans toucher au code.

### 3. Résilience

Si un worker crash, la tâche est automatiquement re-routée vers un autre worker via Redis. Le scheduler et le webserver ne sont pas impactés. Avec LocalExecutor, un crash du scheduler entraîne la perte de toutes les tâches en cours.

### Quand utiliser LocalExecutor ?

Pour le développement et les tests : pas besoin de Redis, stack plus légère. En production avec des charges constantes et faibles (< 10 DAGs, < 50 tâches/jour), LocalExecutor suffit.

---

## Q2 — Comment garantir l'idempotence du pipeline ?

**Définition** : Un pipeline idempotent produit le même résultat qu'on l'exécute 1 fois ou N fois pour les mêmes paramètres d'entrée.

### Mécanismes mis en place

1. **ON CONFLICT DO UPDATE en SQL** : Chaque insertion utilise une contrainte d'unicité (`semaine, syndrome`) et met à jour les données existantes au lieu de créer un doublon :

```sql
INSERT INTO donnees_hebdomadaires (semaine, syndrome, valeur_ias, ...)
ON CONFLICT (semaine, syndrome)
DO UPDATE SET valeur_ias = EXCLUDED.valeur_ias, updated_at = CURRENT_TIMESTAMP;
```

2. **CREATE TABLE IF NOT EXISTS** : L'initialisation de la base peut être rejouée sans erreur.

3. **INSERT ... ON CONFLICT DO NOTHING** : Les données de référence (syndromes, départements) ne sont insérées qu'une seule fois.

4. **Écrasement des fichiers** : L'archivage local écrase le fichier existant (`shutil.copy2`) plutôt que de créer un nouveau fichier avec un suffixe.

5. **max_active_runs=1** : Un seul DAG Run à la fois, éliminant les conditions de course.

### Pourquoi c'est critique ?

Le TP impose `catchup=True` : si on démarre le pipeline avec une `start_date` de 3 mois en arrière, Airflow va automatiquement exécuter les DAG Runs manquants. Si une exécution échoue à mi-parcours et qu'on la relance, l'idempotence garantit qu'aucune donnée n'est dupliquée.

---

## Q3 — Expliquer le rôle de chaque service Docker Compose

| Service | Rôle | Port exposé |
|---|---|---|
| **postgres** | Base de données Airflow : stocke les métadonnées (DAG Runs, tâches, XComs, connexions) | — |
| **postgres-ars** | Base de données ARS : stocke les données de santé publique (syndromes, indicateurs, rapports) | 5433 |
| **redis** | Broker de messages Celery : transporte les tâches du scheduler vers les workers | 6379 |
| **airflow-webserver** | Interface web Airflow : visualisation des DAGs, monitoring, déclenchement des Runs | 8080 |
| **airflow-scheduler** | Cœur d'Airflow : parse les DAGs, planifie les tâches, les soumet au broker Celery | — |
| **airflow-worker** | Exécute les tâches : consomme les messages Redis et exécute le code Python | — |
| **flower** | Monitoring Celery : tableau de bord des workers, tâches en file, temps d'exécution | 5555 |
| **airflow-init** | Initialisation unique : `airflow db init` + création de l'utilisateur admin | — |

### Flux de communication

```
Scheduler → Redis (soumet les tâches) → Worker (exécute)
     ↓                                      ↓
  PostgreSQL (airflow)                  PostgreSQL (ars_epidemio)
     ↑                                      ↑
  Webserver (lit les statuts)           Worker (lit/écrit les données)
```

---

## Q4 — Comment fonctionne le BranchPythonOperator ?

Le `BranchPythonOperator` est un opérateur Airflow qui retourne le **task_id** de la tâche suivante à exécuter. Les autres branches sont automatiquement **skippées**.

### Implémentation dans ce TP

```python
def evaluer_situation_epidemique(**context) -> str:
    # Lit les statuts depuis PostgreSQL
    if nb_urgence > 0:
        return "declencher_alerte_ars"    # branche URGENCE
    elif nb_alerte > 0:
        return "envoyer_bulletin_surveillance"  # branche ALERTE
    else:
        return "confirmer_situation_normale"     # branche NORMALE
```

### Problème du rapport final

Après le branchement, le `generer_rapport_hebdomadaire` doit s'exécuter **quelle que soit la branche**. Par défaut, une tâche en aval d'une branche skippée est aussi skippée.

**Solution** : `trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS`
- La tâche s'exécute si **au moins une** tâche en amont a réussi
- Et **aucune** tâche en amont n'a échoué (les skippées ne comptent pas comme échecs)

---

## Q5 — Pourquoi séparer les scripts Python du DAG ?

### Principes respectés

1. **Séparation des responsabilités** : Le DAG (`ars_epidemio_dag.py`) définit **l'orchestration** (quoi, quand, dans quel ordre). Les scripts (`collecte_ias.py`, `calcul_indicateurs.py`) contiennent la **logique métier** (comment).

2. **Testabilité** : Les scripts peuvent être testés indépendamment d'Airflow :
```bash
python scripts/collecte_ias.py  # test unitaire sans Airflow
```

3. **Réutilisabilité** : Les scripts peuvent être utilisés par d'autres DAGs ou hors Airflow.

4. **Lisibilité** : Le DAG reste concis et lisible, avec des fonctions de 10-20 lignes qui importent les modules métier.

### Comment le DAG charge les scripts

```python
sys.path.insert(0, "/opt/airflow/scripts")
from collecte_ias import telecharger_csv_ias, filtrer_semaine, agreger_semaine
```

Le volume Docker monte `./scripts` dans `/opt/airflow/scripts`, accessible par tous les workers.
