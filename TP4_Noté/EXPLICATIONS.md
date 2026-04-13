# EXPLICATIONS — TP Noté Jour 4 : Data Platform Santé Publique ARS Occitanie

---

## 1. Architecture Docker Compose

### 1.1 Pourquoi CeleryExecutor ?

Le TP impose **CeleryExecutor** plutôt que LocalExecutor (Jour 1) ou KubernetesExecutor (Jour 3). Les raisons :

- **Parallélisme réel** : avec LocalExecutor, les tâches tournent dans le processus du scheduler. Avec CeleryExecutor, chaque tâche est envoyée à un worker Celery dédié via Redis (broker de messages). Plusieurs tâches peuvent s'exécuter simultanément sur différents workers.
- **Scalabilité horizontale** : on peut ajouter des workers (`docker compose up -d --scale airflow-worker=3`) sans modifier la config.
- **Monitoring** : Flower (port 5555) offre une interface web pour surveiller les workers, les tâches en file et les tâches actives.

### 1.2 Deux bases PostgreSQL

```
postgres (port 5432)       → base Airflow (métadonnées, DAG Runs, XComs, etc.)
postgres-ars (port 5433)   → base ars_epidemio (données de santé publique)
```

Cette séparation est une exigence RGPD : les données de santé publique ne doivent pas être mélangées avec les métadonnées techniques de l'orchestrateur. En production, la base ARS serait sur un serveur dédié avec chiffrement au repos.

### 1.3 Volume Docker nommé `ars-data`

Le volume `ars-data` est monté sur `/data/ars/` dans tous les conteneurs Airflow. Il persiste indépendamment du cycle de vie des conteneurs :

```bash
docker compose down      # les conteneurs sont supprimés
docker compose up -d     # les données sont toujours dans le volume
docker volume rm ars-data  # ← seule cette commande supprime les données
```

Structure de partitionnement temporel :
```
/data/ars/raw/2025/S15/sursaud_2025-S15.json
/data/ars/indicateurs/indicateurs_2025-S15.json
/data/ars/rapports/2025/S15/rapport_2025-S15.json
```

---

## 2. Données IAS® (Indicateurs Avancés Sanitaires)

### 2.1 Source

Les données IAS® sont produites par **OpenHealth / CELTIPHARM** à partir des ventes en pharmacie (antigrippaux, antiémétiques). Elles permettent de détecter l'activité épidémique **2 à 5 jours avant** les systèmes de surveillance classiques.

Deux datasets CSV ouverts sur data.gouv.fr :
- **Syndrome grippal** : `35f46fbb-7a97-46b3-a93c-35a471033447`
- **Gastro-entérite** : `6c415be9-4ebf-4af5-b0dc-9867bb1ec0e3`

### 2.2 Parsing du format français

Les CSV utilisent le format français :
- Séparateur de colonnes : `;` (point-virgule)
- Séparateur décimal : `,` (virgule)
- Encoding : UTF-8
- Valeurs manquantes : `NA`

Le script `collecte_ias.py` convertit les virgules décimales en points et les `NA` en `None` Python.

### 2.3 Agrégation hebdomadaire

Les données sont quotidiennes. Pour obtenir une valeur par semaine ISO (YYYY-SXX) :
1. Filtrer les lignes où la date tombe dans la semaine ISO cible
2. Calculer la **moyenne** de `Loc_Reg76` (colonne Occitanie, code INSEE 76)
3. Récupérer les moyennes des colonnes historiques (`Sais_2019_2020` à `Sais_2023_2024`)
4. Récupérer les seuils `MIN_Saison` et `MAX_Saison`

---

## 3. Indicateurs épidémiques

### 3.1 Z-score (méthode OMS adaptée)

```
z = (valeur_ias_semaine - moyenne_historique) / ecart_type_historique
```

- `moyenne_historique` et `ecart_type_historique` sont calculés sur les 5 saisons passées (colonnes `Sais_2019_2020` à `Sais_2023_2024`)
- Minimum 3 saisons requises ; sinon `z = None` → statut `NORMAL`
- `ddof=1` (écart-type corrigé, estimateur sans biais de Bessel)

### 3.2 Classification

Deux critères indépendants :

| Critère | NORMAL | ALERTE | URGENCE |
|---|---|---|---|
| **IAS vs seuils** | `valeur < MIN_Saison` | `MIN_Saison ≤ valeur < MAX_Saison` | `valeur ≥ MAX_Saison` |
| **Z-score** | `z < 1.5` | `1.5 ≤ z < 3.0` | `z ≥ 3.0` |

Le statut final = **max(statut_ias, statut_zscore)** (le plus sévère l'emporte).

### 3.3 R0 simplifié (modèle SIR)

```
R0_estimé = 1 + (taux_croissance_hebdomadaire × durée_infectieuse / 7)
```

- `durée_infectieuse` = 5 jours (grippe), 3 jours (gastro-entérite)
- `taux_croissance` = moyenne des taux de croissance sur les 4 dernières semaines IAS
- En l'absence d'historique en base, R0 est `None` lors des premières exécutions

---

## 4. Chaînage du DAG (TaskGroups et BranchPythonOperator)

### 4.1 TaskGroups

Les tâches sont regroupées en TaskGroups pour une meilleure lisibilité dans l'UI :

- **collecte** : téléchargement des CSV IAS®
- **persistance_brute** : archivage + vérification dans le volume Docker
- **traitement** : calcul des indicateurs statistiques
- **persistance_operationnelle** : insertion dans PostgreSQL

### 4.2 BranchPythonOperator

Après l'insertion en base, `evaluer_situation_epidemique` lit les statuts et route vers :
- `declencher_alerte_ars` (si ≥ 1 syndrome en URGENCE)
- `envoyer_bulletin_surveillance` (si ≥ 1 syndrome en ALERTE)
- `confirmer_situation_normale` (sinon)

### 4.3 TriggerRule

Le rapport final utilise `TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS` : il s'exécute quelle que soit la branche empruntée, à condition qu'au moins une tâche en amont ait réussi et qu'aucune n'ait échoué.

---

## 5. Idempotence et rejouabilité

### 5.1 ON CONFLICT DO UPDATE

Toutes les insertions PostgreSQL utilisent `ON CONFLICT DO UPDATE` :

```sql
INSERT INTO donnees_hebdomadaires (semaine, syndrome, valeur_ias, ...)
VALUES (...)
ON CONFLICT (semaine, syndrome)
DO UPDATE SET valeur_ias = EXCLUDED.valeur_ias, updated_at = CURRENT_TIMESTAMP;
```

Relancer le pipeline sur la même semaine met à jour les données existantes au lieu de créer des doublons.

### 5.2 catchup=True

Le DAG est configuré avec `catchup=True` : il peut rejouer toutes les semaines depuis `start_date` (2025-01-06). Airflow crée un DAG Run par semaine manquante et les exécute séquentiellement (`max_active_runs=1`).

### 5.3 CREATE TABLE IF NOT EXISTS

Le fichier SQL utilise `IF NOT EXISTS` partout + `ON CONFLICT DO NOTHING` pour les données de référence → l'étape init peut être rejouée sans erreur.

---

## 6. Sécurité et RGPD

- **Aucun credential en dur** : les connexions passent par `postgres_conn_id="postgres_ars"` (stocké dans les Connections Airflow)
- **Réseau Docker interne** : les conteneurs communiquent sur un réseau isolé
- **Traçabilité** : colonnes `created_at` / `updated_at` avec trigger automatique
- **Logs** : les logs Airflow ne contiennent pas de données brutes de santé, uniquement des métriques agrégées
