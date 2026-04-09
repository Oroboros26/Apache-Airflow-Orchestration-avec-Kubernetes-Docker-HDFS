# TP Jour 1 - Pipeline Energie & Meteo (Airflow + Docker)

Version de travail en Markdown, extraite et restructuree depuis le PDF `TP_Jour1_Pipeline_Energie_Meteo_Docker.pdf`.

## 1) Contexte

Vous jouez le role de data engineer chez RTE.
Objectif metier: produire un rapport quotidien qui croise meteo et production d'energie renouvelable (solaire/eolien) par region, et detecte des anomalies.

Sources Open Data (sans cle API):

- Open-Meteo: https://api.open-meteo.com/v1/forecast
- eCO2mix (OpenDataSoft): https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-regional-cons-def/records

## 2) Prerequis

- Docker Desktop installe et demarre
- Docker Compose v2 (`docker compose`)
- Python 3.9+
- ~4 Go RAM pour Docker
- Port 8080 libre

## 3) Structure projet attendue

```text
airflow-energie/
  docker-compose.yaml
  .env
  dags/
    energie_meteo_dag.py
  logs/
  plugins/
```

Dans ce repo, la structure existe deja (`dags/`, `logs/`, `plugins/`, `docker-compose.yaml`).

## 4) Setup Docker Airflow

### 4.1 Fichier `.env`

Valeurs attendues:

```env
AIRFLOW_UID=50000
AIRFLOW_GID=0
```

### 4.2 Demarrage

```bash
# Initialisation Airflow (une seule fois)
docker compose up airflow-init

# Lancement des services
docker compose up -d

# Verification et logs
docker compose ps
docker compose logs -f airflow-scheduler
```

UI Airflow: http://localhost:8080
Identifiants: `admin / admin`

## 5) APIs a utiliser

### 5.1 Open-Meteo

Exemple:

```text
GET /v1/forecast
?latitude=48.8566
&longitude=2.3522
&daily=sunshine_duration,wind_speed_10m_max
&timezone=Europe/Paris
&forecast_days=1
```

Points importants:

- `sunshine_duration` est en secondes (convertir en heures via `/3600`)
- `wind_speed_10m_max` en km/h

### 5.2 eCO2mix

Exemple:

```text
GET /api/explore/v2.1/catalog/datasets/eco2mix-regional-cons-def/records
?limit=100
&timezone=Europe%2FParis
```

Champs utiles:

- `libelle_region`
- `solaire` (MW)
- `eolien` (MW)
- `date`, `heure`

## 6) Architecture DAG attendue

DAG: `energie_meteo_dag` (schedule quotidien 06:00 Europe/Paris)

Ordonnancement:

```python
t1 >> [t2, t3] >> t4 >> t5
```

Taches:

1. `verifier_apis`
2. `collecter_meteo_regions`
3. `collecter_production_electrique`
4. `analyser_correlation`
5. `generer_rapport_energie`

## 7) Travail demande (checklist)

## Etape 1 - Demarrer et valider Airflow

- [ ] Lancer les services Docker
- [ ] Verifier etat `healthy`
- [ ] Ouvrir l'UI Airflow
- [ ] Verifier qu'il n'y a pas de DAGs d'exemple

## Etape 2 - Creer le DAG

- [ ] Creer `dags/energie_meteo_dag.py`
- [ ] Definir timezone Paris (`pendulum.timezone("Europe/Paris")`)
- [ ] Definir `REGIONS` (5 regions)
- [ ] Definir `default_args`
- [ ] Declarer les 5 `PythonOperator`
- [ ] Definir les dependances `t1 >> [t2, t3] >> t4 >> t5`

## Etape 3 - `verifier_apis()`

- [ ] Tester Open-Meteo + eCO2mix
- [ ] `timeout=10`
- [ ] Verifier code HTTP = 200
- [ ] Logger les succes
- [ ] Lever une exception explicite si echec

## Etape 4 - `collecter_meteo_regions()`

Pour chaque region:

- [ ] Appeler Open-Meteo (`timeout=15`)
- [ ] `raise_for_status()`
- [ ] Lire JSON
- [ ] `sunshine_duration` -> heures
- [ ] Recuperer `wind_speed_10m_max`
- [ ] Retourner structure:

```python
{
  "Region": {
    "ensoleillement_h": float,
    "vent_kmh": float
  }
}
```

## Etape 5 - `collecter_production_electrique()`

- [ ] Appeler eCO2mix (`limit=100`)
- [ ] Filtrer sur les 5 regions
- [ ] Gerer les `null` (`or 0.0`)
- [ ] Agreger par region
- [ ] Calculer moyenne journaliere solaire/eolien
- [ ] Retourner structure:

```python
{
  "Region": {
    "solaire_mw": float,
    "eolien_mw": float
  }
}
```

## Etape 6 - `analyser_correlation()`

Recuperer les XCom de `collecter_meteo_regions` et `collecter_production_electrique`.

Regles metier:

1. Alerte solaire si `ensoleillement_h > 6` ET `solaire_mw <= 1000`
2. Alerte eolien si `vent_kmh > 30` ET `eolien_mw <= 2000`
3. Bonus: anomalie donnees si `solaire_mw > 0` ET `ensoleillement_h == 0`

Sortie attendue par region:

```python
{
  "alertes": ["..."],
  "ensoleillement_h": float,
  "vent_kmh": float,
  "solaire_mw": float,
  "eolien_mw": float,
  "statut": "ALERTE" | "OK"
}
```

## Etape 7 - `generer_rapport_energie()`

- [ ] Lire XCom de `analyser_correlation`
- [ ] Afficher tableau recap dans les logs
- [ ] Generer un JSON:

```text
/tmp/rapport_energie_<YYYY-MM-DD>.json
```

- [ ] Retourner le chemin du fichier (XCom)

Commande de verification:

```bash
docker compose exec airflow-scheduler cat /tmp/rapport_energie_$(date +%Y-%m-%d).json
```

## 8) Questions de reflexion (REPONSES.md)

1. `LocalExecutor` vs `CeleryExecutor` vs `KubernetesExecutor`
2. Volumes Docker (`bind mount` vs volume nomme) et impact production
3. `catchup=False`, idempotence, impact si `catchup=True`
4. Importance de `timezone=Europe/Paris` (dont changement heure ete/hiver)

## 9) Livrables

- `docker-compose.yaml`
- `.env`
- `dags/energie_meteo_dag.py` complet
- `REPONSES.md` avec reponses + captures

Captures minimales:

1. DAG `energie_meteo_dag` en succes dans l'UI
2. Vue Graph avec les 5 taches
3. Logs de `generer_rapport_energie` (tableau)
4. XCom de `analyser_correlation`
5. Contenu du JSON genere

## 10) Exercices supplementaires (optionnel)

1. SLA + callback d'alerte (`sla_miss_callback`)
2. Dynamic Task Mapping avec `.expand()`
3. Debug d'un DAG casse (`dag_broken.py`) et correction de 5 erreurs

---

## Plan d'action immediat (pour nous)

1. Finaliser/valider le fichier `dags/energie_meteo_dag.py`
2. Lancer le DAG manuellement et verifier chaque XCom
3. Verifier le JSON final dans le conteneur
4. Rediger `REPONSES.md`
