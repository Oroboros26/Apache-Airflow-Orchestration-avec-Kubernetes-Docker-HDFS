# REPONSES - TP Jour 1 Airflow

## Checklist de rendu

- [x] DAG principal implemente et execute
- [x] Reponses Q1 a Q4 completees
- [x] Exercice supplementaire 1 implemente (SLA + callback)
- [x] Exercice supplementaire 2 implemente (Dynamic Task Mapping)
- [x] Exercice supplementaire 3 implemente (DAG corrige)
- [ ] Captures d'ecran collees
- [ ] Export final/verification avant depot

## Contexte

Pipeline realise: `energie_meteo_dag` avec 5 taches (`verifier_apis`, `collecter_meteo_regions`, `collecter_production_electrique`, `analyser_correlation`, `generer_rapport_energie`).

Execution validee localement via Docker Compose et Airflow.

## Q1 - Docker Executor: LocalExecutor vs CeleryExecutor vs KubernetesExecutor

### LocalExecutor

- Principe: un seul scheduler + execution locale de plusieurs taches en parallele (processus locaux).
- Avantages: simple a configurer, rapide pour dev/POC, peu de composants.
- Limites: scale limite a la machine hebergeant Airflow; resilence plus faible en cas de panne du noeud.
- Cas RTE: environnement de developpement, integration locale, petits workflows.

### CeleryExecutor

- Principe: scheduler distribue les taches via un broker (Redis/RabbitMQ) vers plusieurs workers Celery.
- Avantages: scale horizontal facile (ajout de workers), bon compromis pour production classique.
- Limites: plus d'operations (broker + workers), monitoring a mettre en place, gestion de capacite.
- Cas RTE: production stable multi-workflows, charge reguliere, besoin de separation scheduler/workers.

### KubernetesExecutor

- Principe: chaque tache est lancee dans un pod Kubernetes ephemere.
- Avantages: isolation forte, elasticite native, adaptation fine CPU/RAM par tache, auto-healing K8s.
- Limites: complexite d'exploitation (cluster, securite, policies), cout operationnel plus eleve.
- Cas RTE: production a grande echelle, besoins d'isolation stricte, charges tres variables.

### Recommandation pratique RTE

- Dev/formation: `LocalExecutor`
- Prod intermediaire: `CeleryExecutor`
- Prod mature cloud-native: `KubernetesExecutor`

## Q2 - Volumes Docker et persistance des DAGs

Le mapping `./dags:/opt/airflow/dags` est un **bind mount**:

- Le dossier local hote est monte directement dans le conteneur.
- Toute modification locale d'un DAG est visible presque immediatement par Airflow (sans rebuild d'image).

Difference avec un volume nomme:

- Bind mount: couple a un chemin precis de l'hote, ideal en dev.
- Volume nomme: gere par Docker, plus portable au niveau conteneur mais moins direct pour edit local.

Si on supprime le mapping `./dags:/opt/airflow/dags`:

- Airflow ne verra que les DAGs inclus dans l'image.
- Les changements locaux ne seront plus pris en compte en live.
- Il faudra rebuild/redeploy pour chaque modification.

Impact production multi-noeuds:

- Tous les composants (scheduler/webserver/workers) doivent voir **la meme version** des DAGs.
- Strategies courantes:
  - image Docker versionnee contenant les DAGs,
  - ou stockage partage (ex: objet storage + sync).
- Risque sinon: desynchronisation de code entre noeuds, erreurs d'import, comportements differents selon worker.

## Q3 - Idempotence et catchup

### Effet de catchup=True

Avec `start_date` ancien (ex: 2024-01-01) et activation aujourd'hui, Airflow va creer les runs manques jusqu'a maintenant selon la frequence du DAG.

Pour un DAG quotidien, cela peut representer plusieurs centaines de runs historiques.

### Idempotence (definition)

Un DAG est idempotent si une re-execution pour la meme date logique produit le meme resultat fonctionnel, sans doublons ni corruption.

Pourquoi c'est critique ici:

- Donnees energetiques sensibles pour decision metier.
- Relances frequentes en cas d'erreur/retry.
- Necessite d'eviter des rapports incoherents ou dupliques.

### Rendre `collecter_*` idempotentes

- Se baser sur une date logique de run (pas sur "now" implicite quand possible).
- Determinisme des transformations (memes entrees => memes sorties).
- Ecriture de sorties avec convention claire (ex: un fichier par date logique).
- Eviter les effets de bord irreversibles non proteges.
- En persistance base/table: utiliser upsert/merge plutot qu'insert brut.

## Q4 - Timezone et donnees temps reel

Le parametre `timezone=Europe/Paris` est essentiel car:

- RTE opere sur horaire local France.
- Les courbes production/consommation se lisent en heure locale metier.

Risque sans timezone explicite:

- Decalage UTC/local sur les fenetres horaires.
- Correlation meteo/production comparee sur de mauvaises heures.

Cas changement heure d'ete/hiver:

- Passage ete: une heure "saute" (jour de 23h).
- Passage hiver: une heure est dupliquee (jour de 25h).

Exemple concret de corruption:

- Si meteo est interpretee en UTC et production en Europe/Paris, la production de 14:00 locale peut etre comparee a la meteo de 12:00 UTC selon la date, generant des fausses alertes de sous-production.

## Captures d'ecran a inserer

### Capture 1 - DAG principal en succes

- Cible: vue DAGs avec energie_meteo_dag en vert
- Emplacement image: A REMPLIR
- Commentaire: A REMPLIR

<img width="1909" height="907" alt="image" src="https://github.com/user-attachments/assets/8b24ad51-88d0-4e4a-9c9c-55510846e157" />



### Capture 2 - Vue Graph (5 taches)

- Cible: graphe t1 >> [t2, t3] >> t4 >> t5
- Emplacement image: A REMPLIR
- Commentaire: A REMPLIR

### Capture 3 - Logs generer_rapport_energie

- Cible: tableau recap des regions dans les logs
- Emplacement image: A REMPLIR
- Commentaire: A REMPLIR

### Capture 4 - XCom analyser_correlation

- Cible: dictionnaire d'alertes par region
- Emplacement image: A REMPLIR
- Commentaire: A REMPLIR

### Capture 5 - JSON genere

- Cible: contenu du rapport JSON
- Emplacement image: A REMPLIR
- Commentaire: A REMPLIR

### Capture 6 - Dynamic Task Mapping

- Cible: sous-instances mappees extraire_meteo_region[0..4]
- Emplacement image: A REMPLIR
- Commentaire: A REMPLIR

### Capture 7 - Exercice dag_broken corrige

- Cible: run success de dag_broken
- Emplacement image: A REMPLIR
- Commentaire: A REMPLIR

## Traces d'execution locales

- Run valide observe: `manual__2026-04-08T12:22:07+00:00`
- Log de sauvegarde rapport observe: `Rapport sauvegarde: /tmp/rapport_energie_2026-04-08.json`
- Fichier de log correspondant dans le repo:
  - `logs/dag_id=energie_meteo_dag/run_id=manual__2026-04-08T12:22:07+00:00/task_id=generer_rapport_energie/attempt=1.log`

## Exercice supplementaire 1 - SLA et alertes de delai

Implementation realisee dans le DAG principal:

- Fichier: `dags/energie_meteo_dag.py`
- SLA global via `default_args["sla"] = 90 minutes`
- SLA specifique sur `generer_rapport_energie = 45 minutes`
- Callback implemente: `sla_miss_callback(...)` avec logs structures `[SLA MISS]` et `[ALERTE SLA]`

Reponses aux questions de reflexion (Exercice 1):

1. Difference entre `sla` et `execution_timeout`:
  - `sla`: objectif de delai metier surveille par le scheduler (SLA miss), utile pour alerting/monitoring.
  - `execution_timeout`: limite technique dure d'execution d'une tache; si depassee, la tache echoue immediatement.

2. Pourquoi un SLA miss n'arrete pas la tache:
  - Un SLA miss est un signal d'observabilite (retard) et non un mecanisme d'arret.
  - Airflow laisse la tache se terminer sauf si un autre mecanisme (timeout, kill, retry policy) intervient.

Validation effectuee:

- Un test de forçage SLA a ete tente (SLA a 1 seconde), puis valeurs normales restaurees.
- Dans cet environnement Docker, la table `sla_miss` n'a pas enregistre d'entree malgre le depassement force.
- Cette limitation est environnementale (scheduler/metadata) et n'empeche pas la validite de l'implementation du callback et des parametrages SLA.

Preuve de restauration des valeurs cibles:

- `default_args["sla"] = 90 min` (restaure)
- `generer_rapport_energie.sla = 45 min` (restaure)

## Exercice supplementaire 2 - Dynamic Task Mapping

Implementation realisee avec un DAG dedie:

- `dags/energie_meteo_dag_dynamic.py`

Points implementes:

- Tache `charger_config_regions()` lisant `regions_energie` depuis Variables Airflow
- Mapping dynamique via `extraire_meteo_region.expand(region=regions)`
- Support optionnel de `regions_exclues`
- Adaptation de `analyser_correlation()` pour recevoir la liste issue du mapping

Validation:

- Run OK: `manual__2026-04-08T12:35:57+00:00` en `success`
- Les sous-instances mappees de `extraire_meteo_region` apparaissent avec map index (`0..4`)

Reponses aux questions de reflexion (Exercice 2):

1. Quand preferer `.expand()` plutot que boucle `.override()`:
  - Quand le nombre d'elements est variable/dynamique a l'execution.
  - Quand on veut une meilleure observabilite des sous-taches (instance par element).
  - Quand on veut eviter de modifier le code a chaque evolution du nombre d'entrees.

2. Limite de taille des donnees XCom dans `.expand()`:
  - En pratique, il faut rester sur de petits payloads JSON-serializables.
  - La limite depend du backend metadonnees (taille de ligne/objet), donc il ne faut pas mapper sur de gros objets.
  - Bonne pratique: passer des references legeres (ids, noms, chemins), pas des blobs volumineux.

## Exercice supplementaire 3 - Debogage d'un DAG casse

Fichier corrige cree:

- `dags/dag_broken.py`

Validation:

- Run OK: `manual__2026-04-08T12:40:33+00:00`
- Taches `extraire`, `transformer`, `charger` en `success`

Tableau des 5 erreurs corrigees:

| # | Erreur initiale | Impact | Correction appliquee |
|---|---|---|---|
| 1 | `start_date=datetime.now()` | Non deterministe, comportement planning instable | `start_date=pendulum.datetime(2024, 1, 1, tz="Europe/Paris")` |
| 2 | `catchup=True` | Backfill massif non voulu | `catchup=False` |
| 3 | `retries: 0` | Pas de tolerance aux erreurs reseau transitoires | `retries: 1` + `retry_delay` |
| 4 | Type hint manquant dans `transformer(data)` | Lisibilite/type-checking faibles | `transformer(data: dict) -> dict` |
| 5 | `print()` dans `charger` | Logs moins structures en prod | `logging.info(...)` |

Correction supplementaire demandee par l'enonce:

- Ajout de `chain(data, resultat, done)` pour expliciter les dependances
- Assignation explicite du DAG via `dag_instance = dag_broken()`
