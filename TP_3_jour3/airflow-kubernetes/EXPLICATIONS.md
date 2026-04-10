# 📘 TP Jour 3 — Explication Complète et Réponses

> **Formation** : Data Engineering — Apache Airflow  
> **École** : IPSSI Montpellier — M2  
> **Thème** : Déploiement et orchestration Airflow sur Kubernetes  
> **Branche Git** : `mohamed_dev_airflow`

---

## 📌 Table des matières

1. [Ce que nous avons fait](#1--ce-que-nous-avons-fait)
2. [Architecture mise en place](#2--architecture-mise-en-place)
3. [Kubernetes — Concepts clés](#3--kubernetes--concepts-clés)
4. [Les DAGs — Explication détaillée](#4--les-dags--explication-détaillée)
5. [Les exercices réalisés](#5--les-exercices-réalisés)
6. [Questions de réflexion (Q1–Q4)](#6--questions-de-réflexion-q1q4)
7. [Commandes Kubernetes essentielles](#7--commandes-kubernetes-essentielles)
8. [Troubleshooting courant](#8--troubleshooting-courant)

---

## 1 — Ce que nous avons fait

### Résumé global

Nous avons déployé **Apache Airflow sur un cluster Kubernetes** local (Kind) et créé des pipelines de données qui exploitent les capacités d'orchestration de conteneurs de Kubernetes.

Le projet comprend **deux méthodes de déploiement** :
- **Docker Compose** (développement local — LocalExecutor)
- **Kubernetes** via Kind (KubernetesExecutor — un pod par tâche)

### Étapes réalisées

1. **Cluster Kubernetes** : Création d'un cluster Kind avec 3 nœuds (1 control-plane + 2 workers)
2. **Manifestes YAML** : Écriture de 8 manifestes K8s pour déployer Airflow (namespace, RBAC, ConfigMap, PVC, PostgreSQL, scheduler, webserver)
3. **Helm Values** : Fichier `values.yaml` pour déploiement alternatif via Helm Chart officiel
4. **6 DAGs** : 3 DAGs principaux + 3 exercices pratiques
5. **Scripts d'automatisation** : Setup, déploiement, vérification, nettoyage
6. **Documentation complète** : REPONSES.md, EXPLICATIONS.md, PLAN.md, README.md

### Fichiers créés

```
airflow-kubernetes/
├── docker-compose.yaml           # Env dev : Airflow LocalExecutor
├── Dockerfile                    # Image custom (kubectl + K8s providers)
├── requirements.txt              # Dépendances Python
├── .env                          # AIRFLOW_UID
├── REPONSES.md                   # 4 questions de réflexion
├── EXPLICATIONS.md               # Ce fichier
├── dags/
│   ├── hello_kubernetes_dag.py   # DAG 1 : vérification env
│   ├── k8s_pod_operator_dag.py   # DAG 2 : KubernetesPodOperator
│   ├── k8s_etl_pipeline_dag.py   # DAG 3 : pipeline ETL complet
│   ├── dag_executor_comparison.py # Exercice 1 : executors
│   ├── dag_resource_management.py # Exercice 2 : ressources
│   └── dag_helm_monitoring.py    # Exercice 3 : Helm + monitoring
├── kubernetes/
│   ├── kind-config.yaml          # Cluster Kind (3 nœuds)
│   ├── namespace.yaml            # Namespace airflow
│   ├── postgres.yaml             # PostgreSQL complet
│   ├── airflow-configmap.yaml    # Config KubernetesExecutor
│   ├── airflow-pvc.yaml          # PVC DAGs + Logs
│   ├── airflow-rbac.yaml         # RBAC complet
│   ├── airflow-webserver.yaml    # Webserver + NodePort
│   ├── airflow-scheduler.yaml    # Scheduler + init-db
│   └── values-airflow-helm.yaml  # Values Helm
├── scripts/
│   ├── setup_kind.sh             # Créer cluster
│   ├── deploy_airflow.sh         # Déployer Airflow
│   ├── verify.sh                 # Vérifier
│   └── cleanup.sh                # Nettoyer
├── logs/
└── plugins/
```

---

## 2 — Architecture mise en place

### Comparaison des 3 jours

```
Jour 1 (Docker)          Jour 2 (HDFS)           Jour 3 (Kubernetes)
═══════════════          ════════════════         ═══════════════════
┌──────────┐             ┌──────────┐            ┌──────────────────┐
│  Docker  │             │  Docker  │            │   Kubernetes     │
│ Compose  │             │ Compose  │            │   (Kind)         │
│          │             │          │            │                  │
│ Airflow  │             │ Airflow  │            │ ┌──────────────┐ │
│ Postgres │             │ Postgres │            │ │   Airflow    │ │
│          │             │ NameNode │            │ │  Scheduler   │ │
│ Local    │             │ DataNode │            │ │  Webserver   │ │
│ Executor │             │          │            │ │  Postgres    │ │
└──────────┘             │ Local    │            │ │              │ │
                         │ Executor │            │ │  K8s         │ │
                         └──────────┘            │ │  Executor    │ │
                                                 │ └──────────────┘ │
                                                 │  Worker pods     │
                                                 │  (éphémères)     │
                                                 └──────────────────┘
```

### Architecture détaillée — Airflow sur Kubernetes

```
                    ┌─────────────────────────────────────┐
                    │       Cluster Kubernetes (Kind)      │
                    │                                      │
  Utilisateur       │  ┌──────────────────────────────┐   │
  ────────────►     │  │     Namespace: airflow        │   │
  port 30080        │  │                               │   │
  (NodePort)        │  │  ConfigMap ─── airflow-config │   │
                    │  │                               │   │
                    │  │  ┌─────────┐  ┌───────────┐  │   │
                    │  │  │Webserver│  │ Scheduler │  │   │
                    │  │  │ :8080   │  │           │  │   │
                    │  │  └────┬────┘  └─────┬─────┘  │   │
                    │  │       │              │        │   │
                    │  │       │    ┌─────────┘        │   │
                    │  │       │    │ KubernetesExecutor│   │
                    │  │       ▼    ▼                  │   │
                    │  │  ┌──────────┐                 │   │
                    │  │  │PostgreSQL│  Worker Pods:   │   │
                    │  │  │  :5432   │  ┌────┐ ┌────┐ │   │
                    │  │  │ (PVC 1Gi)│  │Pod1│ │Pod2│ │   │
                    │  │  └──────────┘  └────┘ └────┘ │   │
                    │  │                 (éphémères)   │   │
                    │  │  PVC:                         │   │
                    │  │   dags-pvc (1Gi)              │   │
                    │  │   logs-pvc (2Gi)              │   │
                    │  │                               │   │
                    │  │  RBAC:                        │   │
                    │  │   ServiceAccount: airflow     │   │
                    │  │   Role + RoleBinding          │   │
                    │  └──────────────────────────────┘   │
                    │                                      │
                    │  Node: control-plane                 │
                    │  Node: worker-1                      │
                    │  Node: worker-2                      │
                    └─────────────────────────────────────┘
```

### Flux d'exécution avec KubernetesExecutor

```
1. L'utilisateur déclenche un DAG via l'UI (port 30080)
2. Le Webserver enregistre le DAG run dans PostgreSQL
3. Le Scheduler détecte les tâches à exécuter
4. Pour chaque tâche, le Scheduler :
   a. Crée un pod Kubernetes via l'API K8s
   b. Le pod exécute la tâche Airflow
   c. Le pod écrit les résultats/logs
   d. Le pod se termine et est supprimé
5. Le Scheduler met à jour le statut dans PostgreSQL
6. Le Webserver affiche le résultat à l'utilisateur
```

---

## 3 — Kubernetes — Concepts clés

### Primitives utilisées dans ce TP

| Ressource | Rôle | Fichier |
|---|---|---|
| **Namespace** | Isolation logique des ressources | `namespace.yaml` |
| **Deployment** | Gère le cycle de vie des pods (réplicas, rolling update) | `airflow-*.yaml`, `postgres.yaml` |
| **Service** | Expose les pods sur le réseau (ClusterIP, NodePort) | `airflow-webserver.yaml`, `postgres.yaml` |
| **ConfigMap** | Configuration externalisée (variables d'env) | `airflow-configmap.yaml` |
| **PersistentVolumeClaim** | Stockage persistant pour les données | `airflow-pvc.yaml`, `postgres.yaml` |
| **ServiceAccount** | Identité du pod pour l'API K8s | `airflow-rbac.yaml` |
| **Role + RoleBinding** | Permissions RBAC (qui peut faire quoi) | `airflow-rbac.yaml` |

### Pourquoi RBAC est nécessaire

Le **KubernetesExecutor** a besoin de créer/supprimer des pods dynamiquement. Sans RBAC, le scheduler n'a pas la permission de manipuler les pods worker :

```yaml
# Ce que le scheduler doit pouvoir faire :
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["create", "get", "list", "watch", "delete", "patch"]
  - apiGroups: [""]
    resources: ["pods/log"]
    verbs: ["get", "list"]
```

Sans ces permissions → erreur `403 Forbidden` quand le scheduler essaie de créer un worker pod.

### Kind — Kubernetes in Docker

**Kind** (Kubernetes IN Docker) crée des clusters K8s locaux en utilisant des conteneurs Docker comme nœuds :

```
Docker Engine
├── kind-control-plane (conteneur Docker = nœud K8s master)
├── kind-worker        (conteneur Docker = nœud K8s worker 1)
└── kind-worker2       (conteneur Docker = nœud K8s worker 2)
```

**Avantages** :
- Rapide (cluster en ~60s)
- Léger (pas de VM)
- Identique à un vrai cluster K8s
- Parfait pour CI/CD et développement

**Alternatives** : Minikube, k3s, MicroK8s

---

## 4 — Les DAGs — Explication détaillée

### DAG 1 : `hello_kubernetes`

**Objectif** : Vérifier que l'environnement Airflow est correctement configuré pour K8s.

| Tâche | Ce qu'elle fait |
|---|---|
| `afficher_info_environnement` | Affiche hostname, OS, Python, executor configuré |
| `verifier_modules_kubernetes` | Vérifie que `kubernetes` et `airflow.providers.cncf.kubernetes` sont installés |
| `verifier_connectivite_k8s` | Tente de se connecter au cluster K8s et liste les nœuds |
| `resume_verification` | Consolide les résultats via XCom |

**Pourquoi ce DAG ?** Avant de créer des KubernetesPodOperator, il faut valider que :
- Les modules Python sont installés
- Le kubeconfig est accessible
- Le cluster K8s répond

### DAG 2 : `k8s_pod_operator_demo`

**Objectif** : Démontrer 4 cas d'usage du KubernetesPodOperator.

**KubernetesPodOperator** = Opérateur Airflow qui crée un pod K8s pour exécuter une tâche. Chaque tâche tourne dans un conteneur isolé avec sa propre image et ses propres ressources.

```python
KubernetesPodOperator(
    task_id='ma_tache',
    namespace='airflow',           # Namespace K8s
    image='python:3.11-slim',      # Image Docker du pod
    cmds=['python', '-c'],         # Commande à exécuter
    arguments=['print("hello")'],  # Arguments
    get_logs=True,                 # Récupérer les logs du pod
    is_delete_operator_pod=True,   # Supprimer le pod après exécution
    in_cluster=False,              # Utiliser kubeconfig (pas in-cluster)
    config_file='/opt/airflow/.kube/config',  # Chemin kubeconfig
    container_resources=...,       # CPU/mémoire requests+limits
    env_vars={...},                # Variables d'environnement
    volumes=[...],                 # Volumes montés
)
```

**4 exemples progressifs :**
1. **Hello Pod** : Pod minimal — exécuter du Python dans un pod K8s
2. **Pod + Env vars** : Injecter des variables d'environnement
3. **Pod + Resources** : Définir des limites CPU/mémoire
4. **Pod + Volume** : Monter un volume emptyDir pour écrire des données

### DAG 3 : `k8s_etl_pipeline`

**Objectif** : Pipeline ETL complet avec branchement (même logique que Jour 2, mais orienté K8s).

```
extract (200 transactions CSV)
    ↓
transform (agrégation par catégorie)
    ↓
load (chargement simulé)
    ↓
decider_qualite (BranchPythonOperator)
    ├── notifier_succes (si qualité OK)
    └── notifier_alerte (si qualité insuffisante)
           ↓
    rapport_final (trigger_rule: none_failed_min_one_success)
```

**Points clés** :
- **XCom** : Les chemins de fichiers sont passés entre tâches via XCom (return value)
- **BranchPythonOperator** : Décision basée sur le nombre de transactions et le CA
- **trigger_rule** : `none_failed_min_one_success` pour que `rapport_final` s'exécute quelle que soit la branche prise

---

## 5 — Les exercices réalisés

### Exercice 1 : Comparaison des Executors (`dag_executor_comparison.py`)

Compare les 3 executors majeurs d'Airflow en simulant leur comportement et en affichant un tableau comparatif.

**Ce qu'on apprend** :
- LocalExecutor = processus sur la même machine (dev)
- CeleryExecutor = workers distribués via broker (production classique)
- KubernetesExecutor = un pod par tâche (cloud/microservices)
- CeleryKubernetesExecutor = hybride (Airflow 2.7+)

### Exercice 2 : Gestion des Ressources (`dag_resource_management.py`)

Montre 3 profils de charge et les ressources K8s recommandées :
- **Léger** (50m CPU, 64Mi) — healthcheck, notification
- **Moyen** (250m CPU, 256Mi) — ETL, transformation
- **Lourd** (1000m CPU, 1Gi) — ML, agrégation massive

**Ce qu'on apprend** :
- Différence requests vs limits
- Unités K8s (m = millicores, Mi = mébibytes)
- OOMKilled, throttling, Pending
- Bonnes pratiques (ratio 1:2, LimitRange, ResourceQuota)

### Exercice 3 : Helm & Monitoring (`dag_helm_monitoring.py`)

Explique le déploiement Helm et les 3 niveaux de monitoring :
1. **Niveau Airflow** : métriques internes, API /health
2. **Niveau Kubernetes** : kubectl top, events, describe
3. **Niveau Application** : Prometheus, Grafana, Loki

---

## 6 — Questions de réflexion (Q1–Q4)

*(Réponses complètes dans `REPONSES.md`)*

| Question | Résumé de la réponse |
|---|---|
| Q1 — Celery vs K8s Executor | 3 différences : modèle d'exécution, isolation, scaling |
| Q2 — Requests vs Limits | Garanti vs maximum, OOMKilled, profils recommandés |
| Q3 — Helm vs YAML | YAML = éducatif/transparent, Helm = production/maintenable |
| Q4 — Scaling 10x | K8sExecutor natif, HA scheduler, cluster autoscaler, PgBouncer |

---

## 7 — Commandes Kubernetes essentielles

### Gestion du cluster

```bash
# Créer le cluster Kind
kind create cluster --config kubernetes/kind-config.yaml --name airflow-cluster

# Supprimer le cluster Kind
kind delete cluster --name airflow-cluster

# Vérifier le cluster
kubectl cluster-info
kubectl get nodes
```

### Déploiement

```bash
# Appliquer les manifestes
kubectl apply -f kubernetes/namespace.yaml
kubectl apply -f kubernetes/airflow-rbac.yaml
kubectl apply -f kubernetes/airflow-configmap.yaml
kubectl apply -f kubernetes/airflow-pvc.yaml
kubectl apply -f kubernetes/postgres.yaml
kubectl apply -f kubernetes/airflow-scheduler.yaml
kubectl apply -f kubernetes/airflow-webserver.yaml

# Déployer avec Helm
helm install airflow apache-airflow/airflow \
  -f kubernetes/values-airflow-helm.yaml \
  --namespace airflow
```

### Monitoring

```bash
# Voir les pods
kubectl get pods -n airflow -o wide

# Logs d'un pod
kubectl logs <pod-name> -n airflow

# Suivre les logs en continu
kubectl logs -f <pod-name> -n airflow

# Détails d'un pod (événements, état, conditions)
kubectl describe pod <pod-name> -n airflow

# Consommation CPU/mémoire
kubectl top pods -n airflow
kubectl top nodes

# Événements récents
kubectl get events -n airflow --sort-by=.lastTimestamp

# Shell dans un pod
kubectl exec -it <pod-name> -n airflow -- bash

# Port-forward (alternative au NodePort)
kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow
```

### Debugging

```bash
# Pod qui ne démarre pas
kubectl describe pod <pod-name> -n airflow
# → Chercher "Events" en bas : ImagePullBackOff, Pending, etc.

# Pod OOMKilled
kubectl get pod <pod-name> -n airflow -o jsonpath='{.status.containerStatuses[0].lastState}'

# Voir tous les pods (y compris terminés)
kubectl get pods -n airflow --show-all

# Supprimer un pod manuellement
kubectl delete pod <pod-name> -n airflow

# Redémarrer un deployment
kubectl rollout restart deployment airflow-scheduler -n airflow
```

---

## 8 — Troubleshooting courant

### Pod en état Pending

**Cause** : Pas assez de ressources sur le cluster pour satisfaire les requests.

```bash
kubectl describe pod <pod> -n airflow
# Events: 0/3 nodes are available: insufficient cpu/memory

# Solution : réduire les requests ou ajouter des nœuds
```

### Pod en état CrashLoopBackOff

**Cause** : Le conteneur crash au démarrage et K8s essaie de le redémarrer en boucle.

```bash
kubectl logs <pod> -n airflow --previous  # Logs du crash précédent
kubectl describe pod <pod> -n airflow     # Voir le code de sortie

# Causes courantes :
# - Mauvaise commande/arguments
# - DB non disponible (PostgreSQL pas encore prêt)
# - Permissions insuffisantes (RBAC)
```

### Pod en état ImagePullBackOff

**Cause** : Kubernetes ne peut pas télécharger l'image Docker.

```bash
# Vérifier l'image
kubectl describe pod <pod> -n airflow
# Events: Failed to pull image "xxx": rpc error...

# Solutions :
# - Vérifier le nom/tag de l'image
# - Charger l'image dans Kind : kind load docker-image <image> --name airflow-cluster
# - Vérifier les imagePullSecrets si registry privé
```

### Airflow UI non accessible

```bash
# Vérifier le service NodePort
kubectl get svc airflow-webserver -n airflow
# → NodePort doit être 30080

# Vérifier que le pod webserver tourne
kubectl get pods -n airflow -l component=webserver

# Alternative : port-forward
kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow
```

### DAGs non visibles dans l'UI

```bash
# Vérifier que les DAGs sont dans le PVC
kubectl exec -it <scheduler-pod> -n airflow -- ls /opt/airflow/dags/

# Copier les DAGs manuellement
kubectl cp dags/ airflow/<scheduler-pod>:/opt/airflow/dags/

# Vérifier les erreurs de parsing
kubectl logs <scheduler-pod> -n airflow | grep "ERROR"
```

---

## 9 — Questions checkpoint Jour 3

### C1 — Qu'est-ce que le KubernetesExecutor ?

Le KubernetesExecutor est un executor Airflow qui crée un **pod Kubernetes éphémère** pour chaque tâche. Le pod est créé au moment de l'exécution, exécute la tâche, puis est supprimé. Cela offre une isolation complète entre les tâches et un scaling automatique.

### C2 — Différence entre KubernetesExecutor et KubernetesPodOperator ?

- **KubernetesExecutor** : C'est l'**executor** — il détermine **comment** les tâches sont exécutées. Chaque PythonOperator, BashOperator, etc. crée automatiquement un pod.
- **KubernetesPodOperator** : C'est un **opérateur** — il crée un pod avec une **image Docker personnalisée** et des **ressources spécifiques**. Il peut être utilisé avec n'importe quel executor.

En résumé : KubernetesExecutor = mode global (tout en pods), KubernetesPodOperator = opérateur spécifique (un pod sur mesure).

### C3 — Pourquoi utiliser Helm pour déployer Airflow ?

1. **Standardisation** : Le chart officiel est maintenu par la communauté Apache Airflow
2. **Configuration** : Un seul `values.yaml` pour tout personnaliser
3. **Versionning** : Rollback facile avec `helm rollback`
4. **Upgrade** : Mise à jour d'Airflow avec `helm upgrade`
5. **Best practices** : Le chart inclut les bonnes pratiques (PgBouncer, RBAC, probes, etc.)

### C4 — Qu'est-ce qu'un PVC et pourquoi en a-t-on besoin ?

Un **PersistentVolumeClaim** (PVC) est une demande de stockage persistant dans Kubernetes. Les pods sont éphémères — quand ils sont supprimés, leurs données disparaissent. Un PVC permet de conserver les données (DAGs, logs, base de données) indépendamment du cycle de vie des pods.

### C5 — Pourquoi le RBAC est-il nécessaire ?

Le **Role-Based Access Control** (RBAC) définit les permissions de chaque ServiceAccount dans le cluster. Le scheduler Airflow doit pouvoir créer et supprimer des pods worker. Sans les bonnes permissions RBAC, le scheduler recevrait une erreur `403 Forbidden` à chaque tentative de création de pod.

---

> **Ce document couvre l'ensemble du TP Jour 3 et peut servir de référence pour les examens/certifications Kubernetes + Airflow.**
