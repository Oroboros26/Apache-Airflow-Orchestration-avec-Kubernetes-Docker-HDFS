# TP Jour 3 — Airflow sur Kubernetes : Orchestration de Pipelines de Données

> **Formation** : M2 Data Engineering — IPSSI Montpellier  
> **Thème** : Déploiement et orchestration Apache Airflow sur Kubernetes  
> **Branche** : `mohamed_dev_airflow`

---

## Objectif

Déployer Apache Airflow sur un cluster Kubernetes local (Kind) et créer des pipelines de données qui exploitent les capacités de Kubernetes :

1. **Déployer** un cluster Kubernetes local avec Kind (3 nœuds)
2. **Installer** Airflow sur K8s (manifestes YAML et/ou Helm)
3. **Créer** des DAGs utilisant le KubernetesPodOperator
4. **Configurer** le KubernetesExecutor (un pod par tâche)
5. **Monitorer** le cluster et les pipelines
6. **Gérer** les ressources CPU/mémoire par tâche

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   Cluster Kubernetes (Kind)                   │
│                                                               │
│  ┌──────────────────────────────────────────────────────┐    │
│  │              Namespace : airflow                      │    │
│  │                                                       │    │
│  │  ┌─────────────┐  ┌──────────────┐  ┌────────────┐  │    │
│  │  │  PostgreSQL  │  │   Airflow    │  │  Airflow   │  │    │
│  │  │   (DB)       │  │  Webserver   │  │ Scheduler  │  │    │
│  │  │  ClusterIP   │  │  NodePort    │  │            │  │    │
│  │  │  :5432       │  │  :30080      │  │            │  │    │
│  │  └─────────────┘  └──────────────┘  └─────┬──────┘  │    │
│  │                                            │          │    │
│  │                                    KubernetesExecutor │    │
│  │                                            │          │    │
│  │  ┌────────┐ ┌────────┐ ┌────────┐         │          │    │
│  │  │ Worker │ │ Worker │ │ Worker │ ←───────┘          │    │
│  │  │ Pod 1  │ │ Pod 2  │ │ Pod N  │  (éphémères)      │    │
│  │  └────────┘ └────────┘ └────────┘                    │    │
│  └──────────────────────────────────────────────────────┘    │
│                                                               │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐             │
│  │ Node       │  │ Node       │  │ Node       │             │
│  │ Control    │  │ Worker 1   │  │ Worker 2   │             │
│  │ Plane      │  │            │  │            │             │
│  └────────────┘  └────────────┘  └────────────┘             │
└─────────────────────────────────────────────────────────────┘
```

**KubernetesExecutor** : Le scheduler crée un pod K8s éphémère pour chaque tâche Airflow. Le pod exécute la tâche puis est automatiquement supprimé. Cela permet une isolation totale et un scaling automatique.

---

## Structure du projet

```
TP_3_jour3/
├── README.md                              ← ce fichier
├── PLAN.md                                ← plan de travail détaillé
├── apache_airflow_k8s_docker_hdfs_Jour3.pdf ← slides du cours
└── airflow-kubernetes/                    ← TP principal
    ├── docker-compose.yaml                ← env dev (LocalExecutor)
    ├── Dockerfile                         ← image Airflow + kubectl + K8s providers
    ├── requirements.txt                   ← dépendances Python
    ├── .env                               ← UID Airflow
    ├── REPONSES.md                        ← réponses aux questions de réflexion
    ├── EXPLICATIONS.md                    ← explications complètes + Q&A
    ├── dags/
    │   ├── hello_kubernetes_dag.py        ← DAG 1 : vérification environnement
    │   ├── k8s_pod_operator_dag.py        ← DAG 2 : KubernetesPodOperator
    │   ├── k8s_etl_pipeline_dag.py        ← DAG 3 : pipeline ETL complet
    │   ├── dag_executor_comparison.py     ← exercice 1 : comparaison executors
    │   ├── dag_resource_management.py     ← exercice 2 : gestion ressources
    │   └── dag_helm_monitoring.py         ← exercice 3 : Helm + monitoring
    ├── kubernetes/
    │   ├── kind-config.yaml               ← config cluster Kind (3 nœuds)
    │   ├── namespace.yaml                 ← namespace airflow
    │   ├── postgres.yaml                  ← PostgreSQL (Deployment + PVC + Service)
    │   ├── airflow-configmap.yaml         ← config KubernetesExecutor
    │   ├── airflow-pvc.yaml               ← PVC DAGs + Logs
    │   ├── airflow-rbac.yaml              ← ServiceAccount + Role + RoleBinding
    │   ├── airflow-webserver.yaml         ← Webserver (Deployment + NodePort)
    │   ├── airflow-scheduler.yaml         ← Scheduler (Deployment)
    │   └── values-airflow-helm.yaml       ← values Helm (méthode alternative)
    ├── scripts/
    │   ├── setup_kind.sh                  ← créer le cluster Kind
    │   ├── deploy_airflow.sh              ← déployer Airflow (YAML ou Helm)
    │   ├── verify.sh                      ← vérifier le déploiement
    │   └── cleanup.sh                     ← nettoyage complet
    ├── logs/                              ← logs Airflow
    └── plugins/                           ← plugins Airflow
```

---

## DAGs

### DAG 1 — `hello_kubernetes` (Intro)

Vérification de l'environnement et de la connectivité Kubernetes :

```
afficher_info ──┐
                ├── verifier_connectivite_k8s → resume_verification
verifier_modules┘
```

### DAG 2 — `k8s_pod_operator_demo` (KubernetesPodOperator)

4 exemples d'utilisation du KubernetesPodOperator :

```
verifier_prerequis → hello_pod → pod_avec_env → pod_avec_ressources → pod_avec_volume
```

| Tâche | Description |
|---|---|
| `hello_pod` | Pod simple — Hello World |
| `pod_avec_variables_env` | Pod avec variables d'environnement |
| `pod_avec_ressources` | Pod avec limits CPU/mémoire |
| `pod_avec_volume` | Pod avec volume emptyDir |

### DAG 3 — `k8s_etl_pipeline` (Pipeline ETL)

Pipeline ETL complet avec branchement :

```
extract → transform → load → decider_qualite ──┬── notifier_succes ──┐
                                                └── notifier_alerte ──┤
                                                                       └── rapport_final
```

### DAG 4 — `comparaison_executors` (Exercice 1)

Compare Local, Celery et Kubernetes executors :

```
simuler_local ──────┐
simuler_celery ─────┼── comparer_executors
simuler_kubernetes ─┘
```

### DAG 5 — `gestion_ressources_k8s` (Exercice 2)

Profils de ressources Kubernetes (léger, moyen, lourd) :

```
profil_leger ──┐
profil_moyen ──┼── resume_ressources
profil_lourd ──┘
```

### DAG 6 — `helm_monitoring_k8s` (Exercice 3)

Helm deployment et monitoring Kubernetes :

```
expliquer_helm ──────┐
expliquer_monitoring ┼── simuler_healthcheck → generer_rapport_monitoring
```

---

## Démarrage rapide

### Option A : Docker Compose (développement)

```bash
cd TP_3_jour3/airflow-kubernetes
docker compose build
docker compose up airflow-init
docker compose up -d
# → Airflow UI : http://localhost:8080 (admin/admin)
```

### Option B : Kubernetes (Kind)

```bash
cd TP_3_jour3/airflow-kubernetes
./scripts/setup_kind.sh        # Créer le cluster (3 nœuds)
./scripts/deploy_airflow.sh    # Déployer Airflow (YAML)
./scripts/verify.sh            # Vérifier le déploiement
# → Airflow UI : http://localhost:8081 (admin/admin)
```

### Option C : Helm (production-like)

```bash
cd TP_3_jour3/airflow-kubernetes
./scripts/setup_kind.sh             # Créer le cluster
./scripts/deploy_airflow.sh helm    # Déployer via Helm
```

### Nettoyage

```bash
./scripts/cleanup.sh
```

---

## Technologies utilisées

| Outil | Version | Rôle |
|---|---|---|
| Apache Airflow | 2.8.1 | Orchestration du pipeline |
| Kubernetes (Kind) | 1.29+ | Cluster local K8s |
| Docker | 24+ | Conteneurisation |
| Helm | 3.x | Gestionnaire de packages K8s |
| kubectl | 1.29+ | CLI Kubernetes |
| PostgreSQL | 13 | Base de données Airflow |
| Python | 3.11 | Scripts, DAGs, opérateurs |

---

## Progression de la formation

| Jour | Thème | Technologies |
|:---:|---|---|
| **1** | Introduction Airflow + Docker | Docker Compose, LocalExecutor |
| **2** | Pipelines ETL + HDFS | HDFS, WebHDFS, BranchPythonOperator |
| **3** | **Airflow sur Kubernetes** | **Kind, K8sExecutor, KubernetesPodOperator, Helm** |
