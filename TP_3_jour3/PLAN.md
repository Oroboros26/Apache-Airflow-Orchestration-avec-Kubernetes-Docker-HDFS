# 📋 Plan de Travail — TP Jour 3

> **Formation** : Data Engineering — Apache Airflow — IPSSI Montpellier  
> **Branche** : `mohamed_dev_airflow`  
> **Date** : 10 Avril 2026  
> **Durée estimée** : ~4-5h (TP principal 3h + exercices supplémentaires)

---

## 📚 Vue d'ensemble

Le Jour 3 porte sur le **déploiement et l'orchestration d'Airflow avec Kubernetes**. Après avoir utilisé Docker Compose (Jour 1) et HDFS (Jour 2), nous passons à la gestion de pipelines de données sur un cluster Kubernetes.

| Document | Contenu |
|---|---|
| `apache_airflow_k8s_docker_hdfs_Jour3.pdf` | **Cours/Slides** — Kubernetes, KubernetesExecutor, KubernetesPodOperator, Helm, monitoring |

---

## 🗂️ Structure cible du projet

```
TP_3_jour3/
├── PLAN.md                                    ← Ce fichier
├── README.md                                  ← Description du projet
├── apache_airflow_k8s_docker_hdfs_Jour3.pdf   ← Slides du cours
└── airflow-kubernetes/                        ← TP PRINCIPAL
    ├── docker-compose.yaml                    ← Env dev (LocalExecutor)
    ├── Dockerfile                             ← Image Airflow customisée
    ├── requirements.txt                       ← Dépendances Python
    ├── .env                                   ← Variables d'environnement
    ├── REPONSES.md                            ← Réponses aux questions
    ├── EXPLICATIONS.md                        ← Explications détaillées
    ├── dags/
    │   ├── hello_kubernetes_dag.py             ← DAG 1 : Intro + vérification env
    │   ├── k8s_pod_operator_dag.py            ← DAG 2 : KubernetesPodOperator
    │   ├── k8s_etl_pipeline_dag.py            ← DAG 3 : Pipeline ETL complet
    │   ├── dag_executor_comparison.py         ← Exercice 1 : Comparaison executors
    │   ├── dag_resource_management.py         ← Exercice 2 : Gestion ressources
    │   └── dag_helm_monitoring.py             ← Exercice 3 : Helm + monitoring
    ├── kubernetes/
    │   ├── kind-config.yaml                   ← Config cluster Kind
    │   ├── namespace.yaml                     ← Namespace airflow
    │   ├── postgres.yaml                      ← PostgreSQL (DB + PVC + Service)
    │   ├── airflow-configmap.yaml             ← Configuration Airflow
    │   ├── airflow-pvc.yaml                   ← PVC DAGs + Logs
    │   ├── airflow-rbac.yaml                  ← ServiceAccount + Role
    │   ├── airflow-webserver.yaml             ← Webserver (Deployment + Service)
    │   ├── airflow-scheduler.yaml             ← Scheduler (Deployment)
    │   └── values-airflow-helm.yaml           ← Values Helm (alternative)
    ├── scripts/
    │   ├── setup_kind.sh                      ← Création cluster Kind
    │   ├── deploy_airflow.sh                  ← Déploiement Airflow (YAML ou Helm)
    │   ├── verify.sh                          ← Vérification du déploiement
    │   └── cleanup.sh                         ← Nettoyage complet
    ├── logs/
    └── plugins/
```

---

## 🚀 Phase 1 — Infrastructure Kubernetes (Kind)

### Tâche 1.1 — Créer la structure du projet
- [x] Créer le répertoire `airflow-kubernetes/` avec ses sous-dossiers

### Tâche 1.2 — Installer Kind et kubectl
- [x] Vérifier/installer Kind (Kubernetes in Docker)
- [x] Vérifier/installer kubectl
- [x] Créer `kind-config.yaml` (1 control-plane + 2 workers)

### Tâche 1.3 — Créer le cluster Kind
- [x] Exécuter `scripts/setup_kind.sh`
- [x] Vérifier : `kubectl get nodes` → 3 nœuds Ready
- [x] Créer le namespace `airflow`

### Tâche 1.4 — Environnement Docker Compose (dev)
- [x] Créer `docker-compose.yaml` (Airflow LocalExecutor)
- [x] Créer `Dockerfile` (image custom avec kubectl + providers K8s)
- [x] Créer `.env` et `requirements.txt`

---

## 📦 Phase 2 — Déploiement Airflow sur Kubernetes

### Tâche 2.1 — Manifestes YAML
- [x] Créer `namespace.yaml`
- [x] Créer `airflow-rbac.yaml` (ServiceAccount + RBAC)
- [x] Créer `airflow-configmap.yaml` (KubernetesExecutor config)
- [x] Créer `airflow-pvc.yaml` (DAGs + Logs)
- [x] Créer `postgres.yaml` (DB Airflow)
- [x] Créer `airflow-scheduler.yaml`
- [x] Créer `airflow-webserver.yaml`

### Tâche 2.2 — Déploiement YAML
- [x] Exécuter `scripts/deploy_airflow.sh yaml`
- [x] Vérifier : `kubectl get pods -n airflow` → tous Running
- [x] Accéder à l'UI : http://localhost:8081

### Tâche 2.3 — Alternative Helm
- [x] Créer `values-airflow-helm.yaml`
- [x] Commande : `scripts/deploy_airflow.sh helm`

---

## 🔧 Phase 3 — DAGs Kubernetes

### Tâche 3.1 — Hello Kubernetes (`hello_kubernetes_dag.py`)
- [x] Vérifier l'environnement d'exécution
- [x] Tester les modules Python Kubernetes
- [x] Tester la connectivité au cluster K8s
- [x] Résumé des vérifications

### Tâche 3.2 — KubernetesPodOperator (`k8s_pod_operator_dag.py`)
- [x] Pod simple : Hello World dans un pod K8s
- [x] Pod avec variables d'environnement
- [x] Pod avec limites de ressources (CPU/mémoire)
- [x] Pod avec volume (emptyDir)

### Tâche 3.3 — Pipeline ETL K8s (`k8s_etl_pipeline_dag.py`)
- [x] Extract : générer données de ventes (CSV)
- [x] Transform : nettoyer et agréger
- [x] Load : charger les résultats
- [x] Branch : décider qualité données
- [x] Notify : succès ou alerte

---

## 🎯 Phase 4 — Exercices (Slides Jour 3)

### Exercice 1 — Comparaison des Executors (`dag_executor_comparison.py`)
- [x] Simuler LocalExecutor
- [x] Simuler CeleryExecutor
- [x] Simuler KubernetesExecutor
- [x] Tableau comparatif des 3 executors

### Exercice 2 — Gestion des Ressources (`dag_resource_management.py`)
- [x] Profil léger (notification, healthcheck)
- [x] Profil moyen (ETL, transformation)
- [x] Profil lourd (ML, agrégation massive)
- [x] Guide des ressources K8s pour Airflow

### Exercice 3 — Helm & Monitoring (`dag_helm_monitoring.py`)
- [x] Concepts Helm et values.yaml
- [x] Stratégies de monitoring (3 niveaux)
- [x] Healthcheck simulé
- [x] Rapport de monitoring

---

## 📝 Phase 5 — Questions de réflexion (`REPONSES.md`)

- [x] **Q1** — CeleryExecutor vs KubernetesExecutor (3 différences clés)
- [x] **Q2** — Gestion des ressources K8s (requests vs limits, OOMKilled)
- [x] **Q3** — Helm vs manifestes YAML bruts (avantages, cas d'usage)
- [x] **Q4** — Stratégie de scaling Airflow sur K8s (HPA, pods, coûts)

---

## 📘 Phase 6 — Documentation (`EXPLICATIONS.md`)

- [x] Architecture Airflow sur Kubernetes
- [x] Explication détaillée de chaque composant
- [x] Guide KubernetesPodOperator
- [x] Best practices production
- [x] Commandes kubectl essentielles

---

## 📸 Phase 7 — Captures d'écran (Livrables)

- [ ] `kubectl get nodes` → 3 nœuds Ready
- [ ] `kubectl get pods -n airflow` → tous Running
- [ ] Airflow UI → Vue Graph des DAGs K8s
- [ ] Logs `hello_kubernetes` → info environnement
- [ ] Logs `k8s_pod_operator_demo` → pods créés/supprimés
- [ ] `kubectl get events -n airflow` → événements du cluster

---

## 📌 Ordre de travail recommandé

| Priorité | Phase | Description | Statut |
|:---:|:---:|---|:---:|
| 1 | Phase 1 | Infrastructure Kind (cluster K8s) | ✅ |
| 2 | Phase 2 | Déploiement Airflow sur K8s | ✅ |
| 3 | Phase 3 | DAGs Kubernetes (3 DAGs) | ✅ |
| 4 | Phase 4 | Exercices slides (3 exercices) | ✅ |
| 5 | Phase 5 | Questions de réflexion `REPONSES.md` | ✅ |
| 6 | Phase 6 | Documentation `EXPLICATIONS.md` | ✅ |
| 7 | Phase 7 | Captures d'écran | ⬜ |

---

## 🛠️ Technologies utilisées

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

> **Prêt à commencer !** Lancer `scripts/setup_kind.sh`
