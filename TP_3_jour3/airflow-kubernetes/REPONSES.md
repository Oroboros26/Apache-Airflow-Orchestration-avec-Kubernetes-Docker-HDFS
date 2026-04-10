# RÉPONSES — TP Jour 3 : Airflow sur Kubernetes

---

## Q1 — CeleryExecutor vs KubernetesExecutor

**Quelles sont les 3 différences principales entre CeleryExecutor et KubernetesExecutor ? Dans quel contexte choisir l'un ou l'autre ?**

### 1. Modèle d'exécution

- **CeleryExecutor** : Des workers Celery **permanents** consomment les tâches depuis une file de messages (Redis/RabbitMQ). Les workers sont toujours actifs, même sans tâches à exécuter.
- **KubernetesExecutor** : Un **pod éphémère** est créé pour chaque tâche. Le pod est supprimé après l'exécution. Pas de workers permanents → coût nul au repos.

### 2. Isolation des tâches

- **CeleryExecutor** : Toutes les tâches partagent le **même environnement** (même image Docker, mêmes dépendances). Si la tâche A a besoin de la librairie X et la tâche B de la librairie Y, les deux doivent être dans la même image.
- **KubernetesExecutor** : Chaque tâche peut utiliser une **image Docker différente** avec ses propres dépendances. Isolation totale : une tâche qui crash n'affecte pas les autres.

### 3. Scaling

- **CeleryExecutor** : Scaling **manuel** — il faut ajouter/retirer des workers Celery. Nécessite un autoscaler séparé (ex: KEDA) pour ajuster le nombre de workers dynamiquement.
- **KubernetesExecutor** : Scaling **automatique** — Kubernetes crée les pods à la demande et les supprime après. Le cluster K8s gère l'allocation des ressources nativement.

### Quand choisir quoi

| Critère | CeleryExecutor | KubernetesExecutor |
|---|---|---|
| Latence faible | ✅ Workers déjà prêts (~0s) | ❌ Démarrage pod (~10-30s) |
| Tâches fréquentes | ✅ Pas d'overhead de création | ❌ Overhead pod par tâche |
| Tâches hétérogènes | ❌ Même image partout | ✅ Image par tâche |
| Optimisation coûts | ❌ Workers 24/7 | ✅ Pay-per-task |
| Complexité infra | Broker (Redis/RMQ) | Cluster K8s |
| **Recommandation** | Production classique, charges constantes | Cloud, microservices, charges variables |

**Compromis** : Airflow 2.7+ propose le **CeleryKubernetesExecutor** qui combine les deux. Les tâches courantes utilisent Celery (faible latence), les tâches spéciales utilisent le KubernetesPodOperator (isolation).

---

## Q2 — Gestion des ressources Kubernetes (requests vs limits)

**Expliquez la différence entre requests et limits en Kubernetes. Que se passe-t-il si un pod dépasse ses limits ? Comment configurer les ressources pour un pipeline Airflow typique ?**

### Requests vs Limits

| Concept | Request | Limit |
|---|---|---|
| **Définition** | Quantité de ressources **garantie** | Quantité **maximale** autorisée |
| **Rôle** | Utilisé par le **scheduler K8s** pour placer le pod sur un nœud | Utilisé par le **kubelet** pour contraindre le pod |
| **Si dépassé** | Impossible par définition (c'est garanti) | **CPU** : throttling (ralentissement) / **Mémoire** : OOMKilled |

### Que se passe-t-il si un pod dépasse ses limits ?

- **CPU** : Le pod est **throttlé** (ralenti). Il ne reçoit plus de cycles CPU au-delà de sa limite. Le processus tourne plus lentement mais n'est pas tué.
- **Mémoire** : Le pod reçoit un signal **OOMKilled** (Out Of Memory Killed). Le conteneur est tué et redémarré selon la `restartPolicy`. Si le pod est géré par un Deployment, Kubernetes recrée un nouveau pod.

### OOMKilled — Diagnostic et résolution

```bash
# Voir le statut du pod
kubectl describe pod <nom> -n airflow
# → State: Terminated / Reason: OOMKilled

# Solution : augmenter la limite mémoire
resources:
  requests:
    memory: "512Mi"   # Avant: 256Mi
  limits:
    memory: "1Gi"     # Avant: 512Mi
```

### Configuration recommandée pour Airflow

```yaml
# Scheduler (toujours actif, parsing des DAGs)
resources:
  requests: { cpu: 250m, memory: 512Mi }
  limits:   { cpu: 1000m, memory: 1Gi }

# Webserver (UI, API)
resources:
  requests: { cpu: 250m, memory: 512Mi }
  limits:   { cpu: 1000m, memory: 1Gi }

# Worker pod ETL léger
resources:
  requests: { cpu: 100m, memory: 128Mi }
  limits:   { cpu: 250m, memory: 256Mi }

# Worker pod ETL lourd (ML, agrégation)
resources:
  requests: { cpu: 1000m, memory: 2Gi }
  limits:   { cpu: 2000m, memory: 4Gi }
```

### Bonnes pratiques

1. **Toujours définir requests ET limits** pour chaque conteneur
2. **Ratio requests/limits** : entre 1:1 (garanti = QoS Guaranteed) et 1:2 (burst autorisé = QoS Burstable)
3. **LimitRange** : définir des valeurs par défaut au niveau du namespace
4. **ResourceQuota** : limiter la consommation totale du namespace
5. **Monitorer** avec `kubectl top pods` pour ajuster les valeurs

---

## Q3 — Helm vs Manifestes YAML bruts

**Comparez le déploiement d'Airflow via manifestes YAML bruts vs Helm Chart. Quand utiliser chacun ?**

### Manifestes YAML bruts

**Avantages :**
- **Transparence totale** : on voit exactement chaque ressource K8s créée
- **Contrôle total** : chaque champ YAML est explicitement défini
- **Éducatif** : comprendre les primitives Kubernetes (Deployment, Service, PVC, etc.)
- **Aucune dépendance** : uniquement kubectl nécessaire
- **Debugging facile** : le YAML est le YAML, pas de template

**Inconvénients :**
- **Pas de templating** : valeurs en dur, pas de variables
- **Duplication** : répétition de configurations entre environnements
- **Maintenance lourde** : les mises à jour requièrent de modifier chaque fichier
- **Pas de rollback natif** : il faut gérer les versions manuellement

### Helm Chart

**Avantages :**
- **Templating** : `values.yaml` pour personnaliser l'installation
- **Versionné** : rollback facile (`helm rollback airflow 1`)
- **Écosystème** : chart officiel Apache Airflow maintenu par la communauté
- **Un seul fichier** : `values.yaml` pour tout configurer
- **Multi-environnement** : `values-dev.yaml`, `values-prod.yaml`
- **Upgrade simple** : `helm upgrade airflow apache-airflow/airflow`

**Inconvénients :**
- **Boîte noire** : on ne voit pas les manifestes générés (sauf `helm template`)
- **Courbe d'apprentissage** : Go templates, hooks, chart structure
- **Dépendance** : Helm doit être installé et maintenu
- **Debugging complexe** : erreurs dans les templates difficiles à tracer

### Recommandation

| Contexte | Choix | Raison |
|---|---|---|
| **Apprentissage** | YAML bruts | Comprendre chaque composant K8s |
| **Prototype / PoC** | YAML bruts | Rapide, pas de tooling |
| **Production** | Helm | Maintenance, versionning, rollback |
| **Multi-environnement** | Helm | values-dev.yaml / values-prod.yaml |
| **CI/CD** | Helm + ArgoCD | GitOps, déploiement automatisé |

---

## Q4 — Stratégie de scaling Airflow sur Kubernetes

**Comment scaler Airflow sur Kubernetes pour gérer une augmentation de 10x du volume de données ? Quelles métriques surveiller ?**

### Stratégie de scaling en 4 axes

#### 1. KubernetesExecutor — Scaling automatique des workers

Le KubernetesExecutor scale **nativement** : chaque tâche = un pod. Pour 10x plus de tâches, K8s crée 10x plus de pods. **Aucune action requise** côté configuration si le cluster a assez de ressources.

```yaml
# Augmenter la taille du cluster Kind/EKS/GKE
# Ajouter des nœuds worker au cluster
```

#### 2. Scheduler — Augmenter le nombre de schedulers

Depuis Airflow 2.x, le scheduler supporte le mode **HA (High Availability)** : plusieurs instances du scheduler en parallèle.

```yaml
# values.yaml Helm
scheduler:
  replicas: 2  # 2 schedulers en HA
```

#### 3. Cluster Autoscaler — Scaling du cluster lui-même

En cloud (EKS, GKE, AKS), le **Cluster Autoscaler** ajoute/retire des nœuds automatiquement :

```yaml
# AWS EKS Cluster Autoscaler
apiVersion: autoscaling/v1
kind: HorizontalPodAutoscaler
spec:
  minReplicas: 2
  maxReplicas: 20
  targetCPUUtilizationPercentage: 70
```

#### 4. PostgreSQL — Scaler la base de données

La DB Airflow est souvent le goulot d'étranglement. Solutions :
- **PgBouncer** : connection pooling (inclus dans le chart Helm officiel)
- **Read replicas** : pour les lectures de l'UI/API
- **Cloud managed** : RDS, Cloud SQL (scaling automatique)

### Métriques à surveiller

| Métrique | Outil | Seuil d'alerte |
|---|---|---|
| **CPU/mémoire pods** | `kubectl top pods` | > 80% des limits |
| **CPU/mémoire nœuds** | `kubectl top nodes` | > 75% capacité |
| **Pods Pending** | `kubectl get pods` | > 0 pendant > 5 min |
| **Tâches en file** | Airflow UI → Pools | > 100 tâches queued |
| **Durée d'exécution** | Airflow UI → DAG runs | +50% vs baseline |
| **DB connections** | PostgreSQL pg_stat_activity | > 80% max_connections |
| **Scheduler heartbeat** | Airflow UI → Health | latence > 10s |

### Calcul de capacité (exemple)

```
Volume actuel : 100 tâches/jour, 5 DAGs
Volume 10x    : 1000 tâches/jour, 50 DAGs

Ressources actuelles :
  - 3 nœuds Kind (2 vCPU, 4Gi chacun)
  - 1 scheduler, 1 webserver

Ressources 10x :
  - 5-8 nœuds (4 vCPU, 8Gi chacun)
  - 2 schedulers (HA)
  - 2 webservers (load balanced)
  - PgBouncer pour PostgreSQL
  - Prometheus + Grafana pour monitoring
```

---

## Captures d'écran

*(À compléter après exécution du pipeline)*

1. `kubectl get nodes` — 3 nœuds Ready
2. `kubectl get pods -n airflow` — tous Running
3. Airflow UI — Vue Graph des DAGs
4. Logs `hello_kubernetes` — vérification environnement
5. Logs `comparaison_executors` — tableau comparatif
6. `kubectl get events -n airflow` — événements du cluster
