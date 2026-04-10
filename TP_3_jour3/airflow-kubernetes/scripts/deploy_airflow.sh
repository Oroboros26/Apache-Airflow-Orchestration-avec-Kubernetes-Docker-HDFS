#!/bin/bash
# ══════════════════════════════════════════════
# Script : deploy_airflow.sh
# Déploie Airflow sur le cluster Kubernetes
# (2 méthodes : YAML bruts ou Helm)
# ══════════════════════════════════════════════

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="${SCRIPT_DIR}/../kubernetes"
DAGS_DIR="${SCRIPT_DIR}/../dags"
METHOD="${1:-yaml}"  # yaml ou helm

echo "═══════════════════════════════════════════════"
echo "  DÉPLOIEMENT AIRFLOW SUR KUBERNETES"
echo "  Méthode : ${METHOD}"
echo "═══════════════════════════════════════════════"

if [ "$METHOD" = "helm" ]; then
    # ══════════════════════════════════════════
    # MÉTHODE 1 : Helm Chart (recommandé production)
    # ══════════════════════════════════════════
    echo "📦 Installation via Helm..."

    # Installer Helm si nécessaire
    if ! command -v helm &> /dev/null; then
        echo "  Installation de Helm..."
        curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
    fi

    # Ajouter le repo Airflow
    helm repo add apache-airflow https://airflow.apache.org 2>/dev/null || true
    helm repo update

    # Installer Airflow
    helm upgrade --install airflow apache-airflow/airflow \
        -f "${K8S_DIR}/values-airflow-helm.yaml" \
        --namespace airflow \
        --create-namespace \
        --timeout 10m \
        --wait

    echo "  ✅ Airflow déployé via Helm"

else
    # ══════════════════════════════════════════
    # MÉTHODE 2 : Manifestes YAML (éducatif)
    # ══════════════════════════════════════════
    echo "📄 Installation via manifestes YAML..."

    # Ordre d'application important
    echo "  1/6 — Namespace..."
    kubectl apply -f "${K8S_DIR}/namespace.yaml"

    echo "  2/6 — RBAC (ServiceAccount + Role)..."
    kubectl apply -f "${K8S_DIR}/airflow-rbac.yaml"

    echo "  3/6 — ConfigMap..."
    kubectl apply -f "${K8S_DIR}/airflow-configmap.yaml"

    echo "  4/6 — PVC (stockage)..."
    kubectl apply -f "${K8S_DIR}/airflow-pvc.yaml"

    echo "  5/6 — PostgreSQL..."
    kubectl apply -f "${K8S_DIR}/postgres.yaml"

    echo "  ⏳ Attente PostgreSQL (60s max)..."
    kubectl wait --for=condition=ready pod \
        -l app=postgres \
        -n airflow \
        --timeout=60s 2>/dev/null || echo "  ⚠️ PostgreSQL pas encore prêt, on continue..."

    echo "  6/6 — Airflow (Scheduler + Webserver)..."
    kubectl apply -f "${K8S_DIR}/airflow-scheduler.yaml"
    kubectl apply -f "${K8S_DIR}/airflow-webserver.yaml"

    echo "  ✅ Manifestes appliqués"
fi

# ── Copier les DAGs ──
echo ""
echo "📋 Copie des DAGs dans le cluster..."
# Attendre que le scheduler soit prêt
echo "  ⏳ Attente du scheduler (120s max)..."
kubectl wait --for=condition=ready pod \
    -l component=scheduler \
    -n airflow \
    --timeout=120s 2>/dev/null || echo "  ⚠️ Scheduler en cours de démarrage..."

SCHEDULER_POD=$(kubectl get pods -n airflow -l component=scheduler -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

if [ -n "$SCHEDULER_POD" ]; then
    for dag_file in "${DAGS_DIR}"/*.py; do
        if [ -f "$dag_file" ]; then
            filename=$(basename "$dag_file")
            kubectl cp "$dag_file" "airflow/${SCHEDULER_POD}:/opt/airflow/dags/${filename}"
            echo "  ✅ ${filename}"
        fi
    done
fi

echo ""
echo "═══════════════════════════════════════════════"
echo "  ÉTAT DU DÉPLOIEMENT"
echo "═══════════════════════════════════════════════"
kubectl get all -n airflow
echo ""
echo "═══════════════════════════════════════════════"
echo "  ✅ DÉPLOIEMENT TERMINÉ"
echo "═══════════════════════════════════════════════"
echo "  Airflow UI : http://localhost:8081 (NodePort 30080)"
echo "  Login      : admin / admin"
echo ""
echo "  📊 Vérification : ./verify.sh"
echo "═══════════════════════════════════════════════"
