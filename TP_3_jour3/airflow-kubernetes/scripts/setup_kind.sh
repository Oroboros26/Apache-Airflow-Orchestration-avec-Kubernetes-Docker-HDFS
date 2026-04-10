#!/bin/bash
# ══════════════════════════════════════════════
# Script : setup_kind.sh
# Crée un cluster Kubernetes local avec Kind
# ══════════════════════════════════════════════

set -e

CLUSTER_NAME="airflow-cluster"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="${SCRIPT_DIR}/../kubernetes"

echo "═══════════════════════════════════════════════"
echo "  INSTALLATION DU CLUSTER KIND"
echo "═══════════════════════════════════════════════"

# ── 1. Vérifier/Installer Kind ──
if ! command -v kind &> /dev/null; then
    echo "📦 Installation de Kind..."
    [ "$(uname -m)" = x86_64 ] && curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.22.0/kind-linux-amd64
    chmod +x ./kind
    sudo mv ./kind /usr/local/bin/kind
    echo "  ✅ Kind installé"
else
    echo "  ✅ Kind déjà installé ($(kind version))"
fi

# ── 2. Vérifier/Installer kubectl ──
if ! command -v kubectl &> /dev/null; then
    echo "📦 Installation de kubectl..."
    curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
    chmod +x kubectl
    sudo mv kubectl /usr/local/bin/kubectl
    echo "  ✅ kubectl installé"
else
    echo "  ✅ kubectl déjà installé ($(kubectl version --client --short 2>/dev/null || kubectl version --client))"
fi

# ── 3. Supprimer l'ancien cluster si existant ──
if kind get clusters 2>/dev/null | grep -q "${CLUSTER_NAME}"; then
    echo "🗑️  Suppression de l'ancien cluster '${CLUSTER_NAME}'..."
    kind delete cluster --name "${CLUSTER_NAME}"
fi

# ── 4. Créer le cluster ──
echo "🚀 Création du cluster Kind '${CLUSTER_NAME}'..."
kind create cluster \
    --config "${K8S_DIR}/kind-config.yaml" \
    --name "${CLUSTER_NAME}" \
    --wait 120s

echo "  ✅ Cluster créé"

# ── 5. Vérifier le cluster ──
echo ""
echo "📊 État du cluster :"
kubectl cluster-info --context "kind-${CLUSTER_NAME}"
echo ""
kubectl get nodes
echo ""

# ── 6. Créer le namespace Airflow ──
echo "📁 Création du namespace 'airflow'..."
kubectl apply -f "${K8S_DIR}/namespace.yaml"
echo "  ✅ Namespace créé"

echo ""
echo "═══════════════════════════════════════════════"
echo "  ✅ CLUSTER KIND PRÊT"
echo "═══════════════════════════════════════════════"
echo "  Cluster  : ${CLUSTER_NAME}"
echo "  Nœuds    : $(kubectl get nodes --no-headers | wc -l)"
echo "  Context  : kind-${CLUSTER_NAME}"
echo ""
echo "  Étape suivante : ./deploy_airflow.sh"
echo "═══════════════════════════════════════════════"
