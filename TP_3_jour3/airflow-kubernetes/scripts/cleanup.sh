#!/bin/bash
# ══════════════════════════════════════════════
# Script : cleanup.sh
# Supprime le cluster Kind et nettoie tout
# ══════════════════════════════════════════════

set -e

CLUSTER_NAME="airflow-cluster"

echo "═══════════════════════════════════════════════"
echo "  NETTOYAGE COMPLET"
echo "═══════════════════════════════════════════════"

# ── 1. Supprimer les ressources Airflow ──
echo "🗑️  Suppression des ressources Kubernetes..."
kubectl delete namespace airflow --ignore-not-found=true 2>/dev/null || true

# ── 2. Supprimer le cluster Kind ──
if command -v kind &> /dev/null; then
    if kind get clusters 2>/dev/null | grep -q "${CLUSTER_NAME}"; then
        echo "🗑️  Suppression du cluster Kind '${CLUSTER_NAME}'..."
        kind delete cluster --name "${CLUSTER_NAME}"
        echo "  ✅ Cluster supprimé"
    else
        echo "  ℹ️ Aucun cluster '${CLUSTER_NAME}' trouvé"
    fi
fi

# ── 3. Nettoyer Docker (optionnel) ──
echo ""
read -p "Nettoyer les images Docker inutilisées ? (y/N) " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    docker system prune -f 2>/dev/null || true
    echo "  ✅ Docker nettoyé"
fi

echo ""
echo "═══════════════════════════════════════════════"
echo "  ✅ NETTOYAGE TERMINÉ"
echo "═══════════════════════════════════════════════"
