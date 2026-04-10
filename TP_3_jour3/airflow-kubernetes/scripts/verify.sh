#!/bin/bash
# ══════════════════════════════════════════════
# Script : verify.sh
# Vérifie l'état du déploiement Airflow + K8s
# ══════════════════════════════════════════════

set -e

echo "═══════════════════════════════════════════════"
echo "  VÉRIFICATION DU DÉPLOIEMENT"
echo "═══════════════════════════════════════════════"

# ── 1. Cluster ──
echo ""
echo "1️⃣  CLUSTER KUBERNETES"
echo "─────────────────────"
kubectl cluster-info 2>/dev/null || echo "  ❌ Cluster non accessible"
echo ""
kubectl get nodes 2>/dev/null || echo "  ❌ Impossible de lister les nœuds"

# ── 2. Namespace ──
echo ""
echo "2️⃣  NAMESPACE AIRFLOW"
echo "─────────────────────"
kubectl get namespace airflow 2>/dev/null || echo "  ❌ Namespace 'airflow' non trouvé"

# ── 3. Pods ──
echo ""
echo "3️⃣  PODS"
echo "─────────────────────"
kubectl get pods -n airflow -o wide 2>/dev/null || echo "  ❌ Aucun pod trouvé"

# ── 4. Services ──
echo ""
echo "4️⃣  SERVICES"
echo "─────────────────────"
kubectl get svc -n airflow 2>/dev/null || echo "  ❌ Aucun service trouvé"

# ── 5. PVC ──
echo ""
echo "5️⃣  PERSISTENT VOLUME CLAIMS"
echo "─────────────────────"
kubectl get pvc -n airflow 2>/dev/null || echo "  ❌ Aucun PVC trouvé"

# ── 6. ConfigMaps ──
echo ""
echo "6️⃣  CONFIGMAPS"
echo "─────────────────────"
kubectl get configmap -n airflow 2>/dev/null || echo "  ❌ Aucun ConfigMap trouvé"

# ── 7. RBAC ──
echo ""
echo "7️⃣  RBAC"
echo "─────────────────────"
kubectl get serviceaccount -n airflow 2>/dev/null || echo "  ❌ ServiceAccount non trouvé"
kubectl get role -n airflow 2>/dev/null || echo "  ❌ Role non trouvé"
kubectl get rolebinding -n airflow 2>/dev/null || echo "  ❌ RoleBinding non trouvé"

# ── 8. Événements récents ──
echo ""
echo "8️⃣  ÉVÉNEMENTS RÉCENTS"
echo "─────────────────────"
kubectl get events -n airflow --sort-by=.lastTimestamp 2>/dev/null | tail -10 || echo "  Aucun événement"

# ── 9. Test de connectivité Airflow ──
echo ""
echo "9️⃣  CONNECTIVITÉ AIRFLOW"
echo "─────────────────────"
if curl -s -o /dev/null -w "%{http_code}" http://localhost:8081/health 2>/dev/null | grep -q "200"; then
    echo "  ✅ Airflow Webserver accessible sur http://localhost:8081"
    curl -s http://localhost:8081/health 2>/dev/null | python3 -m json.tool 2>/dev/null || true
else
    echo "  ⚠️ Airflow Webserver non accessible sur port 8081"
    echo "  ℹ️ Le webserver peut être en cours de démarrage..."
fi

# ── 10. Résumé ──
echo ""
echo "═══════════════════════════════════════════════"
echo "  RÉSUMÉ"
echo "═══════════════════════════════════════════════"

NB_PODS=$(kubectl get pods -n airflow --no-headers 2>/dev/null | wc -l)
NB_RUNNING=$(kubectl get pods -n airflow --no-headers 2>/dev/null | grep -c "Running" || echo "0")
NB_NODES=$(kubectl get nodes --no-headers 2>/dev/null | wc -l)

echo "  Nœuds K8s    : ${NB_NODES}"
echo "  Pods total   : ${NB_PODS}"
echo "  Pods Running : ${NB_RUNNING}"
echo "═══════════════════════════════════════════════"
