"""
DAG 6 — Helm & Monitoring Kubernetes
=====================================
Exercice pratique sur le déploiement Airflow avec Helm
et le monitoring du cluster Kubernetes.

Exercice Slides 3 : Helm deployment + monitoring.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import json
import os


default_args = {
    'owner': 'mohamed',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def expliquer_helm(**context):
    """Explique les concepts Helm pour le déploiement Airflow."""
    print("=" * 60)
    print("  HELM — Gestionnaire de packages Kubernetes")
    print("=" * 60)
    print()
    print("  📖 Qu'est-ce que Helm ?")
    print("     - Le 'apt/brew' de Kubernetes")
    print("     - Gère les applications K8s via des 'Charts'")
    print("     - Un Chart = ensemble de manifestes YAML templatés")
    print()
    print("  📦 Chart Airflow officiel :")
    print("     helm repo add apache-airflow https://airflow.apache.org")
    print("     helm install airflow apache-airflow/airflow")
    print()
    print("  📄 Fichier values.yaml :")
    print("     - Personnalise l'installation (executor, images, etc.)")
    print("     - Override des valeurs par défaut du Chart")
    print()

    valeurs_importantes = {
        'executor': 'KubernetesExecutor',
        'defaultAirflowRepository': 'apache/airflow',
        'defaultAirflowTag': '2.8.1',
        'webserver.replicas': 1,
        'scheduler.replicas': 1,
        'workers.replicas': 0,  # Pas de workers avec K8sExecutor
        'postgresql.enabled': True,
        'redis.enabled': False,  # Pas de Redis avec K8sExecutor
        'dags.persistence.enabled': True,
        'dags.gitSync.enabled': False,
        'logs.persistence.enabled': True,
    }

    print("  📋 Valeurs clés pour K8sExecutor :")
    for cle, valeur in valeurs_importantes.items():
        print(f"     {cle}: {valeur}")

    print("=" * 60)
    return valeurs_importantes


def expliquer_monitoring(**context):
    """Explique les stratégies de monitoring Airflow sur K8s."""
    print("=" * 60)
    print("  MONITORING — Airflow sur Kubernetes")
    print("=" * 60)
    print()
    print("  🔍 Niveaux de monitoring :")
    print()
    print("  1️⃣ Niveau Airflow (métriques internes) :")
    print("     - Tâches en succès/échec/retry")
    print("     - Durée d'exécution des DAGs")
    print("     - File d'attente du scheduler")
    print("     - Endpoint : /api/v1/dags, /health")
    print()
    print("  2️⃣ Niveau Kubernetes (infrastructure) :")
    print("     - kubectl top nodes       → CPU/mémoire nœuds")
    print("     - kubectl top pods        → CPU/mémoire pods")
    print("     - kubectl get events      → événements cluster")
    print("     - kubectl describe pod X  → détails d'un pod")
    print()
    print("  3️⃣ Niveau Application (observabilité) :")
    print("     - Prometheus + Grafana (métriques)")
    print("     - ELK/Loki (logs centralisés)")
    print("     - Jaeger/Tempo (tracing distribué)")
    print()
    print("  📊 Commandes kubectl utiles :")

    commandes = [
        ("kubectl get pods -n airflow", "Lister les pods Airflow"),
        ("kubectl logs <pod> -n airflow", "Voir les logs d'un pod"),
        ("kubectl describe pod <pod> -n airflow", "Détails d'un pod"),
        ("kubectl top pods -n airflow", "Consommation CPU/mémoire"),
        ("kubectl get events -n airflow --sort-by=.lastTimestamp", "Événements récents"),
        ("kubectl exec -it <pod> -- bash", "Shell dans un pod"),
    ]

    print()
    for cmd, desc in commandes:
        print(f"     $ {cmd}")
        print(f"       → {desc}")
        print()

    print("=" * 60)
    return {'monitoring_levels': ['airflow', 'kubernetes', 'application']}


def simuler_healthcheck(**context):
    """Simule un healthcheck complet du cluster."""
    print("=" * 60)
    print("  HEALTHCHECK — Vérification de l'état du système")
    print("=" * 60)

    checks = {
        'airflow_webserver': True,
        'airflow_scheduler': True,
        'postgresql': True,
        'kubernetes_api': True,
        'dag_parsing': True,
        'task_execution': True,
    }

    all_ok = True
    for composant, status in checks.items():
        emoji = "✅" if status else "❌"
        print(f"  {emoji} {composant:25s} : {'OK' if status else 'ERREUR'}")
        if not status:
            all_ok = False

    print()
    if all_ok:
        print("  🟢 SYSTÈME OPÉRATIONNEL — Tous les composants fonctionnent")
    else:
        print("  🔴 DÉGRADÉ — Certains composants nécessitent une attention")

    print("=" * 60)
    return {'healthy': all_ok, 'checks': checks}


def generer_rapport_monitoring(**context):
    """Génère un rapport de monitoring complet."""
    ti = context['ti']
    helm_info = ti.xcom_pull(task_ids='expliquer_helm')
    monitoring = ti.xcom_pull(task_ids='expliquer_monitoring')
    health = ti.xcom_pull(task_ids='simuler_healthcheck')

    print("=" * 60)
    print("  RAPPORT DE MONITORING")
    print("=" * 60)
    print(f"  Date : {context['ds']}")
    print(f"  Executor configuré : {helm_info.get('executor', 'N/A')}")
    print(f"  Niveaux monitoring : {', '.join(monitoring.get('monitoring_levels', []))}")
    print(f"  Santé globale : {'🟢 OK' if health.get('healthy') else '🔴 Dégradé'}")
    print("=" * 60)


with DAG(
    dag_id='helm_monitoring_k8s',
    default_args=default_args,
    description='Exercice — Helm deployment et monitoring Kubernetes',
    schedule_interval=None,
    start_date=datetime(2026, 4, 10),
    catchup=False,
    tags=['jour3', 'kubernetes', 'exercice', 'helm', 'monitoring'],
) as dag:

    t_helm = PythonOperator(
        task_id='expliquer_helm',
        python_callable=expliquer_helm,
    )

    t_monitoring = PythonOperator(
        task_id='expliquer_monitoring',
        python_callable=expliquer_monitoring,
    )

    t_health = PythonOperator(
        task_id='simuler_healthcheck',
        python_callable=simuler_healthcheck,
    )

    t_rapport = PythonOperator(
        task_id='generer_rapport_monitoring',
        python_callable=generer_rapport_monitoring,
    )

    [t_helm, t_monitoring] >> t_health >> t_rapport
