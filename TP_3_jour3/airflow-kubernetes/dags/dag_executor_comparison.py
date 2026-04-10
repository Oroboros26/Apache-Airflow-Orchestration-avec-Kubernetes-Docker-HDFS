"""
DAG 4 — Comparaison des Executors Airflow
==========================================
Ce DAG éducatif illustre les différences entre les 3 principaux
executors d'Airflow en simulant leur comportement :

  1. LocalExecutor  — Tâches en parallèle sur la même machine
  2. CeleryExecutor — Workers distribués (file de messages)
  3. KubernetesExecutor — Un pod par tâche

Exercice Slides 1 : Comprendre les executors.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import time
import os
import platform


default_args = {
    'owner': 'mohamed',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def simuler_local_executor(**context):
    """
    Simule le comportement du LocalExecutor.
    Les tâches s'exécutent dans des processus séparés
    sur la même machine que le scheduler.
    """
    print("=" * 60)
    print("  MODE : LocalExecutor")
    print("=" * 60)
    print(f"  Hostname        : {platform.node()}")
    print(f"  PID             : {os.getpid()}")
    print(f"  Parent PID      : {os.getppid()}")
    print()
    print("  📖 Concept :")
    print("  - Le scheduler lance un processus par tâche")
    print("  - Tout tourne sur la même machine")
    print("  - Parallélisme limité par parallelism dans airflow.cfg")
    print("  - Convient pour : développement, petits déploiements")
    print()
    print("  ✅ Avantages : Simple, pas de dépendance externe")
    print("  ❌ Inconvénients : Pas de scaling horizontal")
    print("=" * 60)

    return {
        'executor': 'LocalExecutor',
        'hostname': platform.node(),
        'pid': os.getpid(),
    }


def simuler_celery_executor(**context):
    """
    Simule le comportement du CeleryExecutor.
    Les tâches sont envoyées à des workers via un broker
    de messages (Redis/RabbitMQ).
    """
    print("=" * 60)
    print("  MODE : CeleryExecutor")
    print("=" * 60)
    print(f"  Hostname        : {platform.node()}")
    print(f"  PID             : {os.getpid()}")
    print()
    print("  📖 Concept :")
    print("  - Le scheduler envoie les tâches dans une file (Redis/RabbitMQ)")
    print("  - Des workers Celery consomment les tâches")
    print("  - Les workers peuvent être sur différentes machines")
    print("  - Scaling : ajouter des workers Celery")
    print()
    print("  ✅ Avantages : Scaling horizontal, distribution")
    print("  ❌ Inconvénients : ")
    print("     - Infrastructure supplémentaire (broker)")
    print("     - Workers toujours actifs (coût fixe)")
    print("     - Même image/dépendances sur tous les workers")
    print("=" * 60)

    return {
        'executor': 'CeleryExecutor',
        'hostname': platform.node(),
        'pid': os.getpid(),
    }


def simuler_kubernetes_executor(**context):
    """
    Simule le comportement du KubernetesExecutor.
    Chaque tâche crée un pod Kubernetes éphémère.
    """
    print("=" * 60)
    print("  MODE : KubernetesExecutor")
    print("=" * 60)
    print(f"  Hostname        : {platform.node()}")
    print(f"  PID             : {os.getpid()}")
    print()
    print("  📖 Concept :")
    print("  - Le scheduler crée un pod K8s pour chaque tâche")
    print("  - Le pod exécute la tâche puis est supprimé")
    print("  - Chaque tâche peut avoir sa propre image Docker")
    print("  - Scaling : géré automatiquement par Kubernetes")
    print()
    print("  ✅ Avantages :")
    print("     - Isolation complète par tâche")
    print("     - Images Docker différentes par tâche")
    print("     - Scaling automatique (pas de workers oisifs)")
    print("     - Gestion fine des ressources (CPU/mémoire)")
    print()
    print("  ❌ Inconvénients :")
    print("     - Latence de démarrage des pods (~10-30s)")
    print("     - Complexité d'infrastructure (cluster K8s)")
    print("     - Logs distribués (besoin de centralisation)")
    print("=" * 60)

    return {
        'executor': 'KubernetesExecutor',
        'hostname': platform.node(),
        'pid': os.getpid(),
    }


def comparer_executors(**context):
    """Compare les résultats des 3 simulations."""
    ti = context['ti']
    local = ti.xcom_pull(task_ids='simuler_local_executor')
    celery = ti.xcom_pull(task_ids='simuler_celery_executor')
    k8s = ti.xcom_pull(task_ids='simuler_kubernetes_executor')

    print("=" * 70)
    print("  TABLEAU COMPARATIF DES EXECUTORS")
    print("=" * 70)
    print(f"  {'Critère':30s} {'Local':12s} {'Celery':12s} {'Kubernetes':12s}")
    print("-" * 70)
    print(f"  {'Scaling horizontal':30s} {'❌':12s} {'✅':12s} {'✅':12s}")
    print(f"  {'Isolation par tâche':30s} {'❌':12s} {'❌':12s} {'✅':12s}")
    print(f"  {'Images différentes':30s} {'❌':12s} {'❌':12s} {'✅':12s}")
    print(f"  {'Coût au repos':30s} {'Faible':12s} {'Moyen':12s} {'Nul':12s}")
    print(f"  {'Latence démarrage':30s} {'Nulle':12s} {'Faible':12s} {'Moyenne':12s}")
    print(f"  {'Complexité infra':30s} {'Faible':12s} {'Moyenne':12s} {'Élevée':12s}")
    print(f"  {'Cas d usage':30s} {'Dev/Test':12s} {'Production':12s} {'Cloud/K8s':12s}")
    print("=" * 70)
    print()
    print("  💡 Recommandation :")
    print("     - Développement → LocalExecutor")
    print("     - Production classique → CeleryExecutor")
    print("     - Cloud / microservices → KubernetesExecutor")
    print("     - Hybride → CeleryKubernetesExecutor (Airflow 2.7+)")


with DAG(
    dag_id='comparaison_executors',
    default_args=default_args,
    description='Exercice — Comparaison LocalExecutor vs CeleryExecutor vs KubernetesExecutor',
    schedule_interval=None,
    start_date=datetime(2026, 4, 10),
    catchup=False,
    tags=['jour3', 'kubernetes', 'exercice', 'executors'],
) as dag:

    local = PythonOperator(
        task_id='simuler_local_executor',
        python_callable=simuler_local_executor,
    )

    celery = PythonOperator(
        task_id='simuler_celery_executor',
        python_callable=simuler_celery_executor,
    )

    k8s = PythonOperator(
        task_id='simuler_kubernetes_executor',
        python_callable=simuler_kubernetes_executor,
    )

    comparaison = PythonOperator(
        task_id='comparer_executors',
        python_callable=comparer_executors,
    )

    [local, celery, k8s] >> comparaison
