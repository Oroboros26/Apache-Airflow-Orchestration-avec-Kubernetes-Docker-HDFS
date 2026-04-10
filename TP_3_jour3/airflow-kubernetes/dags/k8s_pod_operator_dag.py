"""
DAG 2 — KubernetesPodOperator Demo
====================================
Démonstration du KubernetesPodOperator pour exécuter
des tâches dans des pods Kubernetes isolés.

Chaque tâche s'exécute dans son propre pod avec
son image Docker, ses ressources et son environnement.

Prérequis : cluster Kubernetes (Kind) actif et configuré.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

try:
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
    from kubernetes.client import models as k8s
    K8S_AVAILABLE = True
except ImportError:
    K8S_AVAILABLE = False


default_args = {
    'owner': 'mohamed',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}


def verifier_prerequis(**context):
    """Vérifie que KubernetesPodOperator est disponible."""
    if not K8S_AVAILABLE:
        raise ImportError(
            "KubernetesPodOperator non disponible. "
            "Installez : pip install apache-airflow-providers-cncf-kubernetes"
        )
    print("✅ KubernetesPodOperator est disponible")
    return True


with DAG(
    dag_id='k8s_pod_operator_demo',
    default_args=default_args,
    description='Démonstration KubernetesPodOperator',
    schedule_interval=None,
    start_date=datetime(2026, 4, 10),
    catchup=False,
    tags=['jour3', 'kubernetes', 'KubernetesPodOperator'],
) as dag:

    check = PythonOperator(
        task_id='verifier_prerequis',
        python_callable=verifier_prerequis,
    )

    if K8S_AVAILABLE:
        # ─────────────────────────────────────────
        # Tâche 1 : Pod simple — Hello World
        # ─────────────────────────────────────────
        hello_pod = KubernetesPodOperator(
            task_id='hello_pod',
            name='hello-pod',
            namespace='airflow',
            image='python:3.11-slim',
            cmds=['python', '-c'],
            arguments=[
                'print("🎉 Hello depuis un Pod Kubernetes !")'
                '\nimport platform'
                '\nprint(f"Hostname: {platform.node()}")'
                '\nprint(f"Python: {platform.python_version()}")'
            ],
            get_logs=True,
            is_delete_operator_pod=True,
            in_cluster=False,
            config_file='/opt/airflow/.kube/config',
        )

        # ─────────────────────────────────────────
        # Tâche 2 : Pod avec variables d'environnement
        # ─────────────────────────────────────────
        pod_avec_env = KubernetesPodOperator(
            task_id='pod_avec_variables_env',
            name='pod-env-vars',
            namespace='airflow',
            image='python:3.11-slim',
            cmds=['python', '-c'],
            arguments=[
                'import os'
                '\nprint(f"APP_NAME: {os.environ.get(\'APP_NAME\', \'non défini\')}")'
                '\nprint(f"ENVIRONMENT: {os.environ.get(\'ENVIRONMENT\', \'non défini\')}")'
                '\nprint(f"VERSION: {os.environ.get(\'VERSION\', \'non défini\')}")'
            ],
            env_vars={
                'APP_NAME': 'airflow-k8s-tp3',
                'ENVIRONMENT': 'development',
                'VERSION': '1.0.0',
            },
            get_logs=True,
            is_delete_operator_pod=True,
            in_cluster=False,
            config_file='/opt/airflow/.kube/config',
        )

        # ─────────────────────────────────────────
        # Tâche 3 : Pod avec limites de ressources
        # ─────────────────────────────────────────
        resources_pod = KubernetesPodOperator(
            task_id='pod_avec_ressources',
            name='pod-resources',
            namespace='airflow',
            image='python:3.11-slim',
            cmds=['python', '-c'],
            arguments=[
                'import os'
                '\n# Lire les limites cgroup'
                '\ntry:'
                '\n    with open("/sys/fs/cgroup/memory.max", "r") as f:'
                '\n        mem = f.read().strip()'
                '\n    print(f"Limite mémoire cgroup: {mem}")'
                '\nexcept FileNotFoundError:'
                '\n    print("cgroup v1 ou non disponible")'
                '\nprint("✅ Pod avec ressources limitées exécuté")'
            ],
            container_resources=k8s.V1ResourceRequirements(
                requests={'cpu': '100m', 'memory': '128Mi'},
                limits={'cpu': '250m', 'memory': '256Mi'},
            ),
            get_logs=True,
            is_delete_operator_pod=True,
            in_cluster=False,
            config_file='/opt/airflow/.kube/config',
        )

        # ─────────────────────────────────────────
        # Tâche 4 : Pod avec volume partagé
        # ─────────────────────────────────────────
        volume = k8s.V1Volume(
            name='shared-data',
            empty_dir=k8s.V1EmptyDirVolumeSource(),
        )
        volume_mount = k8s.V1VolumeMount(
            name='shared-data',
            mount_path='/data',
        )

        pod_avec_volume = KubernetesPodOperator(
            task_id='pod_avec_volume',
            name='pod-volume',
            namespace='airflow',
            image='python:3.11-slim',
            cmds=['python', '-c'],
            arguments=[
                'import json, os'
                '\ndata = {"message": "données depuis le pod", "timestamp": "2026-04-10"}'
                '\nwith open("/data/output.json", "w") as f:'
                '\n    json.dump(data, f)'
                '\nprint(f"✅ Fichier écrit dans /data/output.json")'
                '\nprint(f"Contenu: {json.dumps(data, indent=2)}")'
            ],
            volumes=[volume],
            volume_mounts=[volume_mount],
            get_logs=True,
            is_delete_operator_pod=True,
            in_cluster=False,
            config_file='/opt/airflow/.kube/config',
        )

        # Chaîner les tâches
        check >> hello_pod >> pod_avec_env >> resources_pod >> pod_avec_volume

    else:
        # Fallback si K8s non disponible
        fallback = PythonOperator(
            task_id='k8s_non_disponible',
            python_callable=lambda: print(
                "⚠️ KubernetesPodOperator non disponible.\n"
                "Installez les dépendances et configurez le cluster Kind."
            ),
        )
        check >> fallback
