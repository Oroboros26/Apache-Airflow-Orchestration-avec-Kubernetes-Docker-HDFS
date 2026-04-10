"""
DAG 1 — Hello Kubernetes
========================
DAG d'introduction qui affiche les informations de l'environnement
d'exécution et vérifie la connectivité Kubernetes.

Ce DAG utilise uniquement des PythonOperator pour valider
que l'environnement Airflow est correctement configuré.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import platform
import os


default_args = {
    'owner': 'mohamed',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


def afficher_info_environnement(**context):
    """Affiche les informations de l'environnement d'exécution."""
    info = {
        'hostname': platform.node(),
        'os': platform.platform(),
        'python_version': platform.python_version(),
        'airflow_home': os.environ.get('AIRFLOW_HOME', 'non défini'),
        'executor': os.environ.get('AIRFLOW__CORE__EXECUTOR', 'non défini'),
        'dag_id': context['dag'].dag_id,
        'run_id': context['run_id'],
        'execution_date': str(context['ds']),
    }

    print("=" * 60)
    print("  INFORMATIONS ENVIRONNEMENT AIRFLOW")
    print("=" * 60)
    for cle, valeur in info.items():
        print(f"  {cle:25s} : {valeur}")
    print("=" * 60)

    return info


def verifier_modules_kubernetes(**context):
    """Vérifie que les modules Python Kubernetes sont installés."""
    modules_requis = {
        'kubernetes': False,
        'airflow.providers.cncf.kubernetes': False,
    }

    for module in modules_requis:
        try:
            __import__(module)
            modules_requis[module] = True
            print(f"  ✅ {module} — installé")
        except ImportError:
            print(f"  ❌ {module} — MANQUANT")

    tous_installes = all(modules_requis.values())
    print(f"\n  Résultat : {'✅ Tous les modules sont prêts' if tous_installes else '❌ Modules manquants'}")

    return {'modules': modules_requis, 'ready': tous_installes}


def verifier_connectivite_k8s(**context):
    """Tente de se connecter au cluster Kubernetes."""
    try:
        from kubernetes import client, config

        # Essayer la config in-cluster d'abord, puis kubeconfig
        try:
            config.load_incluster_config()
            mode = "in-cluster"
        except config.ConfigException:
            config.load_kube_config()
            mode = "kubeconfig"

        v1 = client.CoreV1Api()
        nodes = v1.list_node()

        print(f"  ✅ Connecté au cluster Kubernetes (mode: {mode})")
        print(f"  📊 Nombre de nœuds : {len(nodes.items)}")
        for node in nodes.items:
            nom = node.metadata.name
            status = node.status.conditions[-1].type if node.status.conditions else "Unknown"
            print(f"     - {nom} ({status})")

        return {'connected': True, 'mode': mode, 'nodes': len(nodes.items)}

    except Exception as e:
        print(f"  ⚠️ Connexion Kubernetes non disponible : {e}")
        print("  ℹ️ C'est normal si vous exécutez en mode Docker Compose uniquement.")
        print("  ℹ️ Déployez le cluster Kind pour activer la connectivité K8s.")
        return {'connected': False, 'error': str(e)}


def resume_verification(**context):
    """Résume les résultats des vérifications."""
    ti = context['ti']
    env_info = ti.xcom_pull(task_ids='afficher_info_environnement')
    modules_info = ti.xcom_pull(task_ids='verifier_modules_kubernetes')
    k8s_info = ti.xcom_pull(task_ids='verifier_connectivite_k8s')

    print("=" * 60)
    print("  RÉSUMÉ DES VÉRIFICATIONS")
    print("=" * 60)
    print(f"  Environnement  : ✅ OK")
    print(f"  Modules Python : {'✅ OK' if modules_info and modules_info.get('ready') else '❌ Incomplet'}")
    print(f"  Cluster K8s    : {'✅ Connecté' if k8s_info and k8s_info.get('connected') else '⚠️ Non connecté'}")
    print("=" * 60)
    print("\n  🎉 Hello Kubernetes depuis Airflow !")


with DAG(
    dag_id='hello_kubernetes',
    default_args=default_args,
    description='DAG introductif — vérification env Kubernetes',
    schedule_interval=None,
    start_date=datetime(2026, 4, 10),
    catchup=False,
    tags=['jour3', 'kubernetes', 'intro'],
) as dag:

    t1 = PythonOperator(
        task_id='afficher_info_environnement',
        python_callable=afficher_info_environnement,
    )

    t2 = PythonOperator(
        task_id='verifier_modules_kubernetes',
        python_callable=verifier_modules_kubernetes,
    )

    t3 = PythonOperator(
        task_id='verifier_connectivite_k8s',
        python_callable=verifier_connectivite_k8s,
    )

    t4 = PythonOperator(
        task_id='resume_verification',
        python_callable=resume_verification,
    )

    [t1, t2] >> t3 >> t4
