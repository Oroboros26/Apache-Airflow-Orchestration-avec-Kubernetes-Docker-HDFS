"""
DAG 5 — Gestion des ressources Kubernetes
==========================================
Exercice sur la gestion des ressources (requests/limits)
dans un contexte Kubernetes.

Simule différents profils de charge et montre comment
configurer les ressources pour chaque type de tâche.

Exercice Slides 2 : Resource Management.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import os


default_args = {
    'owner': 'mohamed',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def profil_leger(**context):
    """
    Profil LÉGER — Tâche simple avec peu de ressources.
    Kubernetes requests/limits recommandés :
      requests: cpu=50m, memory=64Mi
      limits: cpu=100m, memory=128Mi
    """
    print("=" * 60)
    print("  PROFIL : Léger (notification, log, status check)")
    print("=" * 60)

    # Simulation d'une tâche légère
    donnees = {"status": "ok", "timestamp": context['ds'], "type": "healthcheck"}
    print(f"  Données produites : {json.dumps(donnees)}")

    print()
    print("  📏 Ressources K8s recommandées :")
    print("     requests : cpu=50m, memory=64Mi")
    print("     limits   : cpu=100m, memory=128Mi")
    print()
    print("  💡 Explication :")
    print("     - requests = minimum garanti par le scheduler K8s")
    print("     - limits = maximum autorisé (OOMKilled si dépassé)")
    print("     - 50m CPU = 5% d'un cœur")
    print("     - 64Mi mémoire = 64 mégaoctets")
    print("=" * 60)

    return {'profil': 'leger', 'cpu_request': '50m', 'memory_request': '64Mi'}


def profil_moyen(**context):
    """
    Profil MOYEN — Tâche de traitement de données.
    Kubernetes requests/limits recommandés :
      requests: cpu=250m, memory=256Mi
      limits: cpu=500m, memory=512Mi
    """
    print("=" * 60)
    print("  PROFIL : Moyen (ETL, transformation de données)")
    print("=" * 60)

    # Simulation traitement de données
    donnees = []
    for i in range(10000):
        donnees.append({'id': i, 'valeur': i * 3.14})

    taille_mo = len(json.dumps(donnees)) / (1024 * 1024)
    print(f"  Données traitées : {len(donnees)} enregistrements")
    print(f"  Taille mémoire   : {taille_mo:.2f} Mo")

    print()
    print("  📏 Ressources K8s recommandées :")
    print("     requests : cpu=250m, memory=256Mi")
    print("     limits   : cpu=500m, memory=512Mi")
    print()
    print("  💡 Explication :")
    print("     - 250m CPU = 25% d'un cœur (parsing, I/O)")
    print("     - 256Mi mémoire pour les DataFrames en mémoire")
    print("     - Le ratio requests/limits = 1:2 (burst autorisé)")
    print("=" * 60)

    return {'profil': 'moyen', 'cpu_request': '250m', 'memory_request': '256Mi'}


def profil_lourd(**context):
    """
    Profil LOURD — Tâche intensive (ML, agrégation massive).
    Kubernetes requests/limits recommandés :
      requests: cpu=1000m, memory=1Gi
      limits: cpu=2000m, memory=2Gi
    """
    print("=" * 60)
    print("  PROFIL : Lourd (ML, traitement massif)")
    print("=" * 60)

    # Simulation calcul intensif
    total = 0
    for i in range(100000):
        total += i ** 0.5

    print(f"  Calcul effectué : somme de 100k racines carrées = {total:.2f}")

    print()
    print("  📏 Ressources K8s recommandées :")
    print("     requests : cpu=1000m, memory=1Gi")
    print("     limits   : cpu=2000m, memory=2Gi")
    print()
    print("  💡 Explication :")
    print("     - 1000m CPU = 1 cœur complet (calcul intensif)")
    print("     - 1Gi mémoire pour les datasets volumineux")
    print("     - Node affinity recommandé si GPU nécessaire")
    print("     - tolerations pour utiliser des nœuds dédiés")
    print("=" * 60)

    return {'profil': 'lourd', 'cpu_request': '1000m', 'memory_request': '1Gi'}


def resume_ressources(**context):
    """Résume les profils de ressources."""
    ti = context['ti']
    leger = ti.xcom_pull(task_ids='profil_leger')
    moyen = ti.xcom_pull(task_ids='profil_moyen')
    lourd = ti.xcom_pull(task_ids='profil_lourd')

    print("=" * 70)
    print("  GUIDE DES RESSOURCES KUBERNETES POUR AIRFLOW")
    print("=" * 70)
    print()
    print(f"  {'Profil':15s} {'CPU Req':10s} {'CPU Lim':10s} {'Mem Req':10s} {'Mem Lim':10s}")
    print("-" * 70)
    print(f"  {'Léger':15s} {'50m':10s} {'100m':10s} {'64Mi':10s} {'128Mi':10s}")
    print(f"  {'Moyen':15s} {'250m':10s} {'500m':10s} {'256Mi':10s} {'512Mi':10s}")
    print(f"  {'Lourd':15s} {'1000m':10s} {'2000m':10s} {'1Gi':10s} {'2Gi':10s}")
    print(f"  {'GPU (ML)':15s} {'2000m':10s} {'4000m':10s} {'4Gi':10s} {'8Gi':10s}")
    print("=" * 70)
    print()
    print("  📌 Bonnes pratiques :")
    print("     1. Toujours définir requests ET limits")
    print("     2. requests ≤ limits (sinon le pod ne démarre pas)")
    print("     3. Ratio requests/limits entre 1:1 et 1:2")
    print("     4. Utiliser LimitRange et ResourceQuota par namespace")
    print("     5. Monitorer avec kubectl top pods")
    print()
    print("  ⚠️ Erreurs courantes :")
    print("     - OOMKilled : mémoire insuffisante → augmenter limits")
    print("     - CrashLoopBackOff : pod redémarre en boucle")
    print("     - Pending : pas assez de ressources sur le cluster")
    print("     - Evicted : nœud sous pression mémoire")


with DAG(
    dag_id='gestion_ressources_k8s',
    default_args=default_args,
    description='Exercice — Gestion des ressources CPU/mémoire Kubernetes',
    schedule_interval=None,
    start_date=datetime(2026, 4, 10),
    catchup=False,
    tags=['jour3', 'kubernetes', 'exercice', 'ressources'],
) as dag:

    t_leger = PythonOperator(
        task_id='profil_leger',
        python_callable=profil_leger,
    )

    t_moyen = PythonOperator(
        task_id='profil_moyen',
        python_callable=profil_moyen,
    )

    t_lourd = PythonOperator(
        task_id='profil_lourd',
        python_callable=profil_lourd,
    )

    resume = PythonOperator(
        task_id='resume_ressources',
        python_callable=resume_ressources,
    )

    [t_leger, t_moyen, t_lourd] >> resume
