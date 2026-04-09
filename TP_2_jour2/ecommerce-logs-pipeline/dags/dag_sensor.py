"""
Exercice Slides 3 — Sensor personnalisé
Attend que le fichier /tmp/go.txt existe avant de déclencher la suite.
"""
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

log = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def traitement_apres_signal(**context):
    """Tâche exécutée après détection du fichier."""
    log.info("[OK] Le fichier /tmp/go.txt a été détecté ! Lancement du traitement...")
    log.info("Traitement en cours...")
    log.info("Traitement terminé avec succès.")


with DAG(
    dag_id="dag_sensor",
    default_args=default_args,
    description="Exercice : FileSensor attend /tmp/go.txt",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["exercice", "sensor", "tp2"],
) as dag:

    t_attendre = FileSensor(
        task_id="attendre_fichier_go",
        filepath="/tmp/go.txt",
        poke_interval=10,       # Vérifie toutes les 10 secondes
        timeout=600,            # Abandonne après 10 minutes
        mode="reschedule",      # Libère le worker entre chaque poke
    )

    t_traitement = PythonOperator(
        task_id="traitement_apres_signal",
        python_callable=traitement_apres_signal,
    )

    t_attendre >> t_traitement
