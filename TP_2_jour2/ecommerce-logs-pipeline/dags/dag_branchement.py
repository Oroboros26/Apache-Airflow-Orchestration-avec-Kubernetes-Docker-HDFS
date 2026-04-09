"""
Exercice Slides 1 — DAG avec branchement conditionnel
Génère un nombre aléatoire :
- Si pair → tâche "pair"
- Si impair → tâche "impair"
- Converge vers une tâche finale
"""
import random
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def generer_nombre(**context):
    """Génère un nombre aléatoire et le retourne via XCom."""
    nombre = random.randint(1, 100)
    log.info("Nombre généré : %d", nombre)
    return nombre


def choisir_branche(**context):
    """Décide si le nombre est pair ou impair."""
    ti = context["ti"]
    nombre = ti.xcom_pull(task_ids="generer_nombre")
    log.info("Nombre reçu via XCom : %d", nombre)

    if nombre % 2 == 0:
        return "traitement_pair"
    else:
        return "traitement_impair"


def traiter_pair(**context):
    ti = context["ti"]
    nombre = ti.xcom_pull(task_ids="generer_nombre")
    log.info("Le nombre %d est PAIR. Résultat : %d", nombre, nombre * 2)


def traiter_impair(**context):
    ti = context["ti"]
    nombre = ti.xcom_pull(task_ids="generer_nombre")
    log.info("Le nombre %d est IMPAIR. Résultat : %d", nombre, nombre * 3 + 1)


with DAG(
    dag_id="dag_branchement",
    default_args=default_args,
    description="Exercice : branchement pair/impair avec convergence",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["exercice", "branchement", "tp2"],
) as dag:

    t_generer = PythonOperator(
        task_id="generer_nombre",
        python_callable=generer_nombre,
    )

    t_choisir = BranchPythonOperator(
        task_id="choisir_branche",
        python_callable=choisir_branche,
    )

    t_pair = PythonOperator(
        task_id="traitement_pair",
        python_callable=traiter_pair,
    )

    t_impair = PythonOperator(
        task_id="traitement_impair",
        python_callable=traiter_impair,
    )

    t_final = EmptyOperator(
        task_id="tache_finale",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    t_generer >> t_choisir >> [t_pair, t_impair] >> t_final
