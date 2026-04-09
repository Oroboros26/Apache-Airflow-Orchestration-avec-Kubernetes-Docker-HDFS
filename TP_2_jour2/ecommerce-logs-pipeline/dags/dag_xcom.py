"""
Exercice Slides 2 — Communication XCom
- Tâche 1 : Génère une liste de 5 nombres et la retourne
- Tâche 2 : Récupère la liste via XCom et calcule sa somme
- Tâche 3 : Affiche le résultat final
"""
import random
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def generer_liste(**context):
    """Génère une liste de 5 nombres aléatoires."""
    liste = [random.randint(1, 100) for _ in range(5)]
    log.info("Liste générée : %s", liste)
    return liste


def calculer_somme(**context):
    """Récupère la liste via XCom et calcule la somme."""
    ti = context["ti"]
    liste = ti.xcom_pull(task_ids="generer_liste")
    somme = sum(liste)
    log.info("Liste reçue : %s → Somme = %d", liste, somme)
    return somme


def afficher_resultat(**context):
    """Affiche le résultat final."""
    ti = context["ti"]
    liste = ti.xcom_pull(task_ids="generer_liste")
    somme = ti.xcom_pull(task_ids="calculer_somme")
    log.info("=== RÉSULTAT FINAL ===")
    log.info("Liste : %s", liste)
    log.info("Somme : %d", somme)
    log.info("Moyenne : %.2f", somme / len(liste))


with DAG(
    dag_id="dag_xcom",
    default_args=default_args,
    description="Exercice : communication inter-tâches avec XCom",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["exercice", "xcom", "tp2"],
) as dag:

    t_generer = PythonOperator(
        task_id="generer_liste",
        python_callable=generer_liste,
    )

    t_somme = PythonOperator(
        task_id="calculer_somme",
        python_callable=calculer_somme,
    )

    t_afficher = PythonOperator(
        task_id="afficher_resultat",
        python_callable=afficher_resultat,
    )

    t_generer >> t_somme >> t_afficher
