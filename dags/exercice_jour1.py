from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def get_today_date() -> None:
    today = datetime.today().strftime("%Y-%m-%d")
    print(f"Date du jour: {today}")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


with DAG(
    dag_id="exercice_jour1",
    default_args=default_args,
    description="TP Jour 1: premier DAG avec 3 taches",
    schedule_interval=None,
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["tp", "jour1"],
) as dag:
    t1 = BashOperator(
        task_id="debut_workflow",
        bash_command='echo "Debut du workflow"',
    )

    t2 = PythonOperator(
        task_id="date_du_jour",
        python_callable=get_today_date,
    )

    t3 = BashOperator(
        task_id="fin_workflow",
        bash_command='echo "Fin du workflow"',
    )

    t1 >> t2 >> t3