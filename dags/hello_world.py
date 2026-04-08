
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


# Arguments par defaut

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def print_hello() -> None:
    print("Hello from Airflow!")


with DAG(
    dag_id="hello_world",
    default_args=default_args,
    description="My first DAG: print hello then date",
    schedule_interval="@daily",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["learning"],
) as dag:
    hello_task = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello,
    )

    date_task = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    hello_task >> date_task