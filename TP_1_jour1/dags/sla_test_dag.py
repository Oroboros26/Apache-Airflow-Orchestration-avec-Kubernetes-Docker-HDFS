from __future__ import annotations

import logging
import time
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    delayed = [ti.task_id for ti in blocking_task_list] if blocking_task_list else []
    logging.warning("[SLA MISS TEST] dag=%s delayed=%s", dag.dag_id, delayed)


@dag(
    dag_id="sla_test_dag",
    start_date=pendulum.now("UTC").subtract(minutes=3),
    schedule="* * * * *",
    catchup=True,
    default_args={"owner": "airflow", "sla": timedelta(seconds=1)},
    sla_miss_callback=sla_miss_callback,
    tags=["test", "sla"],
)
def sla_test_dag():
    @task(sla=timedelta(seconds=1), retries=0)
    def slow_task() -> str:
        time.sleep(5)
        return "done"

    slow_task()


sla_test_dag()
