from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain


@dag(
    dag_id="dag_broken",
    start_date=pendulum.datetime(2024, 1, 1, tz="Europe/Paris"),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "owner": "stagiaire",
    },
)
def dag_broken() -> None:
    @task()
    def extraire() -> dict:
        resp = requests.get(
            "https://api.open-meteo.com/v1/forecast"
            "?latitude=48.8566&longitude=2.3522&current_weather=true",
            timeout=20,
        )
        resp.raise_for_status()
        return resp.json().get("current_weather", {})

    @task()
    def transformer(data: dict) -> dict:
        return {
            "temperature": data.get("temperature", 0.0),
            "vent": data.get("windspeed", 0.0),
            "statut": "OK" if float(data.get("windspeed", 0.0)) < 50 else "ALERTE",
        }

    @task()
    def charger(resultat: dict) -> None:
        logging.info("Resultat: %s", resultat)

    data = extraire()
    resultat = transformer(data)
    done = charger(resultat)

    chain(data, resultat, done)


dag_instance = dag_broken()
