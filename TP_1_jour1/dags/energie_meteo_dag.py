from __future__ import annotations

import json
import logging
from datetime import date, timedelta

import pendulum
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

local_tz = pendulum.timezone("Europe/Paris")

REGIONS = {
    "Île-de-France": {"lat": 48.8566, "lon": 2.3522},
    "Occitanie": {"lat": 43.6047, "lon": 1.4442},
    "Nouvelle-Aquitaine": {"lat": 44.8378, "lon": -0.5792},
    "Auvergne-Rhône-Alpes": {"lat": 45.7640, "lon": 4.8357},
    "Hauts-de-France": {"lat": 50.6292, "lon": 3.0573},
}


default_args = {
    "owner": "rte-data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(minutes=90),
}


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Log structured SLA miss alerts for delayed tasks."""
    delayed_tasks = [ti.task_id for ti in blocking_task_list] if blocking_task_list else []

    logging.warning("[SLA MISS] DAG=%s delayed_tasks=%s", dag.dag_id, delayed_tasks)

    for sla in slas or []:
        try:
            delay_seconds = (sla.timestamp - sla.sla_date).total_seconds()
            logging.warning(
                "[ALERTE SLA] task_id=%s dag_id=%s sla_date=%s finished=%s delay_seconds=%.0f",
                sla.task_id,
                sla.dag_id,
                sla.sla_date,
                sla.timestamp,
                delay_seconds,
            )
        except Exception:
            logging.warning("[ALERTE SLA] event=%s", sla)


def verifier_apis(**context):
    """
    Vérifie la disponibilité des APIs Open-Meteo et éCO2mix.
    Lève une exception si une API est indisponible pour bloquer le pipeline.
    """
    apis = {
        "Open-Meteo": (
            "https://api.open-meteo.com/v1/forecast"
            "?latitude=48.8566&longitude=2.3522"
            "&daily=sunshine_duration&timezone=Europe/Paris&forecast_days=1"
        ),
        "éCO2mix": (
            "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets"
            "/eco2mix-regional-cons-def/records?limit=1&timezone=Europe%2FParis"
        ),
    }

    for nom, url in apis.items():
        try:
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                raise ValueError(f"API {nom} indisponible (status={response.status_code})")
            logging.info("API %s disponible (status=%s)", nom, response.status_code)
        except Exception as exc:
            raise ValueError(f"Echec verification API {nom}: {exc}") from exc

    logging.info("Toutes les APIs sont disponibles. Pipeline autorise a continuer.")


def collecter_meteo_regions(**context):
    """
    Collecte pour chaque région : durée d'ensoleillement (h) et vitesse max du vent (km/h).
    Retourne un dictionnaire {region: {ensoleillement_h: float, vent_kmh: float}}.
    """
    base_url = "https://api.open-meteo.com/v1/forecast"
    resultats = {}

    for region, coords in REGIONS.items():
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "daily": "sunshine_duration,wind_speed_10m_max",
            "timezone": "Europe/Paris",
            "forecast_days": 1,
        }

        response = requests.get(base_url, params=params, timeout=15)
        response.raise_for_status()
        payload = response.json()

        daily = payload.get("daily", {})
        sunshine_duration_s = (daily.get("sunshine_duration") or [0.0])[0] or 0.0
        wind_kmh = (daily.get("wind_speed_10m_max") or [0.0])[0] or 0.0

        ensoleillement_h = float(sunshine_duration_s) / 3600.0
        vent_kmh = float(wind_kmh)

        resultats[region] = {
            "ensoleillement_h": round(ensoleillement_h, 2),
            "vent_kmh": round(vent_kmh, 2),
        }

        logging.info(
            "Meteo %s -> ensoleillement_h=%.2f, vent_kmh=%.2f",
            region,
            ensoleillement_h,
            vent_kmh,
        )

    return resultats


def collecter_production_electrique(**context):
    """
    Collecte depuis éCO2mix la production solaire et éolienne par région.
    Agrège les valeurs horaires pour obtenir la moyenne journalière en MW.
    Retourne un dictionnaire {region: {solaire_mw: float, eolien_mw: float}}.
    """
    base_url = (
        "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets"
        "/eco2mix-regional-cons-def/records"
    )
    params = {
        "limit": 100,
        "timezone": "Europe/Paris",
    }

    response = requests.get(base_url, params=params, timeout=20)
    response.raise_for_status()
    results = response.json().get("results", [])

    accumulation = {region: {"solaire": [], "eolien": []} for region in REGIONS}

    for enregistrement in results:
        region = enregistrement.get("libelle_region")
        if region in REGIONS:
            solaire = enregistrement.get("solaire") or 0.0
            eolien = enregistrement.get("eolien") or 0.0
            accumulation[region]["solaire"].append(float(solaire))
            accumulation[region]["eolien"].append(float(eolien))

    production = {}
    for region, valeurs in accumulation.items():
        liste_solaire = valeurs["solaire"]
        liste_eolien = valeurs["eolien"]

        solaire_moy = sum(liste_solaire) / len(liste_solaire) if liste_solaire else 0.0
        eolien_moy = sum(liste_eolien) / len(liste_eolien) if liste_eolien else 0.0

        production[region] = {
            "solaire_mw": round(solaire_moy, 2),
            "eolien_mw": round(eolien_moy, 2),
        }

        logging.info(
            "Production %s -> solaire_mw=%.2f, eolien_mw=%.2f",
            region,
            solaire_moy,
            eolien_moy,
        )

    return production


def analyser_correlation(**context):
    """
    Corrèle les données météo et les données de production.

    Règles métier:
      - Si ensoleillement > 6h et production solaire <= 1000 MW -> ALERTE solaire
      - Si vent > 30 km/h et production éolienne <= 2000 MW -> ALERTE éolien
      - Bonus: solaire > 0 et ensoleillement == 0 -> anomalie de données
    """
    ti = context["ti"]

    donnees_meteo = ti.xcom_pull(task_ids="collecter_meteo_regions") or {}
    donnees_production = ti.xcom_pull(task_ids="collecter_production_electrique") or {}

    alertes = {}

    for region in REGIONS:
        meteo = donnees_meteo.get(region, {})
        production = donnees_production.get(region, {})

        alertes_region = []

        ensoleillement = float(meteo.get("ensoleillement_h", 0) or 0)
        vent = float(meteo.get("vent_kmh", 0) or 0)
        solaire = float(production.get("solaire_mw", 0) or 0)
        eolien = float(production.get("eolien_mw", 0) or 0)

        if ensoleillement > 6 and solaire <= 1000:
            alertes_region.append(
                f"ALERTE SOLAIRE: {ensoleillement:.1f}h de soleil mais seulement {solaire:.0f} MW produits"
            )

        if vent > 30 and eolien <= 2000:
            alertes_region.append(
                f"ALERTE EOLIEN: vent a {vent:.1f} km/h mais seulement {eolien:.0f} MW produits"
            )

        if solaire > 0 and ensoleillement == 0:
            alertes_region.append(
                "ANOMALIE DONNEES: production solaire positive avec ensoleillement nul"
            )

        alertes[region] = {
            "alertes": alertes_region,
            "ensoleillement_h": ensoleillement,
            "vent_kmh": vent,
            "solaire_mw": solaire,
            "eolien_mw": eolien,
            "statut": "ALERTE" if alertes_region else "OK",
        }

    nb_alertes = sum(1 for region in alertes.values() if region["statut"] == "ALERTE")
    logging.warning("%s region(s) en alerte sur %s analysees.", nb_alertes, len(REGIONS))

    return alertes


def generer_rapport_energie(**context):
    """
    Génère un rapport JSON et affiche un tableau comparatif dans les logs Airflow.
    Sauvegarde le rapport dans /tmp/rapport_energie_<YYYY-MM-DD>.json.
    Retourne le chemin du fichier généré.
    """
    ti = context["ti"]
    analyse = ti.xcom_pull(task_ids="analyser_correlation") or {}
    today = date.today().isoformat()

    logging.info("=" * 80)
    logging.info("RAPPORT ENERGIE & METEO - RTE - %s", today)
    logging.info("=" * 80)
    logging.info(
        "%-25s %10s %12s %13s %12s %8s",
        "Region",
        "Soleil(h)",
        "Vent(km/h)",
        "Solaire(MW)",
        "Eolien(MW)",
        "Statut",
    )
    logging.info("-" * 80)

    for region, data in analyse.items():
        logging.info(
            "%-25s %10.1f %12.1f %13.0f %12.0f %8s",
            region,
            float(data.get("ensoleillement_h", 0) or 0),
            float(data.get("vent_kmh", 0) or 0),
            float(data.get("solaire_mw", 0) or 0),
            float(data.get("eolien_mw", 0) or 0),
            data.get("statut", "OK"),
        )

    logging.info("=" * 80)

    rapport = {
        "date": today,
        "source": "RTE eCO2mix + Open-Meteo",
        "pipeline": "energie_meteo_dag",
        "regions": analyse,
        "resume": {
            "nb_regions_analysees": len(analyse),
            "nb_alertes": sum(1 for r in analyse.values() if r.get("statut") == "ALERTE"),
            "regions_en_alerte": [r for r, d in analyse.items() if d.get("statut") == "ALERTE"],
        },
    }

    chemin = f"/tmp/rapport_energie_{today}.json"
    with open(chemin, "w", encoding="utf-8") as file:
        json.dump(rapport, file, ensure_ascii=False, indent=2)

    logging.info("Rapport sauvegarde: %s", chemin)
    return chemin


with DAG(
    dag_id="energie_meteo_dag",
    default_args=default_args,
    description="Correlation meteo / production energetique - RTE",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Europe/Paris"),
    catchup=False,
    sla_miss_callback=sla_miss_callback,
    tags=["rte", "energie", "meteo", "open-data"],
) as dag:
    t1 = PythonOperator(
        task_id="verifier_apis",
        python_callable=verifier_apis,
    )

    t2 = PythonOperator(
        task_id="collecter_meteo_regions",
        python_callable=collecter_meteo_regions,
    )

    t3 = PythonOperator(
        task_id="collecter_production_electrique",
        python_callable=collecter_production_electrique,
    )

    t4 = PythonOperator(
        task_id="analyser_correlation",
        python_callable=analyser_correlation,
    )

    t5 = PythonOperator(
        task_id="generer_rapport_energie",
        python_callable=generer_rapport_energie,
        sla=timedelta(minutes=45),
    )

    t1 >> [t2, t3] >> t4 >> t5
