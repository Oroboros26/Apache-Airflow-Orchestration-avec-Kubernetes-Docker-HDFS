from __future__ import annotations

import json
import logging
from datetime import date

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.models import Variable

DEFAULT_REGIONS = [
    {"nom": "Île-de-France", "lat": 48.8566, "lon": 2.3522},
    {"nom": "Occitanie", "lat": 43.6047, "lon": 1.4442},
    {"nom": "Nouvelle-Aquitaine", "lat": 44.8378, "lon": -0.5792},
    {"nom": "Auvergne-Rhône-Alpes", "lat": 45.7640, "lon": 4.8357},
    {"nom": "Hauts-de-France", "lat": 50.6292, "lon": 3.0573},
]


@dag(
    dag_id="energie_meteo_dag_dynamic",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Europe/Paris"),
    catchup=False,
    tags=["rte", "energie", "meteo", "dynamic-mapping"],
)
def energie_meteo_dag_dynamic():
    @task(retries=2, retry_delay=pendulum.duration(minutes=1))
    def verifier_apis() -> bool:
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
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                raise ValueError(f"API {nom} indisponible (status={response.status_code})")
            logging.info("API %s disponible", nom)

        return True

    @task()
    def charger_config_regions() -> list[dict]:
        """
        Charge la configuration régions depuis Variable Airflow `regions_energie`.
        Optionnel: applique un filtre via Variable `regions_exclues`.
        """
        raw_regions = Variable.get(
            "regions_energie",
            default_var=json.dumps(DEFAULT_REGIONS, ensure_ascii=False),
        )
        regions = json.loads(raw_regions)

        raw_exclues = Variable.get("regions_exclues", default_var="[]")
        regions_exclues = set(json.loads(raw_exclues))

        if regions_exclues:
            regions = [r for r in regions if r.get("nom") not in regions_exclues]

        if not regions:
            raise ValueError("Aucune région disponible après chargement/filtrage")

        logging.info("%s region(s) chargee(s): %s", len(regions), [r["nom"] for r in regions])
        return regions

    @task(retries=2, retry_delay=pendulum.duration(minutes=1))
    def extraire_meteo_region(region: dict) -> dict:
        base_url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": region["lat"],
            "longitude": region["lon"],
            "daily": "sunshine_duration,wind_speed_10m_max",
            "timezone": "Europe/Paris",
            "forecast_days": 1,
        }

        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()

        daily = payload.get("daily", {})
        sunshine_duration_s = (daily.get("sunshine_duration") or [0.0])[0] or 0.0
        wind_kmh = (daily.get("wind_speed_10m_max") or [0.0])[0] or 0.0

        result = {
            "region": region["nom"],
            "ensoleillement_h": round(float(sunshine_duration_s) / 3600.0, 2),
            "vent_kmh": round(float(wind_kmh), 2),
        }
        logging.info("Meteo %s: %s", region["nom"], result)
        return result

    @task(retries=2, retry_delay=pendulum.duration(minutes=1))
    def collecter_production_electrique(regions: list[dict]) -> dict:
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
        records = response.json().get("results", [])

        noms_regions = {r["nom"] for r in regions}
        accumulation = {nom: {"solaire": [], "eolien": []} for nom in noms_regions}

        for rec in records:
            region = rec.get("libelle_region")
            if region in accumulation:
                accumulation[region]["solaire"].append(float(rec.get("solaire") or 0.0))
                accumulation[region]["eolien"].append(float(rec.get("eolien") or 0.0))

        production = {}
        for nom, values in accumulation.items():
            s_vals = values["solaire"]
            e_vals = values["eolien"]
            production[nom] = {
                "solaire_mw": round(sum(s_vals) / len(s_vals), 2) if s_vals else 0.0,
                "eolien_mw": round(sum(e_vals) / len(e_vals), 2) if e_vals else 0.0,
            }

        logging.info("Production agrégée: %s", production)
        return production

    @task()
    def analyser_correlation(meteos: list[dict], production: dict) -> dict:
        analyse = {}

        for meteo in meteos:
            region = meteo["region"]
            ensoleillement = float(meteo.get("ensoleillement_h") or 0.0)
            vent = float(meteo.get("vent_kmh") or 0.0)

            prod = production.get(region, {})
            solaire = float(prod.get("solaire_mw") or 0.0)
            eolien = float(prod.get("eolien_mw") or 0.0)

            alertes_region = []
            if ensoleillement > 6 and solaire <= 1000:
                alertes_region.append(
                    f"ALERTE SOLAIRE: {ensoleillement:.1f}h de soleil mais {solaire:.0f} MW"
                )
            if vent > 30 and eolien <= 2000:
                alertes_region.append(f"ALERTE EOLIEN: vent {vent:.1f} km/h mais {eolien:.0f} MW")
            if solaire > 0 and ensoleillement == 0:
                alertes_region.append("ANOMALIE DONNEES: solaire > 0 avec ensoleillement = 0")

            analyse[region] = {
                "alertes": alertes_region,
                "ensoleillement_h": ensoleillement,
                "vent_kmh": vent,
                "solaire_mw": solaire,
                "eolien_mw": eolien,
                "statut": "ALERTE" if alertes_region else "OK",
            }

        logging.info("Analyse correlation: %s", analyse)
        return analyse

    @task()
    def generer_rapport_energie(analyse: dict) -> str:
        today = date.today().isoformat()

        rapport = {
            "date": today,
            "source": "RTE eCO2mix + Open-Meteo",
            "pipeline": "energie_meteo_dag_dynamic",
            "regions": analyse,
            "resume": {
                "nb_regions_analysees": len(analyse),
                "nb_alertes": sum(1 for r in analyse.values() if r.get("statut") == "ALERTE"),
                "regions_en_alerte": [r for r, d in analyse.items() if d.get("statut") == "ALERTE"],
            },
        }

        output_path = f"/tmp/rapport_energie_dynamic_{today}.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(rapport, f, ensure_ascii=False, indent=2)

        logging.info("Rapport dynamique sauvegarde: %s", output_path)
        return output_path

    readiness = verifier_apis()
    regions = charger_config_regions()

    meteos = extraire_meteo_region.expand(region=regions)
    production = collecter_production_electrique(regions)

    readiness >> meteos
    readiness >> production

    analyse = analyser_correlation(meteos, production)
    generer_rapport_energie(analyse)


energie_meteo_dag_dynamic()
