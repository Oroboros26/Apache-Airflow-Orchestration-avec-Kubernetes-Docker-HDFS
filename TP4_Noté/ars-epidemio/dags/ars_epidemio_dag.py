"""
DAG : ars_epidemio_dag
Pipeline de surveillance épidémiologique — ARS Occitanie
Collecte IAS® → Archivage → Calcul indicateurs → PostgreSQL → Alerte/Bulletin → Rapport
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import sys
from datetime import datetime, timedelta
from typing import Optional

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_semaine(context: dict) -> str:
    """Retourne la semaine ISO au format YYYY-SXX depuis execution_date."""
    execution_date = context["execution_date"]
    year, week, _ = execution_date.isocalendar()
    return f"{year}-S{week:02d}"


# ---------------------------------------------------------------------------
# Étape 4 — Collecte des données IAS®
# ---------------------------------------------------------------------------

def collecter_donnees_ias(**context) -> str:
    """Télécharge les CSV IAS® et retourne le chemin du fichier JSON créé."""
    semaine: str = _get_semaine(context)
    archive_path: str = Variable.get("archive_base_path", default_var="/data/ars")
    output_dir: str = f"{archive_path}/raw"

    sys.path.insert(0, "/opt/airflow/scripts")
    from collecte_sursaud import (
        DATASETS_IAS,
        agreger_semaine,
        filtrer_semaine,
        sauvegarder_donnees,
        telecharger_csv_ias,
    )

    resultats: dict = {}
    for syndrome, url in DATASETS_IAS.items():
        rows_all = telecharger_csv_ias(url)
        rows_sem = filtrer_semaine(rows_all, semaine)
        resultats[syndrome] = agreger_semaine(rows_sem, syndrome, semaine)

    chemin = sauvegarder_donnees(resultats, semaine, output_dir)
    logger.info(f"Collecte terminée pour {semaine} : {chemin}")
    return chemin


# ---------------------------------------------------------------------------
# Étape 5 — Archivage local + Vérification
# ---------------------------------------------------------------------------

def archiver_local(**context) -> str:
    """Organise le fichier brut dans la structure d'archivage partitionnée."""
    semaine: str = _get_semaine(context)
    annee: str = semaine.split("-")[0]
    num_sem: str = semaine.split("-")[1]

    chemin_source: str = context["task_instance"].xcom_pull(
        task_ids="collecte.collecter_donnees_sursaud"
    )
    if not chemin_source:
        raise FileNotFoundError("Aucun chemin reçu via XCom depuis la collecte")

    archive_dir: str = f"/data/ars/raw/{annee}/{num_sem}"
    os.makedirs(archive_dir, exist_ok=True)

    chemin_dest: str = f"{archive_dir}/sursaud_{semaine}.json"
    shutil.copy2(chemin_source, chemin_dest)

    logger.info(f"ARCHIVE_OK:{chemin_dest}")
    return chemin_dest


def verifier_archive(**context) -> bool:
    """Vérifie que le fichier d'archive existe et n'est pas vide."""
    semaine: str = _get_semaine(context)
    annee: str = semaine.split("-")[0]
    num_sem: str = semaine.split("-")[1]

    chemin: str = f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json"
    if not os.path.exists(chemin):
        raise FileNotFoundError(f"Archive manquante : {chemin}")

    taille: int = os.path.getsize(chemin)
    if taille == 0:
        raise ValueError(f"Archive vide : {chemin}")

    logger.info(f"ARCHIVE_VALIDE:{chemin} ({taille} octets)")
    return True


# ---------------------------------------------------------------------------
# Étape 6 — Calcul des indicateurs épidémiques
# ---------------------------------------------------------------------------

def calculer_indicateurs_epidemiques(**context) -> str:
    """Calcule z-score, classification et R0 pour chaque syndrome."""
    semaine: str = _get_semaine(context)
    annee: str = semaine.split("-")[0]
    num_sem: str = semaine.split("-")[1]

    # Lire les données collectées
    chemin_donnees: str = f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json"
    with open(chemin_donnees, "r", encoding="utf-8") as f:
        donnees = json.load(f)

    sys.path.insert(0, "/opt/airflow/scripts")
    from calcul_indicateurs import calculer_indicateurs_complets

    indicateurs: list[dict] = calculer_indicateurs_complets(donnees, semaine)

    # Sauvegarder les indicateurs
    output_dir: str = f"/data/ars/indicateurs"
    os.makedirs(output_dir, exist_ok=True)
    output_path: str = f"{output_dir}/indicateurs_{semaine}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(indicateurs, f, ensure_ascii=False, indent=2)

    logger.info(f"INDICATEURS_OK: {len(indicateurs)} indicateurs calculés pour {semaine}")
    return output_path


# ---------------------------------------------------------------------------
# Étape 7 — Insertion dans PostgreSQL
# ---------------------------------------------------------------------------

def inserer_donnees_postgres(**context) -> None:
    """
    Insère les données hebdomadaires et les indicateurs dans PostgreSQL.
    Utilise ON CONFLICT DO UPDATE pour garantir l'idempotence.
    """
    semaine: str = _get_semaine(context)
    annee: str = semaine.split("-")[0]
    num_sem: str = semaine.split("-")[1]

    # Lire les données brutes
    chemin_donnees: str = f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json"
    with open(chemin_donnees, "r", encoding="utf-8") as f:
        donnees_brutes = json.load(f)

    # Lire les indicateurs calculés
    chemin_indic: str = f"/data/ars/indicateurs/indicateurs_{semaine}.json"
    with open(chemin_indic, "r", encoding="utf-8") as f:
        indicateurs = json.load(f)

    hook = PostgresHook(postgres_conn_id="postgres_ars")

    # Insérer les données hebdomadaires
    sql_donnees: str = """
        INSERT INTO donnees_hebdomadaires
            (semaine, syndrome, valeur_ias, seuil_min_saison, seuil_max_saison, nb_jours_donnees)
        VALUES
            (%(semaine)s, %(syndrome)s, %(valeur_ias)s, %(seuil_min)s, %(seuil_max)s, %(nb_jours)s)
        ON CONFLICT (semaine, syndrome)
        DO UPDATE SET
            valeur_ias       = EXCLUDED.valeur_ias,
            seuil_min_saison = EXCLUDED.seuil_min_saison,
            seuil_max_saison = EXCLUDED.seuil_max_saison,
            nb_jours_donnees = EXCLUDED.nb_jours_donnees,
            updated_at       = CURRENT_TIMESTAMP;
    """

    # Insérer les indicateurs
    sql_indicateurs: str = """
        INSERT INTO indicateurs_epidemiques
            (semaine, syndrome, valeur_ias, z_score, r0_estime,
             nb_saisons_reference, statut, statut_ias, statut_zscore, commentaire)
        VALUES
            (%(semaine)s, %(syndrome)s, %(valeur_ias)s, %(z_score)s, %(r0_estime)s,
             %(nb_saisons_reference)s, %(statut)s, %(statut_ias)s, %(statut_zscore)s, %(commentaire)s)
        ON CONFLICT (semaine, syndrome)
        DO UPDATE SET
            valeur_ias           = EXCLUDED.valeur_ias,
            z_score              = EXCLUDED.z_score,
            r0_estime            = EXCLUDED.r0_estime,
            nb_saisons_reference = EXCLUDED.nb_saisons_reference,
            statut               = EXCLUDED.statut,
            statut_ias           = EXCLUDED.statut_ias,
            statut_zscore        = EXCLUDED.statut_zscore,
            commentaire          = EXCLUDED.commentaire,
            updated_at           = CURRENT_TIMESTAMP;
    """

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            # Insérer les données hebdomadaires par syndrome
            syndromes_data = donnees_brutes.get("syndromes", {})
            for syndrome_code, data in syndromes_data.items():
                if data.get("valeur_ias") is not None:
                    cur.execute(sql_donnees, {
                        "semaine":    semaine,
                        "syndrome":   syndrome_code,
                        "valeur_ias": data["valeur_ias"],
                        "seuil_min":  data.get("seuil_min"),
                        "seuil_max":  data.get("seuil_max"),
                        "nb_jours":   data.get("nb_jours", 0),
                    })

            # Insérer les indicateurs calculés
            for indicateur in indicateurs:
                cur.execute(sql_indicateurs, indicateur)

            conn.commit()

    logger.info(f"{len(indicateurs)} indicateurs insérés/mis à jour pour {semaine}")


# ---------------------------------------------------------------------------
# Étape 8 — BranchPythonOperator : évaluation de la situation
# ---------------------------------------------------------------------------

def evaluer_situation_epidemique(**context) -> str:
    """
    Lit les indicateurs de la semaine depuis PostgreSQL et détermine
    le chemin d'exécution selon la situation la plus sévère.
    """
    semaine: str = _get_semaine(context)
    hook = PostgresHook(postgres_conn_id="postgres_ars")

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT statut, COUNT(*) AS nb_syndromes
                FROM indicateurs_epidemiques
                WHERE semaine = %s
                GROUP BY statut
            """, (semaine,))
            resultats = {row[0]: row[1] for row in cur.fetchall()}

    nb_urgence: int = resultats.get("URGENCE", 0)
    nb_alerte: int = resultats.get("ALERTE", 0)

    context["task_instance"].xcom_push(key="nb_urgence", value=nb_urgence)
    context["task_instance"].xcom_push(key="nb_alerte", value=nb_alerte)

    logger.info(f"Semaine {semaine}: {nb_urgence} URGENCE, {nb_alerte} ALERTE")

    if nb_urgence > 0:
        return "declencher_alerte_ars"
    elif nb_alerte > 0:
        return "envoyer_bulletin_surveillance"
    else:
        return "confirmer_situation_normale"


def declencher_alerte_ars(**context) -> None:
    """Alerte ARS déclenchée — départements en URGENCE."""
    nb_urgence: int = context["task_instance"].xcom_pull(
        task_ids="evaluer_situation_epidemique", key="nb_urgence"
    )
    logger.critical(f"ALERTE ARS DÉCLENCHÉE — {nb_urgence} syndromes en URGENCE")


def envoyer_bulletin_surveillance(**context) -> None:
    """Bulletin de surveillance envoyé — départements en ALERTE."""
    nb_alerte: int = context["task_instance"].xcom_pull(
        task_ids="evaluer_situation_epidemique", key="nb_alerte"
    )
    logger.warning(f"Bulletin de surveillance envoyé — {nb_alerte} syndromes en ALERTE")


def confirmer_situation_normale(**context) -> None:
    """Situation épidémiologique normale."""
    logger.info("Situation épidémiologique normale en Occitanie — aucune action requise")


# ---------------------------------------------------------------------------
# Étape 9 — Génération du rapport hebdomadaire
# ---------------------------------------------------------------------------

def generer_rapport_hebdomadaire(**context) -> None:
    """Génère le rapport JSON et le sauvegarde dans le volume + PostgreSQL."""
    semaine: str = _get_semaine(context)
    hook = PostgresHook(postgres_conn_id="postgres_ars")

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ie.syndrome, s.libelle,
                       ie.valeur_ias, ie.z_score, ie.r0_estime,
                       ie.statut, ie.statut_ias, ie.statut_zscore,
                       ie.nb_saisons_reference
                FROM indicateurs_epidemiques ie
                JOIN syndromes s ON ie.syndrome = s.code
                WHERE ie.semaine = %s
                ORDER BY ie.statut DESC, ie.valeur_ias DESC
            """, (semaine,))
            indicateurs = cur.fetchall()

    statuts: list[str] = [row[5] for row in indicateurs]
    if "URGENCE" in statuts:
        situation_globale = "URGENCE"
    elif "ALERTE" in statuts:
        situation_globale = "ALERTE"
    else:
        situation_globale = "NORMAL"

    nb_urgence: int = statuts.count("URGENCE")
    nb_alerte: int = statuts.count("ALERTE")

    recommandations_par_niveau: dict[str, list[str]] = {
        "URGENCE": [
            "Activation du plan de réponse épidémique régional",
            "Renforcement des équipes de surveillance dans les services d'urgences",
            "Communication renforcée auprès des professionnels de santé libéraux",
            "Notification immédiate à Santé Publique France et au Ministère de la Santé",
        ],
        "ALERTE": [
            "Surveillance renforcée des indicateurs pour les 48h suivantes",
            "Envoi d'un bulletin de surveillance aux partenaires de santé",
            "Vérification des capacités d'accueil des services d'urgences",
        ],
        "NORMAL": [
            "Maintien de la surveillance standard",
            "Prochain point épidémiologique dans 7 jours",
        ],
    }

    rapport: dict = {
        "semaine":                   semaine,
        "region":                    "Occitanie",
        "code_region":               "76",
        "date_generation":           datetime.utcnow().isoformat(),
        "situation_globale":         situation_globale,
        "nb_departements_surveilles": 13,
        "nb_syndromes_urgence":      nb_urgence,
        "nb_syndromes_alerte":       nb_alerte,
        "indicateurs": [
            {
                "syndrome":             row[0],
                "libelle":              row[1],
                "valeur_ias":           row[2],
                "z_score":              row[3],
                "r0_estime":            row[4],
                "statut":               row[5],
                "statut_ias":           row[6],
                "statut_zscore":        row[7],
                "nb_saisons_reference": row[8],
            }
            for row in indicateurs
        ],
        "recommandations":  recommandations_par_niveau[situation_globale],
        "genere_par":       "ars_epidemio_dag v1.0",
        "pipeline_version": "2.8",
    }

    # Sauvegarder dans le volume Docker
    annee: str = semaine.split("-")[0]
    num_sem: str = semaine.split("-")[1]
    local_path: str = f"/data/ars/rapports/{annee}/{num_sem}/rapport_{semaine}.json"
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(rapport, f, ensure_ascii=False, indent=2)

    # Sauvegarder dans PostgreSQL
    hook2 = PostgresHook(postgres_conn_id="postgres_ars")
    with hook2.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO rapports_ars
                    (semaine, situation_globale, nb_depts_alerte, nb_depts_urgence,
                     rapport_json, chemin_local)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (semaine) DO UPDATE SET
                    situation_globale = EXCLUDED.situation_globale,
                    nb_depts_alerte   = EXCLUDED.nb_depts_alerte,
                    nb_depts_urgence  = EXCLUDED.nb_depts_urgence,
                    rapport_json      = EXCLUDED.rapport_json,
                    chemin_local      = EXCLUDED.chemin_local,
                    updated_at        = CURRENT_TIMESTAMP
            """, (
                semaine, situation_globale,
                nb_alerte, nb_urgence,
                json.dumps(rapport, ensure_ascii=False),
                local_path,
            ))
            conn.commit()

    # Copier aussi dans output/ pour livrable
    output_path: str = f"/opt/airflow/dags/../output/rapport_{semaine}.json"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(rapport, f, ensure_ascii=False, indent=2)

    logger.info(f"Rapport {semaine} généré — Statut : {situation_globale}")


# ===========================================================================
# DÉFINITION DU DAG
# ===========================================================================

default_args = {
    "owner": "ars-occitanie",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="ars_epidemio_dag",
    default_args=default_args,
    description="Pipeline surveillance épidémiologique ARS Occitanie — IAS® Grippe & GEA",
    schedule_interval="0 6 * * 1",       # Tous les lundis à 6h UTC
    start_date=datetime(2024, 4, 1),     # Premier lundi avec données IAS valides
    catchup=True,
    max_active_runs=1,
    tags=["sante-publique", "epidemio", "docker-compose", "ars-occitanie"],
) as dag:

    # ── Étape 3 : Initialisation de la base ──────────────────────────
    init_base_donnees = PostgresOperator(
        task_id="init_base_donnees",
        postgres_conn_id="postgres_ars",
        sql="sql/init_ars_epidemio.sql",
        autocommit=True,
    )

    # ── Étape 4 : Collecte des données IAS® ─────────────────────────
    with TaskGroup("collecte") as tg_collecte:
        collecter_sursaud = PythonOperator(
            task_id="collecter_donnees_sursaud",
            python_callable=collecter_donnees_ias,
            provide_context=True,
        )

    # ── Étape 5 : Archivage local + Vérification ────────────────────
    with TaskGroup("persistance_brute") as tg_persistance_brute:
        archiver = PythonOperator(
            task_id="archiver_local",
            python_callable=archiver_local,
            provide_context=True,
        )
        verifier = PythonOperator(
            task_id="verifier_archive",
            python_callable=verifier_archive,
            provide_context=True,
        )
        archiver >> verifier

    # ── Étape 6 : Calcul des indicateurs ─────────────────────────────
    with TaskGroup("traitement") as tg_traitement:
        calculer = PythonOperator(
            task_id="calculer_indicateurs_epidemiques",
            python_callable=calculer_indicateurs_epidemiques,
            provide_context=True,
        )

    # ── Étape 7 : Insertion dans PostgreSQL ──────────────────────────
    with TaskGroup("persistance_operationnelle") as tg_persistance_op:
        inserer_postgres = PythonOperator(
            task_id="inserer_donnees_postgres",
            python_callable=inserer_donnees_postgres,
            provide_context=True,
        )

    # ── Étape 8 : Évaluation et branchement ──────────────────────────
    evaluer = BranchPythonOperator(
        task_id="evaluer_situation_epidemique",
        python_callable=evaluer_situation_epidemique,
        provide_context=True,
    )

    alerte_ars = PythonOperator(
        task_id="declencher_alerte_ars",
        python_callable=declencher_alerte_ars,
        provide_context=True,
    )

    bulletin = PythonOperator(
        task_id="envoyer_bulletin_surveillance",
        python_callable=envoyer_bulletin_surveillance,
        provide_context=True,
    )

    normale = PythonOperator(
        task_id="confirmer_situation_normale",
        python_callable=confirmer_situation_normale,
        provide_context=True,
    )

    # ── Étape 9 : Rapport hebdomadaire ───────────────────────────────
    generer_rapport = PythonOperator(
        task_id="generer_rapport_hebdomadaire",
        python_callable=generer_rapport_hebdomadaire,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        provide_context=True,
    )

    # ── Chaînage des tâches ──────────────────────────────────────────
    init_base_donnees >> tg_collecte >> tg_persistance_brute >> tg_traitement >> tg_persistance_op

    tg_persistance_op >> evaluer

    evaluer >> [alerte_ars, bulletin, normale]

    [alerte_ars, bulletin, normale] >> generer_rapport
