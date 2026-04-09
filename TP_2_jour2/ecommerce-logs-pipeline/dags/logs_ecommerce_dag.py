"""
DAG : logs_ecommerce_dag
Pipeline d'ingestion de logs e-commerce vers HDFS.
8 tâches avec branchement conditionnel basé sur le taux d'erreur.

Schedule : chaque nuit à 2h (0 2 * * *)

Architecture : Airflow et namenode partagent un volume Docker /shared/
pour le transfert de fichiers. Les commandes HDFS sont exécutées
via l'API WebHDFS (port 9870) accessible depuis le réseau Docker.
"""
import subprocess
import os
import re
import logging
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

log = logging.getLogger(__name__)

SEUIL_ERREUR_PCT = 5.0  # Seuil d'alerte : 5% d'erreurs HTTP
NAMENODE_URL = "http://namenode:9870"
SHARED_DIR = "/shared"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ─── Fonctions Python ────────────────────────────────────────────────────────

def generer_logs_journaliers(**context):
    """
    Génère 1000 lignes de logs Apache pour la date d'exécution du DAG.
    Sauvegarde dans /shared/access_<YYYY-MM-DD>.log (volume partagé avec namenode).
    Retourne le chemin du fichier (stocké dans XCom).
    """
    execution_date = context["ds"]  # Format YYYY-MM-DD
    fichier_sortie = f"{SHARED_DIR}/access_{execution_date}.log"
    script_path = "/opt/airflow/scripts/generer_logs.py"

    # Appel du script de génération
    result = subprocess.run(
        ["python3", script_path, execution_date, "1000", fichier_sortie],
        check=True,
        capture_output=True,
        text=True,
    )
    log.info("Script output: %s", result.stdout)

    # Vérifier que le fichier existe et logger sa taille
    if os.path.exists(fichier_sortie):
        taille = os.path.getsize(fichier_sortie)
        log.info("Fichier généré : %s (%d octets)", fichier_sortie, taille)
    else:
        raise FileNotFoundError(f"Le fichier {fichier_sortie} n'a pas été créé")

    return fichier_sortie


def uploader_vers_hdfs_fn(**context):
    """
    Upload le fichier log vers HDFS via WebHDFS API.
    Le fichier est accessible depuis namenode via le volume partagé /shared/.
    """
    execution_date = context["ds"]
    fichier_local = f"{SHARED_DIR}/access_{execution_date}.log"
    chemin_hdfs = f"/data/ecommerce/logs/raw/access_{execution_date}.log"

    log.info("Upload de %s vers HDFS:%s", fichier_local, chemin_hdfs)

    with open(fichier_local, "rb") as f:
        data = f.read()

    # WebHDFS CREATE — step 1: get redirect URL
    resp = requests.put(
        f"{NAMENODE_URL}/webhdfs/v1{chemin_hdfs}",
        params={"op": "CREATE", "user.name": "root", "overwrite": "true"},
        allow_redirects=False,
        timeout=10,
    )

    if resp.status_code == 307:
        redirect_url = resp.headers["Location"]
        # Step 2: upload data to DataNode
        resp2 = requests.put(redirect_url, data=data, timeout=30)
        resp2.raise_for_status()
        log.info("[OK] Upload terminé : %s (%d octets)", chemin_hdfs, len(data))
    else:
        raise Exception(f"WebHDFS CREATE échoué : {resp.status_code} {resp.text}")


def verifier_fichier_hdfs(**context):
    """Vérifie la présence du fichier dans HDFS via WebHDFS."""
    execution_date = context["ds"]
    chemin_hdfs = f"/data/ecommerce/logs/raw/access_{execution_date}.log"

    resp = requests.get(
        f"{NAMENODE_URL}/webhdfs/v1{chemin_hdfs}",
        params={"op": "GETFILESTATUS", "user.name": "root"},
        timeout=10,
    )
    if resp.status_code == 200:
        size = resp.json()["FileStatus"]["length"]
        log.info("[OK] Fichier présent dans HDFS : %s (%d bytes)", chemin_hdfs, size)
    else:
        raise FileNotFoundError(f"Fichier absent dans HDFS : {chemin_hdfs}")


def analyser_logs_hdfs_fn(**context):
    """
    Lit le fichier de logs depuis HDFS via WebHDFS et effectue l'analyse :
    - Comptage par status code
    - Top 5 URLs
    - Taux d'erreur (4xx + 5xx)
    """
    execution_date = context["ds"]
    chemin_hdfs = f"/data/ecommerce/logs/raw/access_{execution_date}.log"

    log.info("Lecture du fichier HDFS : %s", chemin_hdfs)

    # Lire le fichier depuis HDFS via WebHDFS
    resp = requests.get(
        f"{NAMENODE_URL}/webhdfs/v1{chemin_hdfs}",
        params={"op": "OPEN", "user.name": "root"},
        allow_redirects=True,
        timeout=30,
    )
    resp.raise_for_status()
    lignes = resp.text.strip().split("\n")

    log.info("Nombre total de lignes : %d", len(lignes))

    # Comptage par status code
    status_counts = {}
    url_counts = {}
    erreurs = 0
    total = len(lignes)

    for ligne in lignes:
        # Extraire status code : "METHOD URL HTTP/x.x" STATUS
        match = re.search(r'"(GET|POST|PUT|DELETE) ([^ ]+) HTTP/[0-9.]+" (\d+)', ligne)
        if match:
            url = match.group(2)
            status = int(match.group(3))

            status_counts[status] = status_counts.get(status, 0) + 1
            url_counts[url] = url_counts.get(url, 0) + 1

            if status >= 400:
                erreurs += 1

    # Affichage status codes
    log.info("=== STATUS CODES ===")
    for code, count in sorted(status_counts.items(), key=lambda x: -x[1]):
        log.info("  %d : %d requêtes", code, count)

    # Top 5 URLs
    log.info("=== TOP 5 URLS ===")
    top_urls = sorted(url_counts.items(), key=lambda x: -x[1])[:5]
    for url, count in top_urls:
        log.info("  %d : %s", count, url)

    # Taux d'erreur
    taux_pct = (erreurs / total) * 100 if total > 0 else 0
    log.info("=== TAUX ERREUR ===")
    log.info("Total: %d, Erreurs: %d (%.2f%%)", total, erreurs, taux_pct)

    # Sauvegarder pour la tâche de branchement
    fichier_taux = f"/tmp/taux_erreur_{execution_date}.txt"
    with open(fichier_taux, "w") as f:
        f.write(f"{erreurs} {total}")

    return {"total": total, "erreurs": erreurs, "taux_pct": taux_pct}


def brancher_selon_taux_erreur(**context):
    """
    Lit le taux d'erreur calculé par analyser_logs_hdfs.
    Retourne le task_id de la branche à exécuter.
    """
    execution_date = context["ds"]
    fichier_taux = f"/tmp/taux_erreur_{execution_date}.txt"

    with open(fichier_taux, "r") as f:
        contenu = f.read().strip()

    erreurs, total = contenu.split()
    erreurs = int(erreurs)
    total = int(total)

    taux_pct = (erreurs / total) * 100 if total > 0 else 0
    log.info("Taux d'erreur : %d/%d = %.2f%%", erreurs, total, taux_pct)

    if taux_pct > SEUIL_ERREUR_PCT:
        log.warning("Taux d'erreur ÉLEVÉ (%.2f%% > %.2f%%) → alerte Ops", taux_pct, SEUIL_ERREUR_PCT)
        return "alerter_equipe_ops"
    else:
        log.info("Taux d'erreur normal (%.2f%% <= %.2f%%) → rapport OK", taux_pct, SEUIL_ERREUR_PCT)
        return "archiver_rapport_ok"


def alerter_equipe_ops(**context):
    """Simule l'envoi d'une alerte à l'équipe Ops."""
    execution_date = context["ds"]
    log.warning(
        "[ALERTE] Taux d'erreur HTTP anormal détecté pour les logs du %s. "
        "Vérifiez les serveurs web.", execution_date
    )


def archiver_rapport_ok(**context):
    """Logue que tout est nominal."""
    execution_date = context["ds"]
    log.info(
        "[OK] Taux d'erreur dans les seuils normaux pour les logs du %s.", execution_date
    )


def archiver_logs_hdfs_fn(**context):
    """Déplace le fichier de raw/ vers processed/ dans HDFS via WebHDFS."""
    execution_date = context["ds"]
    source = f"/data/ecommerce/logs/raw/access_{execution_date}.log"
    destination = f"/data/ecommerce/logs/processed/access_{execution_date}.log"

    log.info("Déplacement HDFS : %s → %s", source, destination)

    resp = requests.put(
        f"{NAMENODE_URL}/webhdfs/v1{source}",
        params={
            "op": "RENAME",
            "destination": destination,
            "user.name": "root",
        },
        timeout=10,
    )
    resp.raise_for_status()
    result = resp.json()

    if result.get("boolean"):
        log.info("[OK] Fichier archivé dans la zone processed")
    else:
        raise Exception(f"Échec du déplacement HDFS : {result}")


# ─── DAG ─────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="logs_ecommerce_dag",
    default_args=default_args,
    description="Pipeline d'ingestion de logs e-commerce vers HDFS",
    schedule="0 2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ecommerce", "hdfs", "logs", "tp2"],
) as dag:

    # Tâche 1 — Générer les logs
    t_generer = PythonOperator(
        task_id="generer_logs_journaliers",
        python_callable=generer_logs_journaliers,
    )

    # Tâche 2 — Uploader vers HDFS via WebHDFS
    t_upload = PythonOperator(
        task_id="uploader_vers_hdfs",
        python_callable=uploader_vers_hdfs_fn,
    )

    # Tâche 3 — Vérifier la présence du fichier dans HDFS
    t_sensor = PythonOperator(
        task_id="hdfs_file_sensor",
        python_callable=verifier_fichier_hdfs,
    )

    # Tâche 4 — Analyser les logs HDFS
    t_analyser = PythonOperator(
        task_id="analyser_logs_hdfs",
        python_callable=analyser_logs_hdfs_fn,
    )

    # Tâche 5 — Branchement selon le taux d'erreur
    t_branch = BranchPythonOperator(
        task_id="brancher_selon_taux_erreur",
        python_callable=brancher_selon_taux_erreur,
    )

    # Tâche 6a — Alerte Ops
    t_alerte = PythonOperator(
        task_id="alerter_equipe_ops",
        python_callable=alerter_equipe_ops,
    )

    # Tâche 6b — Rapport OK
    t_archive_ok = PythonOperator(
        task_id="archiver_rapport_ok",
        python_callable=archiver_rapport_ok,
    )

    # Tâche 7 — Archiver les logs dans HDFS (raw → processed)
    t_archiver = PythonOperator(
        task_id="archiver_logs_hdfs",
        python_callable=archiver_logs_hdfs_fn,
        trigger_rule="none_failed_min_one_success",
    )

    # ─── Dépendances ─────────────────────────────────────────────────────────
    (
        t_generer
        >> t_upload
        >> t_sensor
        >> t_analyser
        >> t_branch
        >> [t_alerte, t_archive_ok]
        >> t_archiver
    )
