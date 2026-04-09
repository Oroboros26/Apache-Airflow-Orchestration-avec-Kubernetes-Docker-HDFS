"""
Exercice Supplémentaire 2 — Compaction hebdomadaire des logs HDFS.
Compacte les fichiers journaliers en un fichier hebdomadaire pour
éviter le problème des "small files" dans HDFS.

Schedule : Lundi 3h du matin (0 3 * * 1)
"""
import logging
import requests
from datetime import datetime, timedelta

from airflow.decorators import dag, task

log = logging.getLogger(__name__)

NAMENODE_URL = "http://namenode:9870"
HDFS_RAW_PATH = "/data/ecommerce/logs/raw"
HDFS_WEEKLY_PATH = "/data/ecommerce/logs/weekly"


@dag(
    dag_id="logs_compaction_dag",
    schedule="0 3 * * 1",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["hdfs", "compaction", "maintenance", "tp2"],
)
def logs_compaction_dag():

    @task()
    def lister_fichiers_semaine(**context) -> list:
        """
        Liste les fichiers de la semaine précédente dans HDFS.
        Utilise WebHDFS op=LISTSTATUS.
        """
        execution_date = datetime.strptime(context["ds"], "%Y-%m-%d")
        # Semaine précédente : 7 jours avant
        dates_semaine = [
            (execution_date - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(1, 8)
        ]

        resp = requests.get(
            f"{NAMENODE_URL}/webhdfs/v1{HDFS_RAW_PATH}",
            params={"op": "LISTSTATUS", "user.name": "root"},
            timeout=10,
        )
        resp.raise_for_status()

        tous_fichiers = resp.json().get("FileStatuses", {}).get("FileStatus", [])
        fichiers_semaine = []
        for f in tous_fichiers:
            nom = f["pathSuffix"]
            for d in dates_semaine:
                if d in nom:
                    fichiers_semaine.append(f"{HDFS_RAW_PATH}/{nom}")
                    break

        log.info("Fichiers de la semaine : %s", fichiers_semaine)
        return fichiers_semaine

    @task()
    def fusionner_fichiers(fichiers: list, **context) -> str:
        """
        Lit chaque fichier depuis HDFS et les concatène.
        Upload le résultat dans /data/ecommerce/logs/weekly/YYYY-WXX.log
        """
        if not fichiers:
            log.warning("Aucun fichier à compacter")
            return ""

        execution_date = datetime.strptime(context["ds"], "%Y-%m-%d")
        semaine_precedente = execution_date - timedelta(days=7)
        week_label = semaine_precedente.strftime("%Y-W%W")
        chemin_weekly = f"{HDFS_WEEKLY_PATH}/{week_label}.log"

        # Créer le répertoire weekly si nécessaire
        requests.put(
            f"{NAMENODE_URL}/webhdfs/v1{HDFS_WEEKLY_PATH}",
            params={"op": "MKDIRS", "user.name": "root"},
            timeout=5,
        )

        # Concat de tous les fichiers
        contenu_complet = []
        for fichier in fichiers:
            resp = requests.get(
                f"{NAMENODE_URL}/webhdfs/v1{fichier}",
                params={"op": "OPEN", "user.name": "root"},
                timeout=30,
                allow_redirects=True,
            )
            if resp.status_code == 200:
                contenu_complet.append(resp.text)
                log.info("Lu %d lignes de %s", resp.text.count("\n"), fichier)

        # Upload du fichier consolidé via WebHDFS
        contenu_final = "\n".join(contenu_complet)
        # Étape 1 : CREATE
        resp = requests.put(
            f"{NAMENODE_URL}/webhdfs/v1{chemin_weekly}",
            params={"op": "CREATE", "user.name": "root", "overwrite": "true"},
            allow_redirects=False,
            timeout=5,
        )
        if resp.status_code == 307:
            redirect_url = resp.headers["Location"]
            requests.put(redirect_url, data=contenu_final.encode(), timeout=30)

        log.info("Fichier weekly créé : %s (%d lignes)", chemin_weekly, contenu_final.count("\n"))
        return chemin_weekly

    @task()
    def valider_compaction(chemin_weekly: str, fichiers_source: list) -> None:
        """
        Vérifie que le fichier weekly a bien le bon nombre de lignes.
        """
        if not chemin_weekly:
            log.warning("Pas de fichier weekly à valider")
            return

        resp = requests.get(
            f"{NAMENODE_URL}/webhdfs/v1{chemin_weekly}",
            params={"op": "OPEN", "user.name": "root"},
            allow_redirects=True,
            timeout=30,
        )
        lignes_weekly = resp.text.count("\n")

        total_source = 0
        for fichier in fichiers_source:
            resp = requests.get(
                f"{NAMENODE_URL}/webhdfs/v1{fichier}",
                params={"op": "OPEN", "user.name": "root"},
                allow_redirects=True,
                timeout=30,
            )
            total_source += resp.text.count("\n")

        log.info("Lignes weekly: %d, Lignes sources: %d", lignes_weekly, total_source)
        if lignes_weekly < total_source:
            raise ValueError(
                f"Validation échouée : weekly={lignes_weekly} < sources={total_source}"
            )
        log.info("[OK] Compaction validée")

    @task()
    def supprimer_fichiers_journaliers(fichiers: list) -> None:
        """Supprime les fichiers journaliers après validation."""
        for fichier in fichiers:
            resp = requests.delete(
                f"{NAMENODE_URL}/webhdfs/v1{fichier}",
                params={"op": "DELETE", "user.name": "root"},
                timeout=5,
            )
            if resp.status_code == 200:
                log.info("Supprimé : %s", fichier)
            else:
                log.warning("Échec suppression %s : %s", fichier, resp.text)

    # Chaînage
    fichiers = lister_fichiers_semaine()
    chemin = fusionner_fichiers(fichiers)
    valider_compaction(chemin, fichiers)
    supprimer_fichiers_journaliers(fichiers)


logs_compaction_dag()
