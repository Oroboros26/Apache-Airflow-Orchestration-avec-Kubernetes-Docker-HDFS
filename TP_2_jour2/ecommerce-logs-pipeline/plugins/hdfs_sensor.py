"""
Exercice Supplémentaire 1 — HdfsSensor custom via WebHDFS.
Sensor qui attend qu'un fichier existe dans HDFS via l'API WebHDFS.
"""
import requests
from airflow.sensors.base import BaseSensorOperator


class HdfsFileSensor(BaseSensorOperator):
    """Sensor qui attend qu'un fichier existe dans HDFS via WebHDFS."""

    template_fields = ("hdfs_path",)

    def __init__(self, hdfs_path: str, namenode_url: str = "http://namenode:9870", **kwargs):
        super().__init__(**kwargs)
        self.hdfs_path = hdfs_path
        self.namenode_url = namenode_url

    def poke(self, context) -> bool:
        """
        Vérifie la présence du fichier via WebHDFS.
        Retourne True si le fichier existe.
        """
        try:
            resp = requests.get(
                f"{self.namenode_url}/webhdfs/v1{self.hdfs_path}",
                params={"op": "GETFILESTATUS", "user.name": "root"},
                timeout=5,
            )
            if resp.status_code == 200:
                size = resp.json()["FileStatus"]["length"]
                self.log.info("Fichier trouvé : %s (%d bytes)", self.hdfs_path, size)
                return True
        except Exception as exc:
            self.log.warning("Fichier absent ou HDFS KO : %s", exc)

        return False
