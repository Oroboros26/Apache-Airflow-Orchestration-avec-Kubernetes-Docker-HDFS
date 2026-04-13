#!/usr/bin/env python3
"""
Calcul des indicateurs épidémiques IAS® — ARS Occitanie
Z-score, classification NORMAL/ALERTE/URGENCE, R0 simplifié
"""
from __future__ import annotations

import json
import logging
import os
from typing import Optional

import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def calculer_zscore(
    valeur_actuelle: float, historique: list[float]
) -> Optional[float]:
    """
    Calcule le z-score de la valeur IAS par rapport aux saisons historiques.
    Requiert au minimum 3 valeurs historiques.
    """
    valeurs_valides = [v for v in historique if v is not None]
    if len(valeurs_valides) < 3:
        logger.warning(f"Historique insuffisant ({len(valeurs_valides)} saisons)")
        return None

    try:
        moyenne = float(np.mean(valeurs_valides))
        ecart_type = float(np.std(valeurs_valides, ddof=1))
    except (ValueError, TypeError) as exc:
        logger.error(f"Erreur de calcul z-score : {exc}")
        raise

    if ecart_type == 0:
        return 0.0

    return round(float((valeur_actuelle - moyenne) / ecart_type), 4)


def classifier_statut_ias(
    valeur_ias: float,
    seuil_min: Optional[float],
    seuil_max: Optional[float],
) -> str:
    """Classifie selon les seuils MIN/MAX du dataset IAS."""
    if seuil_max is not None and valeur_ias >= seuil_max:
        return "URGENCE"
    if seuil_min is not None and valeur_ias >= seuil_min:
        return "ALERTE"
    return "NORMAL"


def classifier_statut_zscore(
    z_score: Optional[float],
    seuil_alerte_z: float = 1.5,
    seuil_urgence_z: float = 3.0,
) -> str:
    """Classifie selon le z-score par rapport à l'historique des saisons."""
    if z_score is None:
        return "NORMAL"
    if z_score >= seuil_urgence_z:
        return "URGENCE"
    if z_score >= seuil_alerte_z:
        return "ALERTE"
    return "NORMAL"


def classifier_statut_final(statut_ias: str, statut_zscore: str) -> str:
    """Retient le niveau le plus sévère entre les deux critères."""
    if "URGENCE" in (statut_ias, statut_zscore):
        return "URGENCE"
    if "ALERTE" in (statut_ias, statut_zscore):
        return "ALERTE"
    return "NORMAL"


def calculer_r0_simplifie(
    series_hebdomadaire: list[float],
    duree_infectieuse: int = 5,
) -> Optional[float]:
    """Estimation du R0 par calcul du taux de croissance moyen sur les séries IAS."""
    series_valides = [v for v in series_hebdomadaire if v is not None and v > 0]
    if len(series_valides) < 2:
        return None

    croissances = [
        (series_valides[i] - series_valides[i - 1]) / series_valides[i - 1]
        for i in range(1, len(series_valides))
    ]
    if not croissances:
        return None

    return round(max(0.0, float(1 + np.mean(croissances) * (duree_infectieuse / 7))), 4)


def calculer_indicateurs_complets(
    donnees_semaine: dict,
    semaine: str,
) -> list[dict]:
    """
    Calcule tous les indicateurs pour chaque syndrome d'une semaine.
    Retourne la liste des indicateurs prêts à être insérés en base.
    """
    resultats: list[dict] = []

    syndromes_data = donnees_semaine.get("syndromes", {})

    for syndrome_code, data in syndromes_data.items():
        valeur_ias = data.get("valeur_ias")
        if valeur_ias is None:
            logger.warning(f"Pas de valeur IAS pour {syndrome_code} semaine {semaine}")
            continue

        seuil_min = data.get("seuil_min")
        seuil_max = data.get("seuil_max")

        # Extraire les valeurs historiques des saisons
        historique_dict = data.get("historique", {})
        historique_vals = [v for v in historique_dict.values() if v is not None]

        # Z-score
        z_score = calculer_zscore(valeur_ias, historique_vals)

        # Classification
        statut_ias = classifier_statut_ias(valeur_ias, seuil_min, seuil_max)
        statut_zscore = classifier_statut_zscore(z_score)
        statut_final = classifier_statut_final(statut_ias, statut_zscore)

        # R0 (pas assez de données pour un vrai calcul historique, mais on met en place)
        r0 = None  # sera calculé quand on aura l'historique en BDD

        nb_saisons = len(historique_vals)

        indicateur = {
            "semaine":              semaine,
            "syndrome":             syndrome_code,
            "valeur_ias":           valeur_ias,
            "z_score":              z_score,
            "r0_estime":            r0,
            "nb_saisons_reference": nb_saisons,
            "statut":               statut_final,
            "statut_ias":           statut_ias,
            "statut_zscore":        statut_zscore,
            "commentaire":          f"IAS={valeur_ias}, z={z_score}, seuils=[{seuil_min},{seuil_max}]",
        }
        resultats.append(indicateur)
        logger.info(
            f"{syndrome_code} {semaine}: IAS={valeur_ias}, z={z_score}, "
            f"statut={statut_final} (IAS:{statut_ias}, z:{statut_zscore})"
        )

    return resultats


if __name__ == "__main__":
    semaine = os.environ.get("SEMAINE_CIBLE", "2025-S15")
    input_file = os.environ.get("INPUT_FILE", f"/data/ars/raw/ias_{semaine}.json")
    output_dir = os.environ.get("OUTPUT_DIR", "/data/ars/indicateurs")

    with open(input_file, "r", encoding="utf-8") as f:
        donnees = json.load(f)

    indicateurs = calculer_indicateurs_complets(donnees, semaine)

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"indicateurs_{semaine}.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(indicateurs, f, ensure_ascii=False, indent=2)

    print(f"INDICATEURS_OK:{output_path}")
