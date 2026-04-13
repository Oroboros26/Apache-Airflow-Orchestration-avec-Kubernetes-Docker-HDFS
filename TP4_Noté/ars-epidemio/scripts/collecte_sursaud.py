#!/usr/bin/env python3
"""
Script de collecte des données IAS® (Indicateurs Avancés Sanitaires)
Grippe et Gastro-entérite — OpenHealth / data.gouv.fr
"""
from __future__ import annotations

import csv
import io
import json
import logging
import os
from datetime import datetime, date
from typing import Optional

import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# URLs directes des CSV régionaux sur data.gouv.fr
DATASETS_IAS: dict[str, str] = {
    "GRIPPE": "https://www.data.gouv.fr/api/1/datasets/r/35f46fbb-7a97-46b3-a93c-35a471033447",
    "GEA":    "https://www.data.gouv.fr/api/1/datasets/r/6c415be9-4ebf-4af5-b0dc-9867bb1ec0e3",
}

# Colonne Occitanie dans les CSV (région INSEE 76)
# Le dataset utilise les anciens codes régionaux : Midi-Pyrénées (73) + Languedoc-Roussillon (91)
# La colonne Loc_Reg76 n'existe pas, on fait la moyenne des deux anciennes régions
# Fallback sur IN_Def (indice national définitif) si les colonnes régionales sont NA
COL_OCCITANIE_OLD: list[str] = ["Loc_Reg73", "Loc_Reg91"]
COL_NATIONAL_FALLBACK: str = "IN_Def"
# Fallback saisonnier quand les colonnes IAS ne sont pas encore publiées
COL_SAISON_FALLBACK: str = "Sais_2023_2024"


def get_semaine_iso(reference_date: Optional[date] = None) -> str:
    """Retourne la semaine ISO au format YYYY-SXX."""
    if reference_date is None:
        reference_date = date.today()
    year, week, _ = reference_date.isocalendar()
    return f"{year}-S{week:02d}"


def telecharger_csv_ias(url: str) -> list[dict]:
    """
    Télécharge et parse un CSV IAS® depuis data.gouv.fr.
    Gère le séparateur ';' et le séparateur décimal ',' (format français).
    """
    logger.info(f"Téléchargement : {url}")
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    # Décodage UTF-8
    content = response.content.decode("utf-8")

    # Parser le CSV (séparateur ';', décimal ',')
    reader = csv.DictReader(io.StringIO(content), delimiter=";")
    rows: list[dict] = []
    for row in reader:
        # Convertir les virgules décimales en points
        cleaned = {
            k: v.replace(",", ".") if v not in ("NA", "", None) else None
            for k, v in row.items()
        }
        rows.append(cleaned)

    logger.info(f"{len(rows)} lignes récupérées")
    return rows


def filtrer_semaine(rows: list[dict], semaine: str) -> list[dict]:
    """
    Filtre les lignes correspondant à la semaine ISO demandée.
    Convertit PERIODE (DD-MM-YYYY) en numéro de semaine ISO.
    """
    annee_cible = int(semaine[:4])
    num_sem_cible = int(semaine.split("S")[1])

    filtered: list[dict] = []
    for row in rows:
        periode = row.get("PERIODE")
        if not periode:
            continue
        try:
            d = datetime.strptime(periode, "%d-%m-%Y").date()
            iso_year, iso_week, _ = d.isocalendar()
            if iso_year == annee_cible and iso_week == num_sem_cible:
                filtered.append(row)
        except ValueError:
            continue

    logger.info(f"{len(filtered)} jours pour la semaine {semaine}")
    return filtered


def agreger_semaine(rows: list[dict], syndrome: str, semaine: str) -> dict:
    """
    Agrège les lignes d'une semaine en une valeur IAS hebdomadaire.
    Calcule la moyenne de Loc_Reg76 et récupère les données historiques.
    """
    saisons_cols = [
        "Sais_2023_2024", "Sais_2022_2023", "Sais_2021_2022",
        "Sais_2020_2021", "Sais_2019_2020",
    ]

    valeurs_ias: list[float] = []
    min_saison_vals: list[float] = []
    max_saison_vals: list[float] = []
    historique: dict[str, list[float]] = {col: [] for col in saisons_cols}

    for row in rows:
        # Calculer IAS Occitanie = moyenne(Loc_Reg73, Loc_Reg91), fallback sur IN_Def
        ias_vals_row: list[float] = []
        for col in COL_OCCITANIE_OLD:
            v = row.get(col)
            if v is not None:
                try:
                    ias_vals_row.append(float(v))
                except ValueError:
                    pass
        if ias_vals_row:
            valeurs_ias.append(sum(ias_vals_row) / len(ias_vals_row))
        else:
            # Fallback 1: indice national définitif
            v_nat = row.get(COL_NATIONAL_FALLBACK)
            if v_nat is not None:
                try:
                    valeurs_ias.append(float(v_nat))
                except ValueError:
                    pass
            else:
                # Fallback 2: données saisonnières les plus récentes
                v_sais = row.get(COL_SAISON_FALLBACK)
                if v_sais is not None:
                    try:
                        valeurs_ias.append(float(v_sais))
                    except ValueError:
                        pass

        for col in saisons_cols:
            v = row.get(col)
            if v is not None:
                try:
                    historique[col].append(float(v))
                except ValueError:
                    pass

        for field, dest in [("MIN_Saison", min_saison_vals), ("MAX_Saison", max_saison_vals)]:
            v = row.get(field)
            if v is not None:
                try:
                    dest.append(float(v))
                except ValueError:
                    pass

    def safe_mean(lst: list[float]) -> Optional[float]:
        return round(sum(lst) / len(lst), 3) if lst else None

    return {
        "semaine":     semaine,
        "syndrome":    syndrome,
        "valeur_ias":  safe_mean(valeurs_ias),
        "seuil_min":   safe_mean(min_saison_vals),
        "seuil_max":   safe_mean(max_saison_vals),
        "nb_jours":    len(valeurs_ias),
        "historique":  {col: safe_mean(vals) for col, vals in historique.items()},
    }


def sauvegarder_donnees(donnees: dict, semaine: str, output_dir: str) -> str:
    """Sauvegarde les données IAS agrégées en JSON."""
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"ias_{semaine}.json")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({
            "semaine":     semaine,
            "collecte_le": datetime.utcnow().isoformat(),
            "source":      "IAS_OpenHealth_data.gouv.fr",
            "syndromes":   donnees,
        }, f, ensure_ascii=False, indent=2)

    logger.info(f"Données sauvegardées : {output_path}")
    return output_path


if __name__ == "__main__":
    semaine = os.environ.get("SEMAINE_CIBLE", get_semaine_iso())
    output_dir = os.environ.get("OUTPUT_DIR", "/data/ars/raw")

    resultats: dict = {}
    for syndrome, url in DATASETS_IAS.items():
        rows_all = telecharger_csv_ias(url)
        rows_sem = filtrer_semaine(rows_all, semaine)
        resultats[syndrome] = agreger_semaine(rows_sem, syndrome, semaine)

    chemin = sauvegarder_donnees(resultats, semaine, output_dir)
    print(f"COLLECTE_OK:{chemin}")
