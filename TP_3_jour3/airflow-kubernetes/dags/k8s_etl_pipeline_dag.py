"""
DAG 3 — Pipeline ETL Kubernetes
================================
Pipeline ETL complet qui exécute chaque étape dans un
pod Kubernetes séparé :

  1. extract   — Génère des données de ventes (CSV)
  2. transform — Nettoie et agrège les données
  3. load      — Charge les résultats (simulation)
  4. validate  — Vérifie l'intégrité du chargement
  5. notify    — Envoie une notification de fin

Architecture :
  extract → transform → load → validate → notify

Chaque tâche tourne dans un pod isolé avec ses propres
ressources et son propre environnement Python.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import json
import csv
import os
import random
from io import StringIO


default_args = {
    'owner': 'mohamed',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}


# ═════════════════════════════════════════════════
# Fonctions Python pour le pipeline ETL
# (utilisées en mode Docker Compose / LocalExecutor)
# ═════════════════════════════════════════════════

def extract_donnees_ventes(**context):
    """
    EXTRACT — Génère un jeu de données de ventes simulé.
    En production, cela serait une lecture depuis une API,
    une base de données, ou un fichier distant.
    """
    categories = ['Électronique', 'Vêtements', 'Alimentation', 'Maison', 'Sport']
    produits = {
        'Électronique': ['Laptop', 'Smartphone', 'Tablette', 'Écouteurs', 'Moniteur'],
        'Vêtements': ['T-shirt', 'Jean', 'Veste', 'Chaussures', 'Casquette'],
        'Alimentation': ['Café', 'Chocolat', 'Pâtes', 'Huile olive', 'Miel'],
        'Maison': ['Lampe', 'Coussin', 'Tapis', 'Cadre photo', 'Bougie'],
        'Sport': ['Ballon', 'Raquette', 'Corde à sauter', 'Tapis yoga', 'Haltères'],
    }

    date_exec = context['ds']
    nb_transactions = 200

    ventes = []
    for i in range(nb_transactions):
        categorie = random.choice(categories)
        produit = random.choice(produits[categorie])
        quantite = random.randint(1, 10)
        prix_unitaire = round(random.uniform(5.0, 500.0), 2)
        montant = round(quantite * prix_unitaire, 2)

        ventes.append({
            'transaction_id': f"TXN-{date_exec}-{i:04d}",
            'date': date_exec,
            'categorie': categorie,
            'produit': produit,
            'quantite': quantite,
            'prix_unitaire': prix_unitaire,
            'montant_total': montant,
        })

    # Sauvegarder en fichier CSV temporaire
    fichier = f"/tmp/ventes_{date_exec}.csv"
    with open(fichier, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=ventes[0].keys())
        writer.writeheader()
        writer.writerows(ventes)

    print(f"✅ {nb_transactions} transactions extraites → {fichier}")
    print(f"   Taille : {os.path.getsize(fichier)} octets")

    # Retourner le chemin via XCom
    return fichier


def transform_donnees(**context):
    """
    TRANSFORM — Nettoie, valide et agrège les données.
    Calcule les métriques par catégorie.
    """
    ti = context['ti']
    fichier_source = ti.xcom_pull(task_ids='extract_donnees_ventes')
    date_exec = context['ds']

    # Lire les données
    with open(fichier_source, 'r') as f:
        reader = csv.DictReader(f)
        ventes = list(reader)

    print(f"📊 {len(ventes)} transactions lues depuis {fichier_source}")

    # Agrégation par catégorie
    stats_par_categorie = {}
    total_global = 0

    for vente in ventes:
        cat = vente['categorie']
        montant = float(vente['montant_total'])
        quantite = int(vente['quantite'])
        total_global += montant

        if cat not in stats_par_categorie:
            stats_par_categorie[cat] = {
                'nb_transactions': 0,
                'quantite_totale': 0,
                'chiffre_affaires': 0.0,
                'montant_min': float('inf'),
                'montant_max': 0.0,
            }

        stats = stats_par_categorie[cat]
        stats['nb_transactions'] += 1
        stats['quantite_totale'] += quantite
        stats['chiffre_affaires'] += montant
        stats['montant_min'] = min(stats['montant_min'], montant)
        stats['montant_max'] = max(stats['montant_max'], montant)

    # Arrondir les valeurs
    for cat, stats in stats_par_categorie.items():
        stats['chiffre_affaires'] = round(stats['chiffre_affaires'], 2)
        stats['montant_min'] = round(stats['montant_min'], 2)
        stats['montant_max'] = round(stats['montant_max'], 2)
        stats['panier_moyen'] = round(
            stats['chiffre_affaires'] / stats['nb_transactions'], 2
        )

    # Afficher le rapport
    print("\n" + "=" * 70)
    print(f"  RAPPORT DE TRANSFORMATION — {date_exec}")
    print("=" * 70)
    print(f"  {'Catégorie':20s} {'Transactions':>13s} {'CA (€)':>12s} {'Panier Moyen':>14s}")
    print("-" * 70)
    for cat, stats in sorted(stats_par_categorie.items()):
        print(
            f"  {cat:20s} {stats['nb_transactions']:>13d} "
            f"{stats['chiffre_affaires']:>12.2f} {stats['panier_moyen']:>14.2f}"
        )
    print("-" * 70)
    print(f"  {'TOTAL':20s} {len(ventes):>13d} {round(total_global, 2):>12.2f}")
    print("=" * 70)

    # Sauvegarder le résumé
    fichier_resultat = f"/tmp/ventes_resume_{date_exec}.json"
    resultat = {
        'date': date_exec,
        'nb_transactions_total': len(ventes),
        'chiffre_affaires_total': round(total_global, 2),
        'stats_par_categorie': stats_par_categorie,
    }
    with open(fichier_resultat, 'w') as f:
        json.dump(resultat, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Résumé sauvegardé → {fichier_resultat}")
    return fichier_resultat


def load_resultats(**context):
    """
    LOAD — Charge les résultats transformés.
    En production : écriture en BDD, Data Warehouse, ou Object Storage.
    Ici : simulation avec log détaillé.
    """
    ti = context['ti']
    fichier_resume = ti.xcom_pull(task_ids='transform_donnees')

    with open(fichier_resume, 'r') as f:
        donnees = json.load(f)

    print("=" * 60)
    print("  CHARGEMENT DES DONNÉES")
    print("=" * 60)
    print(f"  Date           : {donnees['date']}")
    print(f"  Transactions   : {donnees['nb_transactions_total']}")
    print(f"  CA Total       : {donnees['chiffre_affaires_total']:.2f} €")
    print(f"  Catégories     : {len(donnees['stats_par_categorie'])}")
    print()

    # Simulation chargement en base
    fichier_load = f"/tmp/ventes_loaded_{donnees['date']}.json"
    donnees['loaded_at'] = datetime.now().isoformat()
    donnees['status'] = 'loaded'

    with open(fichier_load, 'w') as f:
        json.dump(donnees, f, indent=2, ensure_ascii=False)

    print(f"  ✅ Données chargées → {fichier_load}")
    print("=" * 60)

    return fichier_load


def decide_qualite(**context):
    """
    BRANCH — Décide si la qualité des données est suffisante.
    Vérifie le nombre de transactions et le CA.
    """
    ti = context['ti']
    fichier = ti.xcom_pull(task_ids='load_resultats')

    with open(fichier, 'r') as f:
        donnees = json.load(f)

    nb_transactions = donnees['nb_transactions_total']
    ca_total = donnees['chiffre_affaires_total']

    print(f"  Vérification qualité : {nb_transactions} transactions, {ca_total:.2f} € CA")

    # Critères de qualité
    if nb_transactions >= 100 and ca_total > 1000:
        print("  ✅ Qualité OK → notification succès")
        return 'notifier_succes'
    else:
        print("  ⚠️ Qualité insuffisante → alerte")
        return 'notifier_alerte'


def notifier_succes(**context):
    """Notification de succès du pipeline."""
    print("=" * 60)
    print("  ✅ PIPELINE ETL TERMINÉ AVEC SUCCÈS")
    print("=" * 60)
    print(f"  Date d'exécution : {context['ds']}")
    print(f"  Run ID           : {context['run_id']}")
    print("  Statut           : SUCCÈS — Données chargées et validées")
    print("=" * 60)


def notifier_alerte(**context):
    """Notification d'alerte qualité."""
    print("=" * 60)
    print("  ⚠️ ALERTE QUALITÉ — PIPELINE ETL")
    print("=" * 60)
    print(f"  Date d'exécution : {context['ds']}")
    print("  Problème         : Données insuffisantes ou CA trop faible")
    print("  Action requise   : Vérifier la source de données")
    print("=" * 60)


with DAG(
    dag_id='k8s_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL complet — Extract, Transform, Load, Validate',
    schedule_interval='0 6 * * *',
    start_date=datetime(2026, 4, 10),
    catchup=False,
    tags=['jour3', 'kubernetes', 'etl', 'pipeline'],
) as dag:

    extract = PythonOperator(
        task_id='extract_donnees_ventes',
        python_callable=extract_donnees_ventes,
    )

    transform = PythonOperator(
        task_id='transform_donnees',
        python_callable=transform_donnees,
    )

    load = PythonOperator(
        task_id='load_resultats',
        python_callable=load_resultats,
    )

    brancher = BranchPythonOperator(
        task_id='decider_qualite',
        python_callable=decide_qualite,
    )

    succes = PythonOperator(
        task_id='notifier_succes',
        python_callable=notifier_succes,
    )

    alerte = PythonOperator(
        task_id='notifier_alerte',
        python_callable=notifier_alerte,
    )

    rapport_final = BashOperator(
        task_id='rapport_final',
        bash_command='echo "📋 Rapport pipeline ETL K8s — {{ ds }} — terminé à $(date)"',
        trigger_rule='none_failed_min_one_success',
    )

    extract >> transform >> load >> brancher >> [succes, alerte] >> rapport_final
