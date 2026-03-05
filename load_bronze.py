"""
=============================================================================
load_bronze.py -- Couche Bronze : Ingestion données hospitalières synthétiques
=============================================================================
Projet  : Healthcare Readmission Risk Pipeline
Couche  : Bronze -- données brutes, aucune transformation métier
Stack   : Python · Faker · pandas · psycopg2 · PostgreSQL

Description :
    Génère ~50 000 séjours patients synthétiques via Faker et les ingère
    dans la table bronze.patients_raw de PostgreSQL.
    Chaque exécution est horodatée et loggée dans bronze.ingestion_logs.

Usage :
    python load_bronze.py [--rows 50000] [--batch 1000]
=============================================================================
"""

import os
import uuid
import random
import logging
import argparse
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

DB_CONFIG = {
    "host":     os.getenv("PG_HOST",     "localhost"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "dbname":   os.getenv("PG_DB",       "healthcare_dw"),
    "user":     os.getenv("PG_USER",     "postgres"),
    "password": os.getenv("PG_PASSWORD", "postgres"),
}

DEFAULT_NB_ROWS    = 50_000
DEFAULT_BATCH_SIZE =  1_000
LOG_FILE = "logs/bronze_ingestion.log"

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# MEDICAL REFERENCE DATA
# ---------------------------------------------------------------------------

PATHOLOGIES = {
    "Insuffisance cardiaque":       ("I50", 0.22),
    "Diabète type 2":               ("E11", 0.20),
    "BPCO":                         ("J44", 0.15),
    "Pneumonie":                    ("J18", 0.13),
    "Insuffisance rénale":          ("N18", 0.10),
    "Accident vasculaire cérébral": ("I63", 0.08),
    "Sepsis":                       ("A41", 0.07),
    "Fracture du fémur":            ("S72", 0.05),
}

SERVICES = [
    "Cardiologie", "Pneumologie", "Endocrinologie", "Médecine interne",
    "Neurologie", "Gériatrie", "Réanimation", "Néphrologie",
]

REGIONS_FRANCE = [
    "Île-de-France", "Auvergne-Rhône-Alpes", "Provence-Alpes-Côte d'Azur",
    "Occitanie", "Nouvelle-Aquitaine", "Hauts-de-France",
    "Grand Est", "Bretagne", "Pays de la Loire", "Normandie",
]

MODES_SORTIE = ["Domicile", "SSR", "EHPAD", "Transfert", "Décès"]

# ---------------------------------------------------------------------------
# DATA GENERATION
# ---------------------------------------------------------------------------

def generer_sejours(nb_rows: int, seed: int = 42) -> pd.DataFrame:
    """
    Generate nb_rows synthetic hospital stays.

    The target variable readmission_30j is built with clinically coherent logic:
    older patients, more prior hospitalizations, polypharmacy, and complex
    discharge destinations each increase the readmission probability.
    """
    fake = Faker("fr_FR")
    Faker.seed(seed)
    random.seed(seed)
    np.random.seed(seed)

    log.info(f"Generating {nb_rows:,} synthetic stays (seed={seed})...")

    noms_patho  = list(PATHOLOGIES.keys())
    codes_cim10 = [v[0] for v in PATHOLOGIES.values()]
    probs_patho = [v[1] for v in PATHOLOGIES.values()]
    indices_patho = np.random.choice(len(noms_patho), size=nb_rows, p=probs_patho)

    rows = []
    for i in range(nb_rows):

        patient_id = str(uuid.uuid4())
        age = int(np.random.beta(a=5, b=2) * 77 + 18)
        sexe = random.choice(["M", "F"])

        idx_patho = indices_patho[i]
        pathologie = noms_patho[idx_patho]
        diagnostic_cim10 = codes_cim10[idx_patho]
        service = random.choice(SERVICES)
        region = random.choice(REGIONS_FRANCE)

        duree_sejour = max(1, min(int(np.random.gamma(shape=2.5, scale=3)), 45))
        nb_hospi_precedentes = min(int(np.random.poisson(lam=1.2)), 10)
        nb_medicaments = min(max(0, int(np.random.normal(loc=6, scale=3))), 20)

        date_admission = fake.date_between(start_date="-2y", end_date="today")
        date_sortie = date_admission + timedelta(days=duree_sejour)

        if age > 75:
            mode_sortie = random.choices(MODES_SORTIE, weights=[0.40, 0.30, 0.15, 0.05, 0.10])[0]
        else:
            mode_sortie = random.choices(MODES_SORTIE, weights=[0.65, 0.15, 0.05, 0.10, 0.05])[0]

        # Risk score logic
        score_risque = 0.10
        if age > 70:                        score_risque += 0.12
        if age > 80:                        score_risque += 0.08
        if nb_hospi_precedentes >= 2:       score_risque += 0.15
        if nb_hospi_precedentes >= 4:       score_risque += 0.10
        if nb_medicaments >= 8:             score_risque += 0.08
        if duree_sejour >= 10:              score_risque += 0.06
        if mode_sortie in ["SSR", "EHPAD"]: score_risque += 0.07
        if pathologie == "Sepsis":          score_risque += 0.12
        if pathologie == "Insuffisance cardiaque": score_risque += 0.08

        score_risque = min(max(score_risque, 0.05), 0.85)
        readmission_30j = int(np.random.binomial(1, score_risque))

        rows.append({
            "patient_id":                      patient_id,
            "age":                             age,
            "sexe":                            sexe,
            "pathologie":                      pathologie,
            "diagnostic_principal":            diagnostic_cim10,
            "service":                         service,
            "hopital_region":                  region,
            "duree_sejour":                    duree_sejour,
            "nb_hospitalisations_precedentes": nb_hospi_precedentes,
            "nb_medicaments":                  nb_medicaments,
            "date_admission":                  date_admission.isoformat(),
            "date_sortie":                     date_sortie.isoformat(),
            "mode_sortie":                     mode_sortie,
            "readmission_30j":                 readmission_30j,
            "score_risque_theorique":          round(score_risque, 4),
            "ingestion_timestamp":             datetime.utcnow().isoformat(),
            "source_fichier":                  "synthetic_faker_v1",
        })

    df = pd.DataFrame(rows)
    taux = df["readmission_30j"].mean()
    log.info(f"Dataset generated -- readmission rate: {taux:.1%}")
    log.info(f"Mean age: {df['age'].mean():.1f} | Mean LOS: {df['duree_sejour'].mean():.1f} days")
    return df


# ---------------------------------------------------------------------------
# POSTGRESQL SETUP
# ---------------------------------------------------------------------------

SQL_CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS bronze;"

SQL_CREATE_TABLE_PATIENTS = """
CREATE TABLE IF NOT EXISTS bronze.patients_raw (
    id                               SERIAL PRIMARY KEY,
    patient_id                       UUID        NOT NULL,
    age                              SMALLINT,
    sexe                             CHAR(1),
    pathologie                       VARCHAR(60),
    diagnostic_principal             VARCHAR(10),
    service                          VARCHAR(50),
    hopital_region                   VARCHAR(50),
    duree_sejour                     SMALLINT,
    nb_hospitalisations_precedentes  SMALLINT,
    nb_medicaments                   SMALLINT,
    date_admission                   DATE,
    date_sortie                      DATE,
    mode_sortie                      VARCHAR(20),
    readmission_30j                  SMALLINT,
    score_risque_theorique           NUMERIC(5,4),
    ingestion_timestamp              TIMESTAMP,
    source_fichier                   VARCHAR(50)
);
"""

SQL_CREATE_TABLE_LOGS = """
CREATE TABLE IF NOT EXISTS bronze.ingestion_logs (
    id               SERIAL PRIMARY KEY,
    run_timestamp    TIMESTAMP   NOT NULL DEFAULT NOW(),
    nb_lignes        INTEGER,
    taux_readmission NUMERIC(5,4),
    duree_secondes   NUMERIC(8,2),
    statut           VARCHAR(20),
    message          TEXT
);
"""

def initialiser_base(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(SQL_CREATE_SCHEMA)
        cur.execute(SQL_CREATE_TABLE_PATIENTS)
        cur.execute(SQL_CREATE_TABLE_LOGS)
    conn.commit()
    log.info("Bronze schema and tables initialized.")


# ---------------------------------------------------------------------------
# BATCH INSERT
# ---------------------------------------------------------------------------

def inserer_batch(conn, df: pd.DataFrame, batch_size: int) -> None:
    colonnes = [
        "patient_id", "age", "sexe", "pathologie", "diagnostic_principal",
        "service", "hopital_region", "duree_sejour",
        "nb_hospitalisations_precedentes", "nb_medicaments",
        "date_admission", "date_sortie", "mode_sortie",
        "readmission_30j", "score_risque_theorique",
        "ingestion_timestamp", "source_fichier",
    ]

    sql_insert = f"INSERT INTO bronze.patients_raw ({', '.join(colonnes)}) VALUES %s"
    total = len(df)
    inseres = 0

    with conn.cursor() as cur:
        for debut in range(0, total, batch_size):
            lot = df.iloc[debut : debut + batch_size]
            valeurs = [tuple(row[col] for col in colonnes) for _, row in lot.iterrows()]
            execute_values(cur, sql_insert, valeurs)
            conn.commit()
            inseres += len(lot)
            log.info(f"  Progress: {inseres:,}/{total:,} rows ({inseres/total*100:.0f}%)")

    log.info(f"Insert complete -- {inseres:,} rows.")


def sauvegarder_csv(df: pd.DataFrame, chemin: str = "data/raw/patients_raw.csv") -> None:
    os.makedirs(os.path.dirname(chemin), exist_ok=True)
    df.to_csv(chemin, index=False, encoding="utf-8-sig")
    log.info(f"CSV saved: {chemin} ({len(df):,} rows)")


def logger_run(conn, nb_lignes, taux, duree, statut, msg) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO bronze.ingestion_logs (nb_lignes, taux_readmission, duree_secondes, statut, message) VALUES (%s, %s, %s, %s, %s)",
            (nb_lignes, round(taux, 4), round(duree, 2), statut, msg),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main(nb_rows: int = DEFAULT_NB_ROWS, batch_size: int = DEFAULT_BATCH_SIZE) -> None:
    debut = datetime.utcnow()
    log.info("=" * 60)
    log.info("START -- Bronze pipeline: Healthcare Readmission")
    log.info(f"Parameters: {nb_rows:,} rows, batch={batch_size}")
    log.info("=" * 60)

    df = generer_sejours(nb_rows)
    sauvegarder_csv(df)

    log.info(f"Connecting to PostgreSQL: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        log.error(f"PostgreSQL connection failed: {e}")
        raise

    try:
        initialiser_base(conn)
        inserer_batch(conn, df, batch_size)
        duree_s = (datetime.utcnow() - debut).total_seconds()
        taux = df["readmission_30j"].mean()
        logger_run(conn, len(df), taux, duree_s, "SUCCESS", f"{nb_rows:,} rows in {duree_s:.1f}s")
        log.info(f"SUCCESS -- Total duration: {duree_s:.1f}s | Readmission rate: {taux:.1%}")
    except Exception as e:
        duree_s = (datetime.utcnow() - debut).total_seconds()
        logger_run(conn, 0, 0.0, duree_s, "FAILURE", str(e))
        log.error(f"FAILURE: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze layer ingestion")
    parser.add_argument("--rows",  type=int, default=DEFAULT_NB_ROWS)
    parser.add_argument("--batch", type=int, default=DEFAULT_BATCH_SIZE)
    args = parser.parse_args()
    main(nb_rows=args.rows, batch_size=args.batch)
