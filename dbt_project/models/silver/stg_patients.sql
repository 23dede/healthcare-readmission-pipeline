/*
=============================================================================
stg_patients.sql -- Silver Layer : Patient data cleaning and type casting
=============================================================================
Source      : bronze.patients_raw
Destination : silver.stg_patients
Materialization : view

Transformations:
  1. Explicit type casting (age SMALLINT, dates DATE, etc.)
  2. Text normalization (trim, consistent upper/lower)
  3. Exclusion of corrupted rows (null age, null patient_id)
  4. Derived columns (tranche_age, annee_admission)
  5. Renaming to documented snake_case conventions

Quality rules (see schema.yml for associated dbt tests):
  - patient_id  : NOT NULL, valid UUID format
  - age         : between 18 and 95
  - sexe        : values in ('M', 'F')
  - readmission_30j : values in (0, 1)
=============================================================================
*/

WITH source AS (

    SELECT * FROM {{ source('bronze', 'patients_raw') }}

),

nettoyage AS (

    SELECT
        patient_id::UUID                                AS patient_id,
        age::SMALLINT                                   AS age,
        UPPER(TRIM(sexe))                               AS sexe,

        CASE
            WHEN age::SMALLINT BETWEEN 18 AND 44 THEN '18-44'
            WHEN age::SMALLINT BETWEEN 45 AND 64 THEN '45-64'
            WHEN age::SMALLINT BETWEEN 65 AND 74 THEN '65-74'
            WHEN age::SMALLINT BETWEEN 75 AND 84 THEN '75-84'
            WHEN age::SMALLINT >= 85             THEN '85+'
            ELSE 'Inconnu'
        END                                             AS tranche_age,

        TRIM(pathologie)                                AS pathologie,
        UPPER(TRIM(diagnostic_principal))               AS diagnostic_principal,
        INITCAP(TRIM(service))                          AS service,
        INITCAP(TRIM(hopital_region))                   AS hopital_region,
        duree_sejour::SMALLINT                          AS duree_sejour,

        CASE
            WHEN duree_sejour::SMALLINT BETWEEN 1  AND 3  THEN 'Courte (1-3j)'
            WHEN duree_sejour::SMALLINT BETWEEN 4  AND 7  THEN 'Moyenne (4-7j)'
            WHEN duree_sejour::SMALLINT BETWEEN 8  AND 14 THEN 'Longue (8-14j)'
            WHEN duree_sejour::SMALLINT > 14              THEN 'Tres longue (>14j)'
            ELSE 'Inconnu'
        END                                             AS categorie_duree_sejour,

        nb_hospitalisations_precedentes::SMALLINT       AS nb_hospitalisations_precedentes,
        nb_medicaments::SMALLINT                        AS nb_medicaments,

        CASE
            WHEN nb_medicaments::SMALLINT >= 5 THEN TRUE
            ELSE FALSE
        END                                             AS flag_polymedication,

        date_admission::DATE                            AS date_admission,
        date_sortie::DATE                               AS date_sortie,

        EXTRACT(YEAR  FROM date_admission::DATE)::INT   AS annee_admission,
        EXTRACT(MONTH FROM date_admission::DATE)::INT   AS mois_admission,
        EXTRACT(DOW   FROM date_admission::DATE)::INT   AS jour_semaine_admission,

        INITCAP(TRIM(mode_sortie))                      AS mode_sortie,

        CASE
            WHEN TRIM(mode_sortie) IN ('SSR', 'EHPAD', 'Transfert') THEN TRUE
            ELSE FALSE
        END                                             AS flag_sortie_complexe,

        readmission_30j::SMALLINT                       AS readmission_30j,
        ingestion_timestamp::TIMESTAMP                  AS ingestion_timestamp,
        source_fichier

    FROM source

    WHERE
        patient_id IS NOT NULL
        AND age IS NOT NULL
        AND age::SMALLINT BETWEEN 18 AND 95
        AND readmission_30j::SMALLINT IN (0, 1)

)

SELECT * FROM nettoyage
