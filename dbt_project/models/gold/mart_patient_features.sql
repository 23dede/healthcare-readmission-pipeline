/*
=============================================================================
mart_patient_features.sql -- Gold Layer : ML Feature Store
=============================================================================
Sources     : silver.stg_patients, silver.stg_sejours, silver.stg_diagnostics
Destination : gold.mart_patient_features
Materialization : table

Objective:
    Builds the final feature table consumed by the machine learning pipeline
    (logistic regression in analysis/03_prediction_model.ipynb).

    Each feature is documented with its clinical and statistical rationale.
    Categorical features are retained for one-hot encoding in Python.

Grain       : 1 row = 1 patient stay
Target      : readmission_30j (0 / 1)

Feature engineering:
    Numerical  : 10 continuous or ordinal features
    Binary     : 7 boolean flags (cast to INT for sklearn)
    Categorical: 4 variables for downstream one-hot encoding
=============================================================================
*/

WITH patients AS (

    SELECT * FROM {{ ref('stg_patients') }}

),

sejours AS (

    SELECT * FROM {{ ref('stg_sejours') }}

),

diagnostics AS (

    SELECT * FROM {{ ref('stg_diagnostics') }}

),

joined AS (

    SELECT
        p.patient_id,

        -- NUMERICAL FEATURES
        p.age,
        p.nb_hospitalisations_precedentes,
        p.nb_medicaments,
        p.duree_sejour,
        s.duree_zscore_patho,
        s.indice_complexite,
        s.ratio_meds_duree,
        d.severite_normalisee,
        s.taux_readmission_patho,

        -- BINARY FEATURES (0/1 for sklearn)
        p.flag_polymedication::INT                          AS flag_polymedication,
        p.flag_sortie_complexe::INT                         AS flag_sortie_complexe,
        s.flag_deja_hospitalise::INT                        AS flag_deja_hospitalise,
        s.flag_antecedents_lourds::INT                      AS flag_antecedents_lourds,
        s.flag_long_sejour::INT                             AS flag_long_sejour,
        d.flag_pathologie_chronique::INT                    AS flag_pathologie_chronique,
        d.flag_pathologie_critique::INT                     AS flag_pathologie_critique,

        -- CATEGORICAL FEATURES (one-hot encoding in Python)
        p.tranche_age,
        p.mode_sortie,
        p.service,
        d.groupe_clinique,

        -- TEMPORAL FEATURES
        p.annee_admission,
        p.mois_admission,

        CASE
            WHEN p.mois_admission IN (12, 1, 2)  THEN 'Hiver'
            WHEN p.mois_admission IN (3, 4, 5)   THEN 'Printemps'
            WHEN p.mois_admission IN (6, 7, 8)   THEN 'Ete'
            WHEN p.mois_admission IN (9, 10, 11) THEN 'Automne'
        END                                                 AS saison_admission,

        -- TARGET VARIABLE
        p.readmission_30j,

        -- METADATA (not used as features)
        p.pathologie,
        p.diagnostic_principal,
        p.hopital_region,
        p.date_admission,
        p.date_sortie,
        d.risque_readmission_patho,
        p.ingestion_timestamp

    FROM patients p
    INNER JOIN sejours    s ON p.patient_id = s.patient_id
    INNER JOIN diagnostics d ON p.patient_id = d.patient_id

)

SELECT * FROM joined
