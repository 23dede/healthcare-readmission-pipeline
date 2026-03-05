/*
=============================================================================
stg_sejours.sql -- Silver Layer : Stay metrics and risk indicators
=============================================================================
Source      : silver.stg_patients
Destination : silver.stg_sejours
Materialization : view

Objective:
    Computes analytical metrics per stay to be used as features in the
    Gold layer. stg_patients handles identity/demographics, stg_sejours
    handles stay measurements and raw risk indicators.

Indicators computed:
  - Normalized polypharmacy intensity
  - Stay complexity index (composite score)
  - Flag for heavy prior history
  - Stay duration normalized (approximate z-score by pathology)
=============================================================================
*/

WITH patients AS (

    SELECT * FROM {{ ref('stg_patients') }}

),

stats_par_pathologie AS (

    SELECT
        pathologie,
        AVG(duree_sejour)             AS duree_moy_patho,
        STDDEV(duree_sejour)          AS duree_std_patho,
        AVG(nb_medicaments)           AS meds_moy_patho,
        COUNT(*)                      AS nb_sejours_patho,
        AVG(readmission_30j::FLOAT)   AS taux_readmission_patho
    FROM patients
    GROUP BY pathologie

),

enrichi AS (

    SELECT
        p.patient_id,
        p.pathologie,
        p.service,
        p.hopital_region,
        p.duree_sejour,
        p.nb_hospitalisations_precedentes,
        p.nb_medicaments,
        p.mode_sortie,
        p.readmission_30j,

        -- Z-score of stay duration within pathology group
        CASE
            WHEN s.duree_std_patho > 0
            THEN ROUND((p.duree_sejour - s.duree_moy_patho) / s.duree_std_patho, 4)
            ELSE 0
        END                                             AS duree_zscore_patho,

        ROUND(s.taux_readmission_patho::NUMERIC, 4)     AS taux_readmission_patho,

        -- Composite complexity index [0-10]
        -- Weights: stay duration 35%, medications 30%, prior hospitalizations 35%
        LEAST(10, ROUND((
            (LEAST(p.duree_sejour, 30)                      / 30.0) * 3.5 +
            (LEAST(p.nb_medicaments, 15)                    / 15.0) * 3.0 +
            (LEAST(p.nb_hospitalisations_precedentes, 5)    /  5.0) * 3.5
        )::NUMERIC, 2))                                 AS indice_complexite,

        CASE WHEN p.nb_hospitalisations_precedentes >= 3
             THEN TRUE ELSE FALSE
        END                                             AS flag_antecedents_lourds,

        CASE WHEN p.nb_hospitalisations_precedentes >= 1
             THEN TRUE ELSE FALSE
        END                                             AS flag_deja_hospitalise,

        CASE WHEN p.duree_sejour >= 14
             THEN TRUE ELSE FALSE
        END                                             AS flag_long_sejour,

        -- Therapeutic intensity ratio (medications per day of stay)
        CASE
            WHEN p.duree_sejour > 0
            THEN ROUND((p.nb_medicaments::NUMERIC / p.duree_sejour), 4)
            ELSE 0
        END                                             AS ratio_meds_duree,

        s.nb_sejours_patho,
        s.duree_moy_patho                               AS duree_moyenne_patho

    FROM patients p
    LEFT JOIN stats_par_pathologie s ON p.pathologie = s.pathologie

)

SELECT * FROM enrichi
