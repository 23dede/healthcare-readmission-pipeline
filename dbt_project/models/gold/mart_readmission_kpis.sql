/*
=============================================================================
mart_readmission_kpis.sql -- Gold Layer : Pre-aggregated KPIs for Power BI
=============================================================================
Source      : gold.mart_patient_features
Destination : gold.mart_readmission_kpis
Materialization : table

Objective:
    Produces multi-dimensional analytical aggregates consumed by the Power BI
    dashboard. Each CTE corresponds to a distinct analytical view
    (pathology, service, region, age bracket, temporal dimension).

    These aggregates allow Power BI to avoid heavy DAX transformations --
    KPIs are pre-computed and reliable.

KPIs computed:
    - Readmission rate (global and by dimension)
    - Average length of stay
    - Average complexity index
    - Stay volume
    - Polypharmacy rate
    - Complex discharge rate
=============================================================================
*/

WITH features AS (

    SELECT * FROM {{ ref('mart_patient_features') }}

),

par_pathologie AS (

    SELECT
        'pathologie'                                    AS dimension,
        pathologie                                      AS valeur,
        groupe_clinique                                 AS sous_groupe,
        COUNT(*)                                        AS nb_sejours,
        SUM(readmission_30j)                            AS nb_readmissions,
        ROUND(AVG(readmission_30j::FLOAT)::NUMERIC, 4)  AS taux_readmission,
        ROUND(AVG(duree_sejour)::NUMERIC, 2)            AS duree_sejour_moy,
        ROUND(AVG(nb_medicaments)::NUMERIC, 2)          AS nb_medicaments_moy,
        ROUND(AVG(indice_complexite)::NUMERIC, 2)       AS indice_complexite_moy,
        ROUND(AVG(flag_polymedication::FLOAT)::NUMERIC, 4) AS taux_polymedication,
        ROUND(AVG(flag_sortie_complexe::FLOAT)::NUMERIC, 4) AS taux_sortie_complexe,
        ROUND(AVG(age)::NUMERIC, 1)                     AS age_moyen
    FROM features
    GROUP BY pathologie, groupe_clinique

),

par_service AS (

    SELECT
        'service'                                       AS dimension,
        service                                         AS valeur,
        NULL                                            AS sous_groupe,
        COUNT(*)                                        AS nb_sejours,
        SUM(readmission_30j)                            AS nb_readmissions,
        ROUND(AVG(readmission_30j::FLOAT)::NUMERIC, 4)  AS taux_readmission,
        ROUND(AVG(duree_sejour)::NUMERIC, 2)            AS duree_sejour_moy,
        ROUND(AVG(nb_medicaments)::NUMERIC, 2)          AS nb_medicaments_moy,
        ROUND(AVG(indice_complexite)::NUMERIC, 2)       AS indice_complexite_moy,
        ROUND(AVG(flag_polymedication::FLOAT)::NUMERIC, 4) AS taux_polymedication,
        ROUND(AVG(flag_sortie_complexe::FLOAT)::NUMERIC, 4) AS taux_sortie_complexe,
        ROUND(AVG(age)::NUMERIC, 1)                     AS age_moyen
    FROM features
    GROUP BY service

),

par_region AS (

    SELECT
        'region'                                        AS dimension,
        hopital_region                                  AS valeur,
        NULL                                            AS sous_groupe,
        COUNT(*)                                        AS nb_sejours,
        SUM(readmission_30j)                            AS nb_readmissions,
        ROUND(AVG(readmission_30j::FLOAT)::NUMERIC, 4)  AS taux_readmission,
        ROUND(AVG(duree_sejour)::NUMERIC, 2)            AS duree_sejour_moy,
        ROUND(AVG(nb_medicaments)::NUMERIC, 2)          AS nb_medicaments_moy,
        ROUND(AVG(indice_complexite)::NUMERIC, 2)       AS indice_complexite_moy,
        ROUND(AVG(flag_polymedication::FLOAT)::NUMERIC, 4) AS taux_polymedication,
        ROUND(AVG(flag_sortie_complexe::FLOAT)::NUMERIC, 4) AS taux_sortie_complexe,
        ROUND(AVG(age)::NUMERIC, 1)                     AS age_moyen
    FROM features
    GROUP BY hopital_region

),

par_tranche_age AS (

    SELECT
        'tranche_age'                                   AS dimension,
        tranche_age                                     AS valeur,
        NULL                                            AS sous_groupe,
        COUNT(*)                                        AS nb_sejours,
        SUM(readmission_30j)                            AS nb_readmissions,
        ROUND(AVG(readmission_30j::FLOAT)::NUMERIC, 4)  AS taux_readmission,
        ROUND(AVG(duree_sejour)::NUMERIC, 2)            AS duree_sejour_moy,
        ROUND(AVG(nb_medicaments)::NUMERIC, 2)          AS nb_medicaments_moy,
        ROUND(AVG(indice_complexite)::NUMERIC, 2)       AS indice_complexite_moy,
        ROUND(AVG(flag_polymedication::FLOAT)::NUMERIC, 4) AS taux_polymedication,
        ROUND(AVG(flag_sortie_complexe::FLOAT)::NUMERIC, 4) AS taux_sortie_complexe,
        ROUND(AVG(age)::NUMERIC, 1)                     AS age_moyen
    FROM features
    GROUP BY tranche_age

),

par_mois AS (

    SELECT
        'mois'                                          AS dimension,
        CONCAT(annee_admission, '-', LPAD(mois_admission::TEXT, 2, '0')) AS valeur,
        saison_admission                                AS sous_groupe,
        COUNT(*)                                        AS nb_sejours,
        SUM(readmission_30j)                            AS nb_readmissions,
        ROUND(AVG(readmission_30j::FLOAT)::NUMERIC, 4)  AS taux_readmission,
        ROUND(AVG(duree_sejour)::NUMERIC, 2)            AS duree_sejour_moy,
        ROUND(AVG(nb_medicaments)::NUMERIC, 2)          AS nb_medicaments_moy,
        ROUND(AVG(indice_complexite)::NUMERIC, 2)       AS indice_complexite_moy,
        ROUND(AVG(flag_polymedication::FLOAT)::NUMERIC, 4) AS taux_polymedication,
        ROUND(AVG(flag_sortie_complexe::FLOAT)::NUMERIC, 4) AS taux_sortie_complexe,
        ROUND(AVG(age)::NUMERIC, 1)                     AS age_moyen
    FROM features
    GROUP BY annee_admission, mois_admission, saison_admission

),

union_all AS (
    SELECT * FROM par_pathologie
    UNION ALL
    SELECT * FROM par_service
    UNION ALL
    SELECT * FROM par_region
    UNION ALL
    SELECT * FROM par_tranche_age
    UNION ALL
    SELECT * FROM par_mois
)

SELECT
    *,
    ROUND((
        taux_readmission - AVG(taux_readmission) OVER (PARTITION BY dimension)
    )::NUMERIC, 4)                                      AS ecart_vs_moyenne_dimension

FROM union_all
ORDER BY dimension, taux_readmission DESC
