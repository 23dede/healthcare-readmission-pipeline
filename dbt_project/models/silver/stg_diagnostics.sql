/*
=============================================================================
stg_diagnostics.sql -- Silver Layer : ICD-10 reference and diagnostic enrichment
=============================================================================
Source      : silver.stg_patients
Destination : silver.stg_diagnostics
Materialization : view

Objective:
    Standardizes and enriches simplified ICD-10 diagnostic codes.
    Creates clinical groupings usable as categorical features in the Gold
    layer (one-hot or ordinal encoding).

    Clinical groups (aligned with French hospital practice):
      CARDIO    : Cardiovascular pathologies
      PULMO     : Pulmonary / respiratory pathologies
      METABOL   : Metabolism / diabetes
      NEPHRO    : Renal pathologies
      NEURO     : Neurological pathologies
      INFECTIO  : Systemic infections
      TRAUMA    : Trauma / orthopedics
=============================================================================
*/

WITH patients AS (

    SELECT * FROM {{ ref('stg_patients') }}

),

mapping_cim10 (
    code_cim10,
    libelle_complet,
    groupe_clinique,
    severite_relative,
    risque_readmission
) AS (
    VALUES
        ('I50', 'Insuffisance cardiaque',                 'CARDIO',   4, 'Eleve'),
        ('E11', 'Diabete de type 2',                      'METABOL',  3, 'Modere'),
        ('J44', 'BPCO',                                   'PULMO',    3, 'Modere'),
        ('J18', 'Pneumonie',                              'PULMO',    3, 'Modere'),
        ('N18', 'Insuffisance renale chronique',          'NEPHRO',   4, 'Eleve'),
        ('I63', 'Accident vasculaire cerebral ischemique','NEURO',    5, 'Eleve'),
        ('A41', 'Sepsis',                                 'INFECTIO', 5, 'Eleve'),
        ('S72', 'Fracture du col du femur',               'TRAUMA',   3, 'Modere')
),

enrichi AS (

    SELECT
        p.patient_id,
        p.diagnostic_principal                              AS code_cim10,
        p.pathologie,

        COALESCE(m.libelle_complet,  'Autre')               AS libelle_diagnostic,
        COALESCE(m.groupe_clinique,  'AUTRE')               AS groupe_clinique,
        COALESCE(m.severite_relative, 2)                    AS severite_relative,
        COALESCE(m.risque_readmission, 'Faible')            AS risque_readmission_patho,

        ROUND(COALESCE(m.severite_relative, 2) / 5.0, 2)   AS severite_normalisee,

        CASE
            WHEN m.groupe_clinique IN ('CARDIO', 'NEPHRO', 'METABOL', 'PULMO')
            THEN TRUE ELSE FALSE
        END                                                 AS flag_pathologie_chronique,

        CASE
            WHEN COALESCE(m.severite_relative, 2) >= 4
            THEN TRUE ELSE FALSE
        END                                                 AS flag_pathologie_critique,

        p.readmission_30j

    FROM patients p
    LEFT JOIN mapping_cim10 m ON p.diagnostic_principal = m.code_cim10

)

SELECT * FROM enrichi
