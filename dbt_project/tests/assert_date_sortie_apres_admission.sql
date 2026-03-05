-- tests/assert_date_sortie_apres_admission.sql
-- Singular test: verifies temporal coherence of stays
-- date_sortie must be >= date_admission (positive stay duration)

SELECT
    patient_id,
    date_admission,
    date_sortie,
    (date_sortie - date_admission) AS duree_calculee
FROM {{ ref('stg_patients') }}
WHERE date_sortie < date_admission
