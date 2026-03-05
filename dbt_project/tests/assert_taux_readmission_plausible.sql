-- tests/assert_taux_readmission_plausible.sql
-- Singular test: verifies that the overall readmission rate
-- stays within a medically plausible range (5% to 40%).
-- An out-of-range rate signals a data generation problem.

WITH stats AS (
    SELECT
        AVG(readmission_30j::FLOAT) AS taux_global
    FROM {{ ref('mart_patient_features') }}
)
SELECT taux_global
FROM stats
WHERE taux_global < 0.05
   OR taux_global > 0.40
