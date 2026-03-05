-- tests/assert_no_duplicate_features.sql
-- Singular test: verifies that the same patient_id does not appear
-- more than once in the Gold feature table.
-- Duplicates would corrupt model training.

SELECT
    patient_id,
    COUNT(*) AS nb_occurrences
FROM {{ ref('mart_patient_features') }}
GROUP BY patient_id
HAVING COUNT(*) > 1
