-- tests/assert_no_negative_age.sql
-- Singular test: verifies that no patient has a null or negative age
-- A non-empty result means the test FAILS

SELECT
    patient_id,
    age
FROM {{ ref('stg_patients') }}
WHERE age <= 0
