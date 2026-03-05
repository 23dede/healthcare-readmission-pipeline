-- macros/classify_risk.sql
-- Utility macro: classifies a numerical risk score [0-1]
-- into a text category usable in Power BI and reports.
-- Usage: {{ classify_risk('score_risque_theorique') }}

{% macro classify_risk(score_column) %}
    CASE
        WHEN {{ score_column }} >= 0.70 THEN 'Eleve'
        WHEN {{ score_column }} >= 0.40 THEN 'Modere'
        WHEN {{ score_column }} >= 0.20 THEN 'Faible'
        ELSE 'Tres faible'
    END
{% endmacro %}
