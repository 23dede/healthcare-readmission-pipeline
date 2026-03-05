-- macros/safe_ratio.sql
-- Utility macro: safe division with zero-division protection
-- Usage: {{ safe_ratio('numerateur', 'denominateur', 0) }}

{% macro safe_ratio(numerator, denominator, default=0) %}
    CASE
        WHEN {{ denominator }} = 0 OR {{ denominator }} IS NULL
        THEN {{ default }}
        ELSE {{ numerator }}::FLOAT / {{ denominator }}
    END
{% endmacro %}
