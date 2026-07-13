{#
    Builds a deterministic surrogate key by hashing the given columns.
    Nulls are coerced to a sentinel so a null never collides with an empty string.
    Kept local (instead of pulling in dbt_utils) so the project runs offline.
#}
{% macro surrogate_key(columns) %}
    md5(
        {%- for col in columns %}
        coalesce(cast({{ col }} as varchar), '_null_')
        {%- if not loop.last %} || '|' || {% endif -%}
        {% endfor %}
    )
{% endmacro %}
