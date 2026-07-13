{#
    Generic data test: fails for rows where the column falls outside
    [min_value, max_value]. Nulls are ignored (use not_null separately).
    A lightweight local stand-in for dbt_utils.accepted_range.
#}
{% test accepted_range(model, column_name, min_value, max_value) %}

select *
from {{ model }}
where {{ column_name }} is not null
  and (
        {{ column_name }} < {{ min_value }}
     or {{ column_name }} > {{ max_value }}
  )

{% endtest %}
