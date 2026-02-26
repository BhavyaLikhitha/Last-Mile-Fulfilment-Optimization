{% test values_in_set(model, column_name, allowed_values) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} not in (
    {% for v in allowed_values %}
      '{{ v }}'{% if not loop.last %},{% endif %}
    {% endfor %}
  )
{% endtest %}