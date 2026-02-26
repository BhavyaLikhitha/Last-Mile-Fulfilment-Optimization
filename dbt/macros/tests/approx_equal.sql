{% test approx_equal(model, left_expr, right_expr, tolerance=0.01) %}
select *
from {{ model }}
where abs( ({{ left_expr }}) - ({{ right_expr }}) ) > {{ tolerance }}
{% endtest %}