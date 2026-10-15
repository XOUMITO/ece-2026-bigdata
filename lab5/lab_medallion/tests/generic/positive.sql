{% test positive(model, column_name) %}
-- Fails for every row where the column is zero or negative
select *
from {{ model }}
where {{ column_name }} <= 0
{% endtest %}
