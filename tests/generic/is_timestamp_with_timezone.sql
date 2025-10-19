{% test is_timestamp_with_timezone(model, column_name) %}
select
  {{ column_name }}
from
  {{ model }}
where
  {{ column_name }} is not null
  and {{ column_name }} not rlike '^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\+\\d{2}:\\d{2}$'
{% endtest %}