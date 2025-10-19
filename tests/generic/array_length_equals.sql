{% test array_length_equals(model, column_name, length) %}
{% set col = column_name %}
{% set len_expr = "size(" ~ col ~ ")" %}
select *
from {{ model }}
where {{ col }} is null
   or {{ len_expr }} != {{ length }}
{% endtest %}
