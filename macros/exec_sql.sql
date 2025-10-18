{% macro exec_sql(sql) %}
  {% set res = run_query(sql) %}
  {% if res %}
    {% for row in res.columns[0].values() %}
      {{ log(row, info=True) }}
    {% endfor %}
  {% endif %}
{% endmacro %}