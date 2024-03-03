{% macro extract_hour_from_iso8601_string(string) -%}
  substring({{ string }}, 12, 2)
{%- endmacro %}
