{# Shared date-formatting snippets.
   Change the format in one place and every mart follows. #}

{% macro my_date(col) -%}
  strftime({{ col }}, '%Y-%m-%d')
{%- endmacro %}

{% macro my_month(col) -%}
  strftime({{ col }}, '%Y-%m')
{%- endmacro %}
