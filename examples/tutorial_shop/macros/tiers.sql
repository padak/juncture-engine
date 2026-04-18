{# Business rule: what counts as VIP.
   Reads vip_threshold_eur from juncture.yaml vars: (overridable via --var).
   Every mart that calls is_vip() flips together when the threshold changes. #}

{% macro is_vip(amount_col) -%}
  ({{ amount_col }} >= {{ var('vip_threshold_eur', 500) }})
{%- endmacro %}
