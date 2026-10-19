{% macro check_warehouse() %}
  {% if target.type != 'snowflake' %}
    {{ exceptions.raise_compiler_error(
      "Snowplow Error: snowplow_agent_analytics currently supports Snowflake only. "
      ~ "The configured adapter is '" ~ target.type ~ "'. Use a Snowflake target."
    ) }}
  {% endif %}
{% endmacro %}
