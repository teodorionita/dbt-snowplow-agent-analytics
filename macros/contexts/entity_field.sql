{# Snowplow loaders use VARIANT (Snowflake), versioned repeated RECORDs (BigQuery),
   typed arrays (Databricks), or separate entity tables (Redshift). #}
{% macro entity_field(table_alias, entity, field, snowflake_field, dtype) -%}
  {{ return(adapter.dispatch('entity_field', 'snowplow_agent_analytics')(table_alias, entity, field, snowflake_field, dtype)) }}
{%- endmacro %}

{% macro snowflake__entity_field(table_alias, entity, field, snowflake_field, dtype) -%}
  {{ table_alias }}.contexts_{{ entity }}[0]:{{ snowflake_field }}::{{ dbt.type_string() if dtype == 'string' else 'boolean' }}
{%- endmacro %}

{% macro bigquery__entity_field(table_alias, entity, field, snowflake_field, dtype) -%}
  {%- set expressions = snowplow_utils.combine_column_versions(
    relation=source('snowplow', 'events'), column_prefix='contexts_' ~ entity,
    required_fields=[field], relation_alias=table_alias, include_field_alias=false) -%}
  {{ expressions[0] if expressions else 'cast(null as ' ~ (dbt.type_string() if dtype == 'string' else 'boolean') ~ ')' }}
{%- endmacro %}

{% macro databricks__entity_field(table_alias, entity, field, snowflake_field, dtype) -%}
  cast(get({{ table_alias }}.contexts_{{ entity }}, 0).{{ field }} as {{ dbt.type_string() if dtype == 'string' else 'boolean' }})
{%- endmacro %}

{% macro redshift__entity_field(table_alias, entity, field, snowflake_field, dtype) -%}
  {{ table_alias }}.{{ entity }}_{{ field }}
{%- endmacro %}
