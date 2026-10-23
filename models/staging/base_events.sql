{# Upsert by event_id so manifest replays replace prior copies. BigQuery and
   Databricks use merge; Snowflake uses delete+insert. The
   snowplow_optimize meta bounds the target scan to the batch's load_tstamp range. #}
{# full_refresh guard: an accidental --full-refresh would truncate this table while
   the incremental manifest survives, so history would never be reprocessed. Refresh
   requires snowplow__allow_refresh (or the dev target), same as the manifest. #}
{{
  config(
    materialized='incremental',
    incremental_strategy=('merge' if target.type in ['bigquery', 'databricks'] else 'delete+insert'),
    unique_key='event_id',
    meta={'upsert_date_key': 'load_tstamp', 'snowplow_optimize': true},
    on_schema_change='append_new_columns',
    full_refresh=snowplow_agent_analytics.allow_refresh(),
    tags=['snowplow_agent_analytics_incremental']
  )
}}

select * from {{ ref('base_events_this_run') }}
