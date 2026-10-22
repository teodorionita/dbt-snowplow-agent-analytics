select
  {{ dbt_utils.star(from=ref('snowplow_agent_analytics_events'), except=['contexts_nl_basjes_yauaa_context_1', 'contexts_com_snowplowanalytics_snowplow_agent_classification_1', 'contexts_com_snowplowanalytics_snowplow_bot_detection_1', 'contexts_com_snowplowanalytics_snowplow_utm_referrer_1'], relation_alias='ev') }}
from {{ ref('snowplow_agent_analytics_events') }} ev
