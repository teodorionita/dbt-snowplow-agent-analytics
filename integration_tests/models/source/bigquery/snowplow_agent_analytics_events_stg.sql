select
  {{ dbt_utils.star(from=ref('snowplow_agent_analytics_events'), except=['contexts_nl_basjes_yauaa_context_1', 'contexts_com_snowplowanalytics_snowplow_agent_classification_1', 'contexts_com_snowplowanalytics_snowplow_bot_detection_1', 'contexts_com_snowplowanalytics_snowplow_utm_referrer_1'], relation_alias='ev') }},
  array(select as struct json_value(item, '$.agentName') as agent_name from unnest(json_query_array(ev.contexts_nl_basjes_yauaa_context_1)) item) as contexts_nl_basjes_yauaa_context_1_0_0,
  array(select as struct json_value(item, '$.operator') as operator, json_value(item, '$.purpose') as purpose from unnest(json_query_array(ev.contexts_com_snowplowanalytics_snowplow_agent_classification_1)) item) as contexts_com_snowplowanalytics_snowplow_agent_classification_1_0_0,
  array(select as struct cast(json_value(item, '$.bot') as boolean) as bot from unnest(json_query_array(ev.contexts_com_snowplowanalytics_snowplow_bot_detection_1)) item) as contexts_com_snowplowanalytics_snowplow_bot_detection_1_0_0,
  array(select as struct json_value(item, '$.source') as source, json_value(item, '$.medium') as medium from unnest(json_query_array(ev.contexts_com_snowplowanalytics_snowplow_utm_referrer_1)) item) as contexts_com_snowplowanalytics_snowplow_utm_referrer_1_0_0
from {{ ref('snowplow_agent_analytics_events') }} ev
