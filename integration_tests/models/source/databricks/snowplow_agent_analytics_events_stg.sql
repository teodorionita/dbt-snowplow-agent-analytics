select
  {{ dbt_utils.star(from=ref('snowplow_agent_analytics_events'), except=['contexts_nl_basjes_yauaa_context_1', 'contexts_com_snowplowanalytics_snowplow_agent_classification_1', 'contexts_com_snowplowanalytics_snowplow_bot_detection_1', 'contexts_com_snowplowanalytics_snowplow_utm_referrer_1'], relation_alias='ev') }},
  transform(from_json(ev.contexts_nl_basjes_yauaa_context_1, 'array<struct<agentName: string>>'), x -> named_struct('agent_name', x.agentName)) as contexts_nl_basjes_yauaa_context_1,
  transform(from_json(ev.contexts_com_snowplowanalytics_snowplow_agent_classification_1, 'array<struct<operator: string, purpose: string>>'), x -> named_struct('operator', x.operator, 'purpose', x.purpose)) as contexts_com_snowplowanalytics_snowplow_agent_classification_1,
  transform(from_json(ev.contexts_com_snowplowanalytics_snowplow_bot_detection_1, 'array<struct<bot: boolean>>'), x -> named_struct('bot', x.bot)) as contexts_com_snowplowanalytics_snowplow_bot_detection_1,
  transform(from_json(ev.contexts_com_snowplowanalytics_snowplow_utm_referrer_1, 'array<struct<source: string, medium: string>>'), x -> named_struct('source', x.source, 'medium', x.medium)) as contexts_com_snowplowanalytics_snowplow_utm_referrer_1
from {{ ref('snowplow_agent_analytics_events') }} ev
