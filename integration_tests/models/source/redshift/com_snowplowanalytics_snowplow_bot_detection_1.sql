select distinct event_id as root_id, collector_tstamp as root_tstamp,
  cast(nullif(json_extract_path_text(json_extract_array_element_text(contexts_com_snowplowanalytics_snowplow_bot_detection_1, 0, true), 'bot', true), '') as boolean) as bot
from {{ ref('snowplow_agent_analytics_events') }}
where contexts_com_snowplowanalytics_snowplow_bot_detection_1 is not null
