select distinct event_id as root_id, collector_tstamp as root_tstamp,
  cast(nullif(json_extract_path_text(json_extract_array_element_text(contexts_com_snowplowanalytics_snowplow_utm_referrer_1, 0, true), 'source', true), '') as varchar(65535)) as source,
  cast(nullif(json_extract_path_text(json_extract_array_element_text(contexts_com_snowplowanalytics_snowplow_utm_referrer_1, 0, true), 'medium', true), '') as varchar(65535)) as medium
from {{ ref('snowplow_agent_analytics_events') }}
where contexts_com_snowplowanalytics_snowplow_utm_referrer_1 is not null
