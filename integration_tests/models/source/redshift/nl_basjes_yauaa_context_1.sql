select distinct event_id as root_id, collector_tstamp as root_tstamp,
  cast(nullif(json_extract_path_text(json_extract_array_element_text(contexts_nl_basjes_yauaa_context_1, 0, true), 'agentName', true), '') as varchar(65535)) as agent_name
from {{ ref('snowplow_agent_analytics_events') }}
where contexts_nl_basjes_yauaa_context_1 is not null
