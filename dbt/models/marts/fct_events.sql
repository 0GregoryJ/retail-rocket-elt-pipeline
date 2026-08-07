select
    {{dbt_utils.generate_surrogate_key(['session_id', 'event_sequence_number'])}} as event_id,
    visitor_id,
    item_id,
    transaction_id,
    event_timestamp,
    date_trunc('day', event_timestamp) as event_date,
    event_type,
    event_sequence_number,
    seconds_since_previous_event
from {{ ref('int_events_sessionized') }}