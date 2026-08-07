select
    to_timestamp(timestamp / 1000.0) as event_timestamp,
    visitorid as visitor_id,
    event as event_type,
    itemid as item_id,
    transactionid as transaction_id
from {{ source('raw', 'events') }}