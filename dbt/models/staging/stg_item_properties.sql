select
    to_timestamp(timestamp / 1000.0) as snapshot_timestamp,
    itemid as item_id,
    property as property_name,
    value as property_value
from {{ source('raw', 'item_properties') }}
