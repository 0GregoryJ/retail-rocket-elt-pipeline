{{ config(materialized= 'table') }}

with relevant_properties as (
    select
        item_id,
        property_name,
        property_value,
        snapshot_timestamp
    from {{ ref('stg_item_properties') }}
    where property_name in (
        'categoryid',
        'available'
    )
),

ranked_properties as (
    select
        item_id,
        property_name,
        property_value,
        snapshot_timestamp,

        row_number() over (
            partition by
                item_id,
                property_name
            order by
                snapshot_timestamp desc,
                property_value desc
        ) as property_recency_rank
    from relevant_properties
)

select
    item_id,
    property_name,
    property_value,
    snapshot_timestamp as property_updated_at
from ranked_properties
where property_recency_rank = 1