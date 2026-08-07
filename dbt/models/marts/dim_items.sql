with items as (
    select distinct item_id from {{ ref('stg_item_properties') }}

    union

    select distinct item_id from {{ ref('stg_events') }}
),

properties_pivoted as (
    select
        item_id,

        max(property_value) filter (
            where property_name = 'categoryid'
        )::bigint as category_id,

        (
            max(property_value) filter (
                where property_name = 'available'
            )
        )::integer = 1 as is_available,

        max(property_updated_at) filter (
            where property_name = 'categoryid'
        ) as category_updated_at,

        max(property_updated_at) filter (
            where property_name = 'available'
        ) as available_updated_at
    from {{ ref('int_properties_latest' )}}
    group by item_id
),

final as (
    select
        items.item_id,
        properties_pivoted.category_id,
        properties_pivoted.is_available,
        properties_pivoted.category_updated_at,
        properties_pivoted.available_updated_at
    from items
    left join properties_pivoted 
        on items.item_id = properties_pivoted.item_id
)

select * from final