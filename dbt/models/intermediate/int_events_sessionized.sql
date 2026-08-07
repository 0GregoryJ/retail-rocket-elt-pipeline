with events as (
    select *
    from {{ ref('stg_events') }}
),

with_previous_event as (
    select
        *,
        lag(event_timestamp) over (
                partition by visitor_id
                order by
                    event_timestamp,
                    item_id,
                    event_type,
                    coalesce(transaction_id, -1)
            ) as previous_event_timestamp
    from events
),

with_session_boundaries as (
    select
        *,
        case
            when previous_event_timestamp is null then 1
            when (event_timestamp - previous_event_timestamp) > interval '30 minutes' then 1
            else 0
        end as is_new_session
    from with_previous_event
),

with_session_numbers as (
    select
        *,
        sum(is_new_session) over (
            partition by visitor_id
            order by
                event_timestamp,
                item_id,
                event_type,
                coalesce(transaction_id, -1)
            rows unbounded preceding
        ) as visitor_session_number
    from with_session_boundaries
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'visitor_id',
            'visitor_session_number'
        ]) }} as session_id,
        visitor_id,
        item_id,
        event_timestamp,
        event_type,
        transaction_id,
        row_number() over (
            partition by
                visitor_id,
                visitor_session_number
            order by
                event_timestamp,
                item_id,
                event_type,
                coalesce(transaction_id, -1)
        ) as event_sequence_number,
        case
            when is_new_session = 1 then null
            else extract(
                epoch from (
                    event_timestamp - previous_event_timestamp
                )
            )::integer
        end as seconds_since_previous_event
    from with_session_numbers
)

select *
from final