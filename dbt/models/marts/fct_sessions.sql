with sessions as (
    select
        session_id,
        visitor_id,

        min(event_timestamp) as session_started_at,
        max(event_timestamp) as session_ended_at,
        min(event_timestamp)::date as session_date,

        extract(
            epoch from (
                max(event_timestamp) - min(event_timestamp)
            )
        )::integer as session_duration_seconds,

        count(*) as event_count,

        count(*) filter (
            where event_type = 'view'
        ) as view_count,

        count(*) filter (
            where event_type = 'addtocart'
        ) as add_to_cart_count,

        count(*) filter (
            where event_type = 'transaction'
        ) as transaction_count,

        count(distinct item_id) as distinct_item_count,

        bool_or(event_type = 'view') as has_view,
        bool_or(event_type = 'addtocart') as has_add_to_cart,
        bool_or(event_type = 'transaction') as has_transaction,

        count(*) = 1 as is_single_event_session,

        min(event_timestamp) filter (
            where event_type = 'view'
        ) as first_view_at,

        min(event_timestamp) filter (
            where event_type = 'addtocart'
        ) as first_add_to_cart_at,

        min(event_timestamp) filter (
            where event_type = 'transaction'
        ) as first_transaction_at

    from {{ ref('int_events_sessionized') }}
    group by
        session_id,
        visitor_id
),

event_types as (
    select
        session_id,

        max(event_type) filter (
            where event_sequence_number = 1
        ) as first_event_type,

        max(event_type) filter (
            where event_sequence_number = session_event_count
        ) as last_event_type
    from (
        select
            *,
            count(*) over (
                partition by session_id
            ) as session_event_count

        from {{ ref('int_events_sessionized') }}
    ) as events
    group by session_id
),

final as (
    select
        sessions.session_id,
        sessions.visitor_id,

        sessions.session_started_at,
        sessions.session_ended_at,
        sessions.session_date,
        sessions.session_duration_seconds,

        sessions.event_count,
        sessions.view_count,
        sessions.add_to_cart_count,
        sessions.transaction_count,
        sessions.distinct_item_count,

        sessions.has_view,
        sessions.has_add_to_cart,
        sessions.has_transaction,
        sessions.is_single_event_session,

        event_types.first_event_type,
        event_types.last_event_type,

        extract(
            epoch from (
                sessions.first_view_at
                - sessions.session_started_at
            )
        )::integer as seconds_to_first_view,

        extract(
            epoch from (
                sessions.first_add_to_cart_at
                - sessions.session_started_at
            )
        )::integer as seconds_to_first_add_to_cart,

        extract(
            epoch from (
                sessions.first_transaction_at
                - sessions.session_started_at
            )
        )::integer as seconds_to_first_transaction
    from sessions
    left join event_types
        on sessions.session_id = event_types.session_id
)

select *
from final