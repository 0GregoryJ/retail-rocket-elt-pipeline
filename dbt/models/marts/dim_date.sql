with date_spine as (
    {{ dbt.date_spine(
        datepart="day",
        start_date="cast('2015-01-01' as date)",
        end_date="cast('2016-01-01' as date)"
    ) }}
),

date_day_as_date as (
    select date_day as date from date_spine
),

final as (
    select
        to_char(date, 'YYYYMMDD')::integer as date_id,
        date,

        extract(isodow from date)::integer as day_of_week_number,
        trim(to_char(date, 'Day')) as day_name,
        extract(day from date)::integer as day_of_month,
        extract(doy from date)::integer as day_of_year,

        extract(isodow from date) in (6, 7) as is_weekend,

        date_trunc('week', date)::date as week_start_date,
        extract(week from date)::integer as week_of_year,

        extract(month from date)::integer as month_number,
        trim(to_char(date, 'Month')) as month_name,
        date_trunc('month', date)::date as month_start_date,

        extract(quarter from date)::integer as quarter_number,
        'Q' || extract(quarter from date)::integer as quarter_name,
        date_trunc('quarter', date)::date as quarter_start_date,

        extract(year from date)::integer as year,
        date_trunc('year', date)::date as year_start_date
    from date_day_as_date
)

select *
from final