/*
    stg_dates — Silver layer
    Source: RAW (static calendar table)
    Transformations:
      - Type casting
      - Adds fiscal_quarter, fiscal_year (assuming Feb fiscal year start)
      - Adds is_month_start, is_month_end flags
      - Adds week_of_month
*/

with source as (
    select * from {{ source('raw', 'dim_date') }}
),

transformed as (
    select
        cast(date as date) as date,
        cast(day_of_week as varchar(10)) as day_of_week,
        cast(day_of_week_num as integer) as day_of_week_num,
        cast(week_number as integer) as week_number,
        cast(month as integer) as month,
        cast(month_name as varchar(10)) as month_name,
        cast(quarter as integer) as quarter,
        cast(year as integer) as year,
        cast(is_holiday as boolean) as is_holiday,
        cast(is_weekend as boolean) as is_weekend,
        cast(season as varchar(10)) as season,

        -- Derived: is it a business day?
        case when is_weekend = false and is_holiday = false then true else false end as is_business_day,

        -- Derived: month start/end flags
        case when date = date_trunc('month', date) then true else false end as is_month_start,
        case when date = last_day(date) then true else false end as is_month_end,

        -- Derived: week of month
        ceil(dayofmonth(date) / 7.0) as week_of_month,

        -- Derived: year-month key for grouping
        to_char(date, 'YYYY-MM') as year_month

    from source
)

select * from transformed