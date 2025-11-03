with source as (
    select * from {{source('soundcheck_raw', 'user_artist_follows')}}
),

cleaned as (
    
    select

        -- Primary Key
        follow_id,

        -- Foreign Key
        user_id,
        artist_id,

        -- Details
        follow_date,
        notifications_enabled,

        -- Datepart extraction
        extract(year from follow_date) as follow_year,
        extract(month from follow_date) as follow_month,
        extract(day from follow_date) as follow_day,

        -- Metadata
        current_timestamp() as dbt_loaded_at

    from source
)

select * from cleaned