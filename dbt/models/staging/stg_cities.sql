with source as (
    select * from {{source('soundcheck_raw','cities')}}
),

cleaned as (

    select
        -- Primary Key
        city_id,

        -- City Details
        city,
        lower(trim(city)) as city_normalized,
        state,
        upper(trim(state)) as state_normalized,
        population,
        timezone,

        -- City Music Scene
        music_scene_score,
        primary_genres,
        lower(trim(primary_genres)) as primary_genres_normalized,
        avg_ticket_price,
        total_venues,

        -- Metadata
        current_timestamp() as dbt_loaded_at

    from source
)

select * from cleaned