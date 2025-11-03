with source as (
    
    select * from {{ source('soundcheck_raw', 'artist_ratings') }}

),

cleaned as (

    select
        -- Primary key
        artist_rating_id,

        -- Foreign Keys
        artist_id,
        user_id,
        
        -- Rating details
        rating_date,
        overall_rating,
        aspects,

        -- Parse JSON of aspects
        cast(JSON_EXTRACT_SCALAR(aspects, '$.live_performance') as float64) as live_performance,
        cast(JSON_EXTRACT_SCALAR(aspects, '$.stage_presence') as float64) as stage_presence,
        cast(JSON_EXTRACT_SCALAR(aspects, '$.sound_quality') as float64) as sound_quality,
        cast(JSON_EXTRACT_SCALAR(aspects, '$.fan_interaction') as float64) as fan_interaction,
        cast(JSON_EXTRACT_SCALAR(aspects, '$.setlist_variety') as float64) as setlist_variety,

        -- Metadata
        current_timestamp() as dbt_loaded_at

    from source
)

select * from cleaned