with source as (
    
    select * from {{ source('soundcheck_raw', 'venue_reviews') }}

),

cleaned as (

    select
        -- Primary key
        review_id,

        -- Foreign Keys
        venue_id,
        user_id,
        
        -- Review details
        review_date,
        overall_rating,
        review_text,
        aspects,

        -- Parse Date
        extract(year from review_date) as review_year,
        extract(month from review_date) as review_month,
        extract(day from review_date) as review_day,

        -- Parse JSON of aspects
        cast(JSON_EXTRACT_SCALAR(aspects, '$.location_convenience') as float64) as location_convenience,
        cast(JSON_EXTRACT_SCALAR(aspects, '$.sound_system') as float64) as sound_system,
        cast(JSON_EXTRACT_SCALAR(aspects, '$.sightlines') as float64) as sightlines,
        cast(JSON_EXTRACT_SCALAR(aspects, '$.cleanliness') as float64) as cleanliness,
        cast(JSON_EXTRACT_SCALAR(aspects, '$.staff_friendliness') as float64) as staff_friendliness,
        cast(JSON_EXTRACT_SCALAR(aspects, '$.food_quality') as float64) as food_quality,
        cast(JSON_EXTRACT_SCALAR(aspects, '$.drink_prices') as float64) as drink_prices,
        cast(JSON_EXTRACT_SCALAR(aspects, '$.parking') as float64) as parking,
        cast(JSON_EXTRACT_SCALAR(aspects, '$.bathroom_availability') as float64) as bathroom_availability,

        -- Metadata
        current_timestamp() as dbt_loaded_at

    from source
)

select * from cleaned