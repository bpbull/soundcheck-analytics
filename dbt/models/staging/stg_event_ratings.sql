with source as (
    
    select * from {{ source('soundcheck_raw', 'event_ratings') }}

),

cleaned as (

    select
        -- Primary key
        rating_id,

        -- Foreign Keys
        event_id,
        user_id,
        
        -- Rating details
        rating_score,
        rating_date,
        days_after_event,

        -- Review Content
        review_title,
        review_text,
        verified_attendance,
        helpful_count,
        reported,
        aspects,

        -- Parse aspect ratings from JSON
        cast(json_extract_scalar(aspects, '$.sound_quality') as float64) as sound_quality,
        cast(json_extract_scalar(aspects, '$.venue_experience') as float64) as venue_experience,
        cast(json_extract_scalar(aspects, '$.performance_energy') as float64) as performance_energy,
        cast(json_extract_scalar(aspects, '$.setlist_satisfaction') as float64) as setlist_satisfaction,
        cast(json_extract_scalar(aspects, '$.crowd_vibe') as float64) as crowd_vibe,
        cast(json_extract_scalar(aspects, '$.value_for_money') as float64) as value_for_money,
        

        -- Extract date parts for easier analysis
        extract(year from rating_date) as rating_year,
        extract(month from rating_date) as rating_month,
        extract(day from rating_date) as rating_day,

        -- Flag
        case 
            when review_text is not null then true
            else false
        end as has_written_review,
        

        -- Metadata
        current_timestamp() as dbt_loaded_at

    from source
)

select * from cleaned