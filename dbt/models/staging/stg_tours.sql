with source as (
    
    select * from {{ source('soundcheck_raw', 'tours') }}

),

cleaned as (

    select
        -- Primary key
        tour_id,
        
        -- Foreign keys
        artist_id,
       
        -- Tour details
        tour_name,
        lower(trim(tour_name)) as tour_name_normalized,
        
        -- Tour Details
        start_date,
        end_date,
        number_of_shows,
        tour_type,
        tour_legs,
        production_level,
       
        -- Extract date parts for easier analysis
        extract(year from start_date) as start_year,
        extract(month from start_date) as start_month,
        extract(day from start_date) as start_day,
        extract(year from end_date) as end_year,
        extract(month from end_date) as end_month,
        extract(day from end_date) as end_day,
        
        -- Metadata
        current_timestamp() as dbt_loaded_at

    from source
)

select * from cleaned