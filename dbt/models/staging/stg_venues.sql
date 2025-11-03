with source as (
    
    select * from {{ source('soundcheck_raw', 'venues') }}

),

cleaned as (

    select
        -- Primary key
        venue_id,
        
        -- Venue details
        venue_name,
        lower(trim(venue_name)) as venue_name_normalized,
        venue_type,
        lower(trim(venue_type)) as venue_type_normalized,
        capacity,
        standing_room_capacity,
        opened_year,
        venue_website,
        phone,
        validated_capacity,
        typical_ticket_fee,

        -- Location Details
        address,
        city,
        lower(trim(city)) as city_normalized,
        state,
        lower(trim(state)) as state_normalized,
        zip_code,
        latitude,
        longitude,

        -- Amenities
        parking_available,
        valet_parking,
        food_available,
        full_bar,
        accessible_ada,
        box_office,
        
        -- Metadata
        current_timestamp() as dbt_loaded_at

    from source
)

select * from cleaned