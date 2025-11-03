with source as (
    
    select * from {{ source('soundcheck_raw', 'events') }}

),

cleaned as (

    select
        -- Primary key
        event_id,
        
        -- Foreign keys
        artist_id,
        venue_id,
        tour_id,
        
        -- Event details
        event_name,
        lower(trim(event_name)) as event_name_normalized,
        
        -- Dates and times
        event_date,
        event_day_of_week,
        doors_time,
        show_time,
        announced_date,
        on_sale_date,
        
        -- Pricing
        base_ticket_price,
        vip_ticket_price,
        
        -- Ticket info
        ticket_vendor,
        age_restriction,
        
        -- Status and details
        event_status,
        cancellation_reason,
        estimated_attendance,
        weather_condition,
        special_event,
        
        -- Opening acts (JSON field)
        opening_acts,
        
        -- Extract date parts for easier analysis
        extract(year from event_date) as event_year,
        extract(month from event_date) as event_month,
        extract(day from event_date) as event_day,
        format_date('%A', event_date) as event_day_name,
        
        -- Weekend flag
        case 
            when extract(dayofweek from event_date) in (1, 7) then true
            else false
        end as is_weekend,
        
        -- Metadata
        current_timestamp() as dbt_loaded_at

    from source
)

select * from cleaned