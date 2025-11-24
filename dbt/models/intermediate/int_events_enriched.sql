with events as (
    select * from {{ ref('stg_events') }}
),

artists as (
    select * from {{ ref('stg_artists') }}
),

venues as (
    select * from {{ ref('stg_venues') }}
),

tours as (
    select * from {{ ref('stg_tours') }}
),

events_joined as (
    select
        -- Event identifiers
        e.event_id,
        e.event_name,
        e.event_date,
        e.event_status,
        
        -- Artist information
        e.artist_id,
        a.artist_name,
        a.genre_primary,
        a.genre_secondary,
        a.popularity_tier,
        
        -- Venue information
        e.venue_id,
        v.venue_name,
        v.venue_type,
        v.capacity,
        v.city_normalized,
        v.state_normalized,
        
        -- Tour information (may be null)
        e.tour_id,
        t.tour_name,
        t.tour_type,
        
        -- Event timing details
        e.event_year,
        e.event_month,
        e.event_day,
        e.event_day_of_week,
        e.is_weekend,
        e.doors_time,
        e.show_time,
        
        -- Ticket information
        e.base_ticket_price,
        e.vip_ticket_price,
        e.ticket_vendor,
        e.age_restriction,
        
        -- Event attributes
        e.estimated_attendance,
        e.weather_condition,
        e.special_event,
        e.cancellation_reason,
        
        
        -- Calculate capacity utilization (if we have attendance estimate)
        case 
            when e.estimated_attendance is not null and v.capacity is not null and v.capacity > 0
            then round((e.estimated_attendance / v.capacity) * 100, 2)
            else null
        end as capacity_utilization_pct,
        
        -- Season categorization
        case
            when e.event_month in (12, 1, 2) then 'winter'
            when e.event_month in (3, 4, 5) then 'spring'
            when e.event_month in (6, 7, 8) then 'summer'
            else 'fall'
        end as event_season,
        
        -- Price tier categorization
        case
            when e.base_ticket_price < 30 then 'budget'
            when e.base_ticket_price < 75 then 'standard'
            when e.base_ticket_price < 150 then 'premium'
            else 'luxury'
        end as price_tier,
        
        -- Metadata
        current_timestamp() as dbt_updated_at
        
    from events e
    left join artists a on e.artist_id = a.artist_id
    left join venues v on e.venue_id = v.venue_id
    left join tours t on e.tour_id = t.tour_id
)

select * from events_joined