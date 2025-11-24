with events as (
    select * from {{ ref('int_events_enriched') }}
),

event_ratings as (
    select * from {{ ref('stg_event_ratings') }}
),

ticket_sales as (
    select * from {{ ref('stg_ticket_sales') }}
),

-- Aggregate ratings by event
ratings_agg as (
    select
        event_id,
        count(*) as total_ratings,
        avg(rating_score) as avg_rating,
        min(rating_score) as min_rating,
        max(rating_score) as max_rating,
        sum(case when verified_attendance = true then 1 else 0 end) as verified_ratings_count
    from event_ratings
    group by event_id
),

-- Aggregate sales by event
sales_agg as (
    select
        event_id,
        count(*) as total_transactions,
        sum(quantity_sold) as tickets_sold,
        sum(total_amount) as total_revenue,
        avg(total_amount) as avg_transaction_amount
    from ticket_sales
    group by event_id
),

final as (
    select
        -- Event identifiers
        e.event_id,
        e.event_name,
        e.event_date,
        
        -- Artist and venue
        e.artist_id,
        e.artist_name,
        e.genre_primary,
        e.popularity_tier,
        e.venue_id,
        e.venue_name,
        e.venue_type,
        e.capacity,
        
        -- Tour context
        e.tour_name,
        
        -- Event attributes
        e.base_ticket_price,
        e.is_weekend,
        e.price_tier,
        
        -- Calculated metrics from intermediate
        e.capacity_utilization_pct,
        
        -- Rating metrics
        coalesce(r.total_ratings, 0) as total_ratings,
        round(r.avg_rating, 2) as avg_rating,
        round(r.min_rating, 2) as min_rating,
        round(r.max_rating, 2) as max_rating,
        coalesce(r.verified_ratings_count, 0) as verified_ratings_count,
        
        -- Sales metrics
        coalesce(s.total_transactions, 0) as total_transactions,
        coalesce(s.tickets_sold, 0) as tickets_sold,
        round(coalesce(s.total_revenue, 0), 2) as total_revenue,
        round(s.avg_transaction_amount, 2) as avg_transaction_amount,
        
        -- Derived metrics
        case
            when s.tickets_sold >= e.capacity * 0.95 then true
            else false
        end as is_sold_out,
        
        case
            when r.total_ratings > 0 then round(r.total_ratings / s.tickets_sold * 100, 2)
            else null
        end as rating_participation_pct,
        
        -- Metadata
        current_timestamp() as dbt_updated_at
        
    from events e
    left join ratings_agg r on e.event_id = r.event_id
    left join sales_agg s on e.event_id = s.event_id
)

select * from final