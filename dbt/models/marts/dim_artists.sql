with artists as (
    select * from {{ ref('stg_artists') }}
),

events as (
    select * from {{ ref('fct_events') }}
),

artist_metrics as (
    select
        artist_id,
        count(distinct event_id) as total_events,
        sum(tickets_sold) as total_tickets_sold,
        sum(total_revenue) as total_revenue,
        avg(avg_rating) as avg_artist_rating,
        sum(total_ratings) as total_ratings_received,
        avg(capacity_utilization_pct) as avg_capacity_utilization,
        sum(case when is_sold_out = true then 1 else 0 end) as sold_out_events_count
    from events
    group by artist_id
),

final as (
    select
        -- Artist identifiers
        a.artist_id,
        a.artist_name,
        
        -- Artist attributes
        a.genre_primary,
        a.genre_secondary,
        a.popularity_tier,
        a.formed_year,
        a.origin_country,
        
        -- Performance metrics
        coalesce(m.total_events, 0) as total_events,
        coalesce(m.total_tickets_sold, 0) as total_tickets_sold,
        round(coalesce(m.total_revenue, 0), 2) as total_revenue,
        round(m.avg_artist_rating, 2) as avg_artist_rating,
        coalesce(m.total_ratings_received, 0) as total_ratings_received,
        round(m.avg_capacity_utilization, 2) as avg_capacity_utilization,
        coalesce(m.sold_out_events_count, 0) as sold_out_events_count,
        
        -- Derived metrics
        case
            when m.total_events > 0 then round(m.total_revenue / m.total_events, 2)
            else null
        end as avg_revenue_per_event,
        
        case
            when m.total_events > 0 
            then round(m.sold_out_events_count / m.total_events * 100, 2)
            else null
        end as sold_out_rate_pct,
        
        -- Metadata
        current_timestamp() as dbt_updated_at
        
    from artists a
    left join artist_metrics m on a.artist_id = m.artist_id
)

select * from final