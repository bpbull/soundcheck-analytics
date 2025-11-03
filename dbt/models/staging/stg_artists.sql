with source as (
    select * from {{source('soundcheck_raw','artists')}}
),

cleaned as (

    select
        -- Primary Key
        artist_id,

        -- Artist Details
        artist_name,
        lower(trim(artist_name)) as artist_name_normalized,
        formed_year,
        origin_city,
        lower(trim(origin_city)) as origin_city_normalized,
        origin_state,
        upper(trim(origin_state)) as origin_state_normalized,
        origin_country,
        lower(trim(origin_country)) as origin_country_normalized,
        genre_primary,
        lower(trim(genre_primary)) as genre_primary_normalized,
        genre_secondary,
        lower(trim(genre_secondary)) as genre_secondary_normalized,
        popularity_tier,
        has_label,
        verified_artist,

        -- Social media metrics
        spotify_monthly_listeners,
        instagram_followers,

        -- Event metrics
        booking_price_min,
        booking_price_max,
        average_show_duration_minutes,
        tour_frequency,

        -- Metadata
        current_timestamp() as dbt_loaded_at

    from source
)

select * from cleaned