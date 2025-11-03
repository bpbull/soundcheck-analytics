with source as (
    
    select * from {{ source('soundcheck_raw', 'users') }}

),

cleaned as (

    select
        -- Primary key
        user_id,

        -- Details
        username,
        email,
        user_type,
        user_segment,
        join_date,
        home_city,
        home_state,
        age_group,
        preferred_genres,
        profile_completeness,
        email_verified,
        push_notifications_enabled,
        last_active_date,

        -- Metadata
        current_timestamp() as dbt_loaded_at

    from source)

select * from cleaned