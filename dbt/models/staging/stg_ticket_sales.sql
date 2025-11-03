with source as (
    
    select * from {{ source('soundcheck_raw', 'ticket_sales') }}

),

cleaned as (

    select
        -- Primary key
        sale_id,

        -- Foreign Keys
        event_id,
        
        -- Sale details
        sale_date,
        days_before_event,
        quantity_sold,
        ticket_type,
        unit_price,
        fees,
        total_amount,

        -- Extract date parts for easier analysis
        extract(year from sale_date) as sale_year,
        extract(month from sale_date) as sale_month,
        extract(day from sale_date) as sale_day,

        -- Metadata
        current_timestamp() as dbt_loaded_at

    from source
)

select * from cleaned