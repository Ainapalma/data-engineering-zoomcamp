{{
    config(
        materialized='table'
    )
}}

-- Compute the continous p90 of trip_duration partitioning by year, month, pickup_location_id, and dropoff_location_id

select
    year,
    month,
    pickup_datetime,
    pickup_location_id,
    pickup_zone,
    dropoff_datetime,
    dropoff_location_id,
    dropoff_zone,
    timestamp_diff(dropoff_datetime, pickup_datetime, second) as trip_duration,
    cast(percentile_cont(timestamp_diff(dropoff_datetime, pickup_datetime, second), 0.90) 
        over (partition by year, month, pickup_location_id, dropoff_location_id) as numeric) as p90
from {{ ref('dim_fhv_trips') }}
