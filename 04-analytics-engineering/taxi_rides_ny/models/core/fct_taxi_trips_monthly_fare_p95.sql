{{
    config(
        materialized='table'
    )
}}

-- Compute the continous percentile of fare_amount partitioning by service_type, year and and month

select
    distinct
    service_type,
    percentile_cont(fare_amount,0.97) over (partition by service_type,year,month) as p97,
    percentile_cont(fare_amount,0.95) over (partition by service_type,year,month) as p95,
    percentile_cont(fare_amount,0.90) over (partition by service_type,year,month) as p90
from {{ ref('fact_trips') }}
where fare_amount > 0 and trip_distance > 0 and payment_type_description in ('Cash', 'Credit card') 
    and year=2020 and month=4