{{
    config(
        materialized='table'
    )
}}

with dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select
    extract(year from fhv.pickup_datetime) as year,
    extract(month from fhv.pickup_datetime) as month,
    fhv.pickup_datetime,
    fhv.PUlocationID     as pickup_location_id,
    pickup_zone.borough  as pickup_borough,
    pickup_zone.zone     as pickup_zone,
    fhv.dropOff_datetime as dropoff_datetime,
    fhv.DOlocationID     as dropoff_location_id,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone    as dropoff_zone
from {{ source('staging','fhv_tripdata') }} fhv
inner join dim_zones as pickup_zone
on fhv.PUlocationID = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv.DOlocationID = dropoff_zone.locationid
where fhv.dispatching_base_num is not null
