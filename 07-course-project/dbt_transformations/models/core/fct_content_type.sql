{{
    config(
        materialized='table'
    )
}}

with netflix_movies as (
    select
        *, 
        'Netflix' as streaming_service
    from {{ ref('stg_netflix_data') }}
)
select *
from netflix_movies
