{{
    config(
        materialized='table'
    )
}}

-- Compute the Quarterly Revenues for each year for based on total_amount
-- Compute the Quarterly YoY (Year-over-Year) revenue growth
-- e.g.: In 2020/Q1, Green Taxi had -12.34% revenue growth compared to 2019/Q1      
-- e.g.: In 2020/Q4, Yellow Taxi had +34.56% revenue growth compared to 2019/Q4

select
    service_type,
    quarter,
    sum(revenue_2019) as revenue_2019,
    sum(revenue_2020) as revenue_2020,
    sum(revenue_2020) / sum(revenue_2019) * 100 as yoy_growth
from (
    select
        service_type,
        quarter,
        case when year=2019 then cast(sum(total_amount) as integer) end as revenue_2019,
        case when year=2020 then cast(sum(total_amount) as integer) end as revenue_2020
    from {{ ref('fact_trips') }}
    where year between 2019 and 2020
    group by
        service_type,
        quarter,
        year
    ORDER BY 
        service_type,
        quarter,
        year
)
group by
    service_type,
    quarter
order by
    service_type,
    quarter