{{
    config(
        materialized='view'
    )
}}

select *
from {{ source('staging','netflix_data') }}
    
-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
