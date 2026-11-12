
{{
    config(
        materialized='incremental',
        unique_key='product_id'
    )
}}

{% if is_incremental() %}

with watermark as (

    select
        coalesce(
            max(_ingested_at) - INTERVAL 5 MINUTES,
            TIMESTAMP '1900-01-01 00:00:00'
        ) as last_ingested_at
    from {{ this }}

)

select
    source_data.*,
    current_timestamp() as processed_at
from {{ source('walmart_databricks', 'products') }} as source_data
cross join watermark
where source_data._ingested_at >= watermark.last_ingested_at

{% else %}

select
    *,
    current_timestamp() as processed_at
from {{ source('walmart_databricks', 'products') }}

{% endif %}
