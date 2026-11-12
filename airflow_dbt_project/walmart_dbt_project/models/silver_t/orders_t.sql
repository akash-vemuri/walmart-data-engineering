{{
    config(
        materialized='incremental',
        unique_key='order_id'
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
),

filtered_source as (
    select
        source_data.*,
        row_number() over (
            partition by order_id
            order by
                _ingested_at desc,
                updated_timestamp desc
        ) as row_num
    from {{ source('walmart_databricks', 'orders') }} as source_data
    cross join watermark
    where source_data._ingested_at >= watermark.last_ingested_at
)

select
    * except (row_num),
    current_timestamp() as processed_at
from filtered_source
where row_num = 1

{% else %}

select
    *,
    current_timestamp() as processed_at
from {{ source('walmart_databricks', 'orders') }}

{% endif %}
