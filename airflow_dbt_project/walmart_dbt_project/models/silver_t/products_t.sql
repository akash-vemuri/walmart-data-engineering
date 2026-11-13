
{{
    config(
        materialized='incremental',
        unique_key='product_id'
    )
}}

with filtered_source as (

    select
        source_data.*
    from {{ source('walmart_databricks', 'products') }} as source_data

    {% if is_incremental() %}

    cross join (
        select
            coalesce(
                max(_ingested_at) - INTERVAL 5 MINUTES,
                TIMESTAMP '1900-01-01 00:00:00'
            ) as last_ingested_at
        from {{ this }}
    ) as watermark

    where source_data._ingested_at >= watermark.last_ingested_at

    {% endif %}

),

deduplicated_source as (

    select
        *,
        row_number() over (
            partition by product_id
            order by
                updated_timestamp desc,
                _ingested_at desc
        ) as row_num
    from filtered_source

),

latest_source as (

    select
        * except (row_num),
        current_timestamp() as processed_at
    from deduplicated_source
    where row_num = 1

)

{% if is_incremental() %}

select
    source.*
from latest_source as source

left join {{ this }} as target
    on source.product_id = target.product_id

where
    target.product_id is null
    or source.updated_timestamp > target.updated_timestamp
    or (
        source.updated_timestamp = target.updated_timestamp
        and source._ingested_at > target._ingested_at
    )

{% else %}

select *
from latest_source

{% endif %}
