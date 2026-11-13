
{{
    config(
        materialized='incremental',
        unique_key='store_id'
    )
}}

with filtered_source as (

    select
        source_data.*
    from {{ source('walmart_databricks', 'stores') }} as source_data

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
            partition by store_id
            order by
                _ingested_at desc,
                updated_timestamp desc
        ) as row_num
    from filtered_source

)

select
    * except (row_num),
    current_timestamp() as processed_at
from deduplicated_source
where row_num = 1
