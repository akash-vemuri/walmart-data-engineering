select distinct

    order_id,

    customer_id,

    store_id,

    order_timestamp,

    payment_method,

    order_status,

    total_amount,

    created_timestamp as order_created_timestamp,

    updated_timestamp as order_updated_timestamp,

    is_active as order_is_active,

    processed_at as order_processed_at,

    current_timestamp() as order_gold_processed_at

from

    {{ ref('orders_t') }}