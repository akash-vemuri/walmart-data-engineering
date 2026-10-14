select
    order_id,
    order_item_id,
    customer_id,
    product_id,
    store_id,
    quantity,
    total_amount,
    unit_price,
    line_amount
from 
    {{ ref('obt_b') }}