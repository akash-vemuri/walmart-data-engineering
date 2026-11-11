select
    product_id,
    price
from {{ ref('products_t') }}
where price < 0