select id, customer_id, amount, updated_at
from {{ source('raw_shop', 'orders') }}
