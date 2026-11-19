select
    customer_id,
    count(*) as order_count,
    sum(amount) as total_amount,
    max(updated_at) as last_order_at
from {{ ref('orders') }}
where customer_id is not null
group by customer_id
