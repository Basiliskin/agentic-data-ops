select
    orders.id as order_id,
    orders.customer_id,
    customers.email,
    orders.amount,
    orders.updated_at
from {{ ref('orders') }} as orders
join {{ ref('customers') }} as customers
    on customers.id = orders.customer_id
