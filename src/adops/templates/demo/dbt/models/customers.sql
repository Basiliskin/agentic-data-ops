select id, email, updated_at
from {{ source('raw_shop', 'customers') }}
