select *
from {{ source('retail', 'order_items') }}