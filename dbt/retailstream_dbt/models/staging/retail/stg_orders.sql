select *
from {{ source('retail', 'orders') }}