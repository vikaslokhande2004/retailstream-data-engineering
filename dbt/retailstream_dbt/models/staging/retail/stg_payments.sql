select *
from {{ source('retail', 'payments') }}