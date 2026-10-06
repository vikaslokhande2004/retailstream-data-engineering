select *
from {{ source('retail', 'customers') }}