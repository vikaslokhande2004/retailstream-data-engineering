select *
from {{ source('retail', 'products') }}