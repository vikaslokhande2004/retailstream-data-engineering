select *
from {{ source('retail', 'categories') }}