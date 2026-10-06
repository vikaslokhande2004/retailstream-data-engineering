select *
from {{ source('retail', 'returns') }}