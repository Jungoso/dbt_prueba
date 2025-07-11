select *
from {{ source('staging', 'clientes_base') }}
where estado in ('activa', 'inactiva')
