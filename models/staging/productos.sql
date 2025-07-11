select
  id as producto_id,
  nombre,
  precio
from {{ source('staging', 'productos_base') }}
