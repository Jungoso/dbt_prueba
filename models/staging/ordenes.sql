select
  id as orden_id,
  cliente_id,
  fecha,
  total
from {{ source('staging', 'ordenes_base') }}
