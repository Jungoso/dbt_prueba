select
  o.id as orden_id,
  o.fecha,
  o.total,
  o.cliente_id
from {{ ref('ordenes_base') }} o
