select
  p.id as producto_id,
  p.nombre,
  p.precio
from {{ ref('productos_base') }} p
