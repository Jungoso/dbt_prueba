with ordenes_clean as (
    select *
    from {{ ref('ordenes') }}
    where total > 0
      and cliente_id is not null
),
clientes_clean as (
    select *
    from {{ ref('clientes') }}
    where estado in ('activa', 'pendiente')
),
productos_clean as (
    select *
    from {{ ref('productos') }}
)

select
    o.orden_id,
    o.fecha,
    o.total,
    c.id as cliente_id,
    c.estado as cliente_estado,
    p.producto_id,
    p.nombre as producto_nombre,
    p.categoria as producto_categoria
from ordenes_clean o
left join clientes_clean c on o.cliente_id = c.id
