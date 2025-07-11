select
    cliente_id,
    date_trunc('month', fecha) as mes,
    sum(total) as total_ventas,
    count(distinct orden_id) as cantidad_ordenes
from {{ ref('ordenes_enriquecidas') }}
group by cliente_id, mes
order by cliente_id, mes