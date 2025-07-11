select * from {{ ref('clientes_base') }}
where estado in ('activa', 'inactiva')
