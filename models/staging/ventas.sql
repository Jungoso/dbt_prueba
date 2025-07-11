select 1001 as venta_id, 1 as cliente_id, 10 as producto_id, 500.00 as monto
union all
select 1002, 2, 11, null          -- es un error para ver validación de tests
union all
select 1003, 99, 12, 300.00       -- es un error para ver validación
