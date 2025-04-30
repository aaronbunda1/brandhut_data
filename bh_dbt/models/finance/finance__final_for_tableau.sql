{{config(
    materialized='table'
)}}

select 
*,
'' AS color
from (
    SELECT * FROM {{ref('finance__unified_actuals_long')}}
    UNION
    SELECT * FROM {{ref('finance__true_up_long')}}
)