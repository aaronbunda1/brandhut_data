{{config(materialized='ephemeral')}}
SELECT 
*,
DATE_TRUNC(DAY,MONTH)::DATE AS month_date
FROM {{source('sharepoint','manual_metrics')}} 