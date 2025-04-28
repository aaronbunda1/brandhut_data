{{config(
    materialized='incremental',
    unique_key='key')}}

SELECT *,
{{metric_group_2('metric_name')}} as metric_group_2,
{{metric_group_1('metric_group_2')}} as metric_group_1
FROM (
    SELECT * FROM {{ref('int_finance__ledger_monthly_long')}}
    UNION 
    SELECT * FROM {{ref('int_ad_spend__long')}}
    UNION 
    SELECT * FROM {{ref('int_finance__manual_cogs_long')}}
    UNION 
    SELECT * FROM {{ref('int_finance__manual_metrics_long')}}
    UNION 
    SELECT * FROM {{ref('int_finance__ledger_other_distributable_costs_long')}}
    UNION 
    SELECT * FROM {{ref('finance__canada_bank_conversion_long')}}
)