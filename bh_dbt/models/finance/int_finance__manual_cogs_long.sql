SELECT
md5(concat_ws('|', 
    coalesce(BRAND,''),
    coalesce(ACCOUNT_KEY,''),
    coalesce(cast(REGION as varchar(12)),''),
    coalesce(MARKETPLACE_KEY,''),
    coalesce(cast(REPORT_MONTH as varchar(12)),''),
    coalesce(l.asin,''),
    coalesce(SKU,''),
    'Manual COGS',
    coalesce(original_currency,'')
)) as key,
BRAND,
ACCOUNT_KEY,
REGION,
MARKETPLACE_KEY,
report_month,
l.asin,
SKU,
original_currency,
internal_sku_category,
'Manual COGS' AS metric_name,
round(amount*productcost,2) as amount
FROM {{ref('int_finance__ledger_monthly_long')}} l
LEFT JOIN {{ref('stg_finance__manual_cogs')}} c
    ON c.AccountID = account_key
    AND c.marketplace = marketplace_key
    AND c.asin = l.asin
    AND l.report_month >=start_date
    AND l.report_month < end_date
WHERE brand = 'Onanoff'
AND metric_name = 'LEDGER_UNITS_SOLD'