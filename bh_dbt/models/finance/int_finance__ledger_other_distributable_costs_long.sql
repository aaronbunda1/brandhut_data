WITH monthly_gross_sales_by_brand AS (
    SELECT 
    brand,
    report_month,
    SUM(amount) AS sales
    FROM {{ref('int_finance__ledger_monthly_long')}}
    WHERE metric_name = 'LEDGER_GROSS_SALES'
    GROUP BY ALL 
)

,monthly_gross_sales AS (
    SELECT 
    report_month,
    SUM(sales) AS sales
    FROM monthly_gross_sales_by_brand
    GROUP BY 1
)

, other_costs AS (
    SELECT 
    DATE_TRUNC(MONTH,posted_local_date) AS report_month,
    SUM(other_amount_distributable) AS other_amount_distributable
    FROM {{ref('stg_finance__ledger_pivoted')}}
    GROUP BY ALL
)

,distributed_other_costs_by_brand AS (
    SELECT 
    s.brand,
    s.report_month,
    ROUND(c.other_amount_distributable*(s.sales/ms.sales),2) AS dist_ledger_other_amount
    FROM monthly_gross_sales_by_brand s
    LEFT JOIN monthly_gross_sales ms
        ON ms.report_month = s.report_month
    LEFT JOIN other_costs c
        ON c.report_month = s.report_month
    GROUP BY ALL 
) 

SELECT
md5(concat_ws('|', 
    coalesce(brand,''),
    coalesce(cast(REPORT_MONTH as varchar(12)),''),
    'dist_ledger_other_amount'
)) as key,
brand,
'' AS ACCOUNT_KEY,
NULl AS REGION,
'' AS MARKETPLACE_KEY,
report_month,
'' AS asin,
'' AS SKU,
'' AS original_currency,
'' AS internal_sku_category,
'dist_ledger_other_amount' AS metric_name,
SUM(dist_ledger_other_amount) AS amount
FROM distributed_other_costs_by_brand
GROUP BY ALL
