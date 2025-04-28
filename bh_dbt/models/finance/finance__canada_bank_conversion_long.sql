
select 
md5(concat(report_month,brand,'BANK_CONVERSION_FEE')) as key,
brand,
'' as account_key,
'' as region,
marketplace_key,
report_month,
'' as channel_product_id,
'' as sku,
'' as original_currency,
'' as internal_sku_category,
'BANK_CONVERSION_FEE' as metric_name,
-sum(
    case when marketplace_key = 'Amazon-CA' THEN  amount*0.015
    when marketplace_key = 'Amazon-GB' AND brand = 'SPOT' THEN amount*.015
    when marketplace_key = 'Amazon-GB' AND brand ilike '%tiny tree%' THEN amount*0.015
    END) as amount
from {{ref('int_finance__ledger_monthly_long')}}
where (marketplace_key = 'Amazon-CA' OR (marketplace_key = 'Amazon-GB' AND brand ilike any ('SPOT','%tiny tree%')))
and {{metric_group_2('metric_name')}} IN (
    'Ad Spend',
    'Brandhut Commission',
    'Other Expenses',
    'Other Marketing',
    'Referral Fees',
    'Shipping',
    'Taxes',
    'Warehousing',
    'Gross Sales',
    'Returns'
)
group by all