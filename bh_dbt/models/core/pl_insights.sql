select 
l.account_key,
l.amazon_region_id,
l.marketplace_key,
coalesce(l.asin,'') as asin,
coalesce(l.sku,'') as sku,
coalesce(brand_asin.brand,brand_sku.brand) as brand,
date_trunc(month,l.posted_local_date) as posted_month,
sum(case when metric = 'gross_sales' then quantity end) as gross_units_sold,
-sum(case when metric = 'reimbursed_product' then quantity end) as units_returned,
coalesce(gross_units_sold,0)+coalesce(units_returned,0) as net_units_sold,
sum(case when metric = 'gross_sales' then amount_usd end) as gross_sales,
sum(case when metric ilike '%promotion%' then amount_usd end) as net_promotions,
sum(case when metric = 'reimbursed_product' then amount_usd end) as returned_sales,
coalesce(gross_sales,0)+coalesce(returned_sales,0) as net_sales
from DATAHAWK_SHARE_83514.CUSTOM_83514.finance_profit_ledger l
left join (select distinct brand,channel_product_id from {{ref('product_pl_new')}}) brand_asin 
    on brand_asin.channel_product_id = coalesce(l.asin,'')
left join (select distinct brand,sku from {{ref('product_pl_new')}}) brand_sku
    on brand_sku.sku = coalesce(l.sku,'')
group by all 
