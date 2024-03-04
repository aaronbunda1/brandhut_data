{{config(materialized='table')}}

with all_fields as (select 
l.brand as brand,
l.account_key as account_key,
l.amazon_region_id as region,
l.marketplace_key as marketplace_key,
l.posted_local_date as date_day,
l.asin as channel_product_id,
l.sku as sku,
l.currency as currency,
case when l.sku ilike '%blue%' then 'Blue'
when l.sku ilike '%red%' then 'Red'
when l.sku ilike any ('%white%','%wht%') then 'White'
when l.sku ilike any ('%black%','%blk%') then 'Black'
when l.sku ilike '%green%' then 'Green'
when l.sku ilike '%orange%' then 'Orange'
when l.sku ilike '%Brown%' then 'Brown'
when l.sku ilike any ('%gry%','%Grey%') then 'Grey'
when l.sku ilike '%Yellow%' then 'Yellow'
when l.sku ilike '%black%' then 'Black'
when l.sku ilike '%purple%' then 'Purple'
when l.sku ilike '%pink%' then 'Pink'
else 'Other'
end as color,
cr.rate as rate_to_usd,
case 
    when c.category is null and l.brand = 'ZENS' then 'Zens Legacy'
    when l.brand = 'Onanoff 2' and l.sku ilike any ('%SS-%','%STSH-%','%shield%') then 'StoryShield'
    when l.brand = 'Onanoff 2' and l.sku ilike '%storyph%' then 'Storyphones'
    else c.category 
end as internal_sku_category,
-- a.gross_sales as gross_sales, 
l.gross_sales as ledger_gross_sales,
-- a.earned_gross_sales as earned_gross_sales,
l.earned_gross_sales as ledger_earned_gross_sales,
-- a.orders as orders,
-- a.earned_orders as earned_orders,
-- a.units_sold as units_sold,
-- a.EARNED_UNITS_SOLD as earned_units_sold,
-- a.REIMBURSED_PRODUCT_UNIT,
-- a.cogs as cogs,
-- a.SPONSORED_PRODUCTS_COST,
-- a.dist_sponsored_brands_cost,
-- a.DIST_SPONSORED_BRANDS_VIDEO_COST,
-- a.DIST_SPONSORED_DISPLAY_cost,
-- a.total_ad_spend_custom,
-- a.GIFT_WRAP,
l.GIFT_WRAP as ledger_gift_wrap,
-- a.REIMBURSED_PRODUCT,
l.REIMBURSED_PRODUCT as ledger_reimbursed_product,
-- a.REFUND_COMMISSION,
l.refund_comission as ledger_REFUND_COMMISSION,
-- a.REFUNDED_REFERRAL_FEES,
l.REFUNDED_REFERRAL_FEES as ledger_REFUNDED_REFERRAL_FEES,
-- a.REIMBURSED_SHIPPING,
l.REIMBURSED_SHIPPING as ledger_REIMBURSED_SHIPPING,
-- a.REFUND_PROMOTION,
l.REFUND_PROMOTION as ledger_REFUND_PROMOTION,
-- a.REFUND_SHIPPING_CHARGEBACK,
l.REFUND_SHIPPING_CHARGEBACK as ledger_REFUND_SHIPPING_CHARGEBACK,
-- a.GOODWILL,
l.GOODWILL as ledger_GOODWILL,
-- a.REVERSAL_REIMBURSED,
l.REVERSAL_REIMBURSED as ledger_REVERSAL_REIMBURSED,
-- a.GIFT_WRAP_CHARGEBACK,
l.GIFT_WRAP_CHARGEBACK as ledger_GIFT_WRAP_CHARGEBACK,
-- a.SHIPPING,
l.shipping as ledger_shipping,
-- a.SHIPPING_CHARGEBACK,
l.SHIPPING_CHARGEBACK as ledger_SHIPPING_CHARGEBACK,
-- a.DIST_INBOUND_TRANSPORTATION,
l.inbound_transportation as ledger_inbound_transportation,
-- a.DIST_FBA_STORAGE_FEE,
coalesce(l.FBA_STORAGE_FEE,0)+coalesce(l.fba_long_storage_fee,0) as ledger_fba_storage_fee,
-- a.DIST_FBA_INVENTORY_PLACEMENT_SERVICE,
l.FBA_INVENTORY_PLACEMENT_SERVICE as ledger_FBA_INVENTORY_PLACEMENT_SERVICE,
-- a.WAREHOUSE_DAMAGE,
l.WAREHOUSE_DAMAGE as ledger_WAREHOUSE_DAMAGE,
-- a.WAREHOUSE_LOST_MANUAL,
l.WAREHOUSE_LOST_MANUAL as ledger_WAREHOUSE_LOST_MANUAL,
-- a.FBA_PER_UNIT_FULFILMENT_FEE,
l.FBA_PER_UNIT_FULFILMENT_FEE as ledger_FBA_PER_UNIT_FULFILMENT_FEE,
-- a.DIST_DISPOSAL_COMPLETE,
l.disposal_complete as ledger_disposal_complete,
-- a.DIST_REMOVAL_COMPLETE,
l.removal_complete as ledger_removal_complete,
-- a.REFERRAL_FEE,
l.REFERRAL_FEE as ledger_referral_fee,
-- a.PROMOTION,
l.promotion as ledger_promotion,
-- a.SUBSCRIPTION_FEE,
l.SUBSCRIPTION_FEE as ledger_subscription_fee,
-- a.tax_principal,
l.tax_principal_collected as ledger_tax_principal,
-- a.TAX_SHIPPING,
l.tax_shipping as ledger_tax_shipping,
-- a.TAX_REIMBURSED,
l.TAX_REIMBURSED as ledger_tax_reimbursed,
-- a.TAX_OTHER,
l.tax_other as ledger_tax_other,
-- a.DIST_OTHER_AMOUNT,
l.other_amount as ledger_other_amount,
-- a.RESTOCKING_FEE,
l.restocking_fee as ledger_restocking_fee,
l.gross_sales + l.REIMBURSED_PRODUCT + l.REVERSAL_REIMBURSED as net_sales,
sum(l.EARNED_GROSS_SALES) over (partition by date_trunc(month,l.posted_local_date),l.brand) as monthly_brand_gs,
case 
    when l.brand ilike '%cellini%'
        then -net_sales*0.1 
    when l.brand ilike any ('%spot%','%zens%')
        then -net_sales*0.15
    when l.brand ilike '%onanoff 2%'
        then 
        case 
            when l.sku ilike '%storyph%' 
                then 
                    case
                        when monthly_brand_gs <= 50000 then -net_sales*.1
                        when monthly_brand_gs < 251000 then -net_sales *.09
                        else -net_sales * .06
                    end
            when l.sku ilike any ('%SS-%','%STSH-%','%shield%')
                then 
                    case
                        when monthly_brand_gs < 251000 then -net_sales *.12
                        else -net_sales * .08
                    end
            else -net_sales*.1
        end 
    when l.brand = 'Fokus'
        then 
        case 
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 79.99 then -net_sales*.08
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 89.99 then -net_sales*.12
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 100 then -net_sales*.18
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 109.99 then -net_sales*.2
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 119.99 then -net_sales*.22
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 129.99 then -net_sales*.24
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 149.99 then -net_sales*.26
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 159.99 then -net_sales*.26
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 139.99 then -net_sales*.25
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 169.99 then -net_sales*.28
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 179.99 then -net_sales*.28
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 189.99 then -net_sales*.29
            else -net_sales*.3
        end
    else 0 
end as ledger_brandhut_commission,
-- a.turner_costs,
-- a.freight,
-- a.product_samples,
-- a.miscellaneous_cost,
-- a.ad_spend_manual
from {{ref('finance_pl_pivot_new')}} l
left join datahawk_share_83514.referential.referential_currency_rate cr on l.posted_local_date = cr.date_day and l.currency  = cr.currency
left join {{ref('category')}} c
    on c.channel_product_id = l.asin
)

, by_month as (
select
CONCAT(coalesce(ACCOUNT_KEY,''),
coalesce(BRAND,''),
coalesce(marketplace_key,''),
coalesce(region::STRING,''),
coalesce(CHANNEL_PRODUCT_ID,''),date_trunc(month,DATE_DAY),
coalesce(SKU,''),
coalesce(CURRENCY,'')) as key,
BRAND,
ACCOUNT_KEY,
REGION,
MARKETPLACE_KEY,
date_trunc(month,date_day) as date_day,
CHANNEL_PRODUCT_ID,
SKU,
COLOR,
CURRENCY,
-- region_name,
internal_sku_category,
avg(rate_to_usd) as rate_to_usd,
-- sum(CAST(gross_sales AS NUMERIC(18,2))) AS gross_sales,
sum(CAST(ledger_gross_sales AS NUMERIC(18,2))) AS ledger_gross_sales,
-- sum(CAST(earned_gross_sales AS NUMERIC(18,2))) AS earned_gross_sales,
sum(CAST(ledger_earned_gross_sales AS NUMERIC(18,2))) AS ledger_earned_gross_sales,
-- sum(CAST(orders AS NUMERIC(18,2))) AS orders,
-- sum(CAST(earned_orders AS NUMERIC(18,2))) AS earned_orders,
-- sum(CAST(units_sold AS NUMERIC(18,2))) AS units_sold,
-- sum(CAST(earned_units_sold AS NUMERIC(18,2))) AS earned_units_sold,
-- sum(CAST(REIMBURSED_PRODUCT_UNIT AS NUMERIC(18,2))) AS REIMBURSED_PRODUCT_UNIT,
-- sum(CAST(cogs AS NUMERIC(18,2))) AS cogs,
-- sum(CAST(SPONSORED_PRODUCTS_COST AS NUMERIC(18,2))) AS SPONSORED_PRODUCTS_COST,
-- sum(CAST(dist_sponsored_brands_cost AS NUMERIC(18,2))) AS dist_sponsored_brands_cost,
-- sum(CAST(DIST_SPONSORED_BRANDS_VIDEO_COST AS NUMERIC(18,2))) AS DIST_SPONSORED_BRANDS_VIDEO_COST,
-- sum(CAST(DIST_SPONSORED_DISPLAY_cost AS NUMERIC(18,2))) AS DIST_SPONSORED_DISPLAY_cost,
-- sum(CAST(total_ad_spend_custom AS NUMERIC(18,2))) AS total_ad_spend_custom,
-- sum(CAST(GIFT_WRAP AS NUMERIC(18,2))) AS GIFT_WRAP,
sum(CAST(ledger_gift_wrap AS NUMERIC(18,2))) AS ledger_gift_wrap,
-- sum(CAST(REIMBURSED_PRODUCT AS NUMERIC(18,2))) AS REIMBURSED_PRODUCT,
sum(CAST(ledger_reimbursed_product AS NUMERIC(18,2))) AS ledger_reimbursed_product,
-- sum(CAST(REFUND_COMMISSION AS NUMERIC(18,2))) AS REFUND_COMMISSION,
sum(CAST(ledger_REFUND_COMMISSION AS NUMERIC(18,2))) AS ledger_REFUND_COMMISSION,
-- sum(CAST(REFUNDED_REFERRAL_FEES AS NUMERIC(18,2))) AS REFUNDED_REFERRAL_FEES,
sum(CAST(ledger_REFUNDED_REFERRAL_FEES AS NUMERIC(18,2))) AS ledger_REFUNDED_REFERRAL_FEES,
-- sum(CAST(REIMBURSED_SHIPPING AS NUMERIC(18,2))) AS REIMBURSED_SHIPPING,
sum(CAST(ledger_REIMBURSED_SHIPPING AS NUMERIC(18,2))) AS ledger_REIMBURSED_SHIPPING,
-- sum(CAST(REFUND_PROMOTION AS NUMERIC(18,2))) AS REFUND_PROMOTION,
sum(CAST(ledger_REFUND_PROMOTION AS NUMERIC(18,2))) AS ledger_REFUND_PROMOTION,
-- sum(CAST(REFUND_SHIPPING_CHARGEBACK AS NUMERIC(18,2))) AS REFUND_SHIPPING_CHARGEBACK,
sum(CAST(ledger_REFUND_SHIPPING_CHARGEBACK AS NUMERIC(18,2))) AS ledger_REFUND_SHIPPING_CHARGEBACK,
-- sum(CAST(GOODWILL AS NUMERIC(18,2))) AS GOODWILL,
sum(CAST(ledger_GOODWILL AS NUMERIC(18,2))) AS ledger_GOODWILL,
-- sum(CAST(REVERSAL_REIMBURSED AS NUMERIC(18,2))) AS REVERSAL_REIMBURSED,
sum(CAST(ledger_REVERSAL_REIMBURSED AS NUMERIC(18,2))) AS ledger_REVERSAL_REIMBURSED,
-- sum(CAST(GIFT_WRAP_CHARGEBACK AS NUMERIC(18,2))) AS GIFT_WRAP_CHARGEBACK,
sum(CAST(ledger_GIFT_WRAP_CHARGEBACK AS NUMERIC(18,2))) AS ledger_GIFT_WRAP_CHARGEBACK,
-- sum(CAST(SHIPPING AS NUMERIC(18,2))) AS SHIPPING,
sum(CAST(ledger_shipping AS NUMERIC(18,2))) AS ledger_shipping,
-- sum(CAST(SHIPPING_CHARGEBACK AS NUMERIC(18,2))) AS SHIPPING_CHARGEBACK,
sum(CAST(ledger_SHIPPING_CHARGEBACK AS NUMERIC(18,2))) AS ledger_SHIPPING_CHARGEBACK,
-- sum(CAST(DIST_INBOUND_TRANSPORTATION AS NUMERIC(18,2))) AS DIST_INBOUND_TRANSPORTATION,
sum(CAST(ledger_inbound_transportation AS NUMERIC(18,2))) AS ledger_inbound_transportation,
-- sum(CAST(DIST_FBA_STORAGE_FEE AS NUMERIC(18,2))) AS DIST_FBA_STORAGE_FEE,
sum(CAST(ledger_fba_storage_fee AS NUMERIC(18,2))) AS ledger_fba_storage_fee,
-- sum(CAST(DIST_FBA_INVENTORY_PLACEMENT_SERVICE AS NUMERIC(18,2))) AS DIST_FBA_INVENTORY_PLACEMENT_SERVICE,
sum(CAST(ledger_FBA_INVENTORY_PLACEMENT_SERVICE AS NUMERIC(18,2))) AS ledger_FBA_INVENTORY_PLACEMENT_SERVICE,
-- sum(CAST(WAREHOUSE_DAMAGE AS NUMERIC(18,2))) AS WAREHOUSE_DAMAGE,
sum(CAST(ledger_WAREHOUSE_DAMAGE AS NUMERIC(18,2))) AS ledger_WAREHOUSE_DAMAGE,
-- sum(CAST(WAREHOUSE_LOST_MANUAL AS NUMERIC(18,2))) AS WAREHOUSE_LOST_MANUAL,
sum(CAST(ledger_WAREHOUSE_LOST_MANUAL AS NUMERIC(18,2))) AS ledger_WAREHOUSE_LOST_MANUAL,
-- sum(CAST(FBA_PER_UNIT_FULFILMENT_FEE AS NUMERIC(18,2))) AS FBA_PER_UNIT_FULFILMENT_FEE,
sum(CAST(ledger_FBA_PER_UNIT_FULFILMENT_FEE AS NUMERIC(18,2))) AS ledger_FBA_PER_UNIT_FULFILMENT_FEE,
-- sum(CAST(DIST_DISPOSAL_COMPLETE AS NUMERIC(18,2))) AS DIST_DISPOSAL_COMPLETE,
sum(CAST(ledger_disposal_complete AS NUMERIC(18,2))) AS ledger_disposal_complete,
-- sum(CAST(DIST_REMOVAL_COMPLETE AS NUMERIC(18,2))) AS DIST_REMOVAL_COMPLETE,
sum(CAST(ledger_removal_complete AS NUMERIC(18,2))) AS ledger_removal_complete,
-- sum(CAST(REFERRAL_FEE AS NUMERIC(18,2))) AS REFERRAL_FEE,
sum(CAST(ledger_referral_fee AS NUMERIC(18,2))) AS ledger_referral_fee,
-- sum(CAST(PROMOTION AS NUMERIC(18,2))) AS PROMOTION,
sum(CAST(ledger_promotion AS NUMERIC(18,2))) AS ledger_promotion,
-- sum(CAST(SUBSCRIPTION_FEE AS NUMERIC(18,2))) AS SUBSCRIPTION_FEE,
sum(CAST(ledger_subscription_fee AS NUMERIC(18,2))) AS ledger_subscription_fee,
-- sum(CAST(tax_principal AS NUMERIC(18,2))) AS tax_principal,
sum(CAST(ledger_tax_principal AS NUMERIC(18,2))) AS ledger_tax_principal,
-- sum(CAST(TAX_SHIPPING AS NUMERIC(18,2))) AS TAX_SHIPPING,
sum(CAST(ledger_tax_shipping AS NUMERIC(18,2))) AS ledger_tax_shipping,
-- sum(CAST(TAX_REIMBURSED AS NUMERIC(18,2))) AS TAX_REIMBURSED,
sum(CAST(ledger_tax_reimbursed AS NUMERIC(18,2))) AS ledger_tax_reimbursed,
-- sum(CAST(TAX_OTHER AS NUMERIC(18,2))) AS TAX_OTHER,
sum(CAST(ledger_tax_other AS NUMERIC(18,2))) AS ledger_tax_other,
-- sum(CAST(DIST_OTHER_AMOUNT AS NUMERIC(18,2))) AS DIST_OTHER_AMOUNT,
sum(CAST(ledger_other_amount AS NUMERIC(18,2))) AS ledger_other_amount,
-- sum(CAST(RESTOCKING_FEE AS NUMERIC(18,2))) AS RESTOCKING_FEE,
sum(CAST(ledger_restocking_fee AS NUMERIC(18,2))) AS ledger_restocking_fee,
sum(CAST(ledger_brandhut_commission AS NUMERIC(18,2))) AS ledger_brandhut_commission--,
-- sum(CAST(turner_costs AS NUMERIC(18,2))) AS turner_costs,
-- sum(CAST(freight AS NUMERIC(18,2))) AS freight,
-- sum(CAST(product_samples AS NUMERIC(18,2))) AS product_samples,
-- sum(CAST(miscellaneous_cost AS NUMERIC(18,2))) AS miscellaneous_cost,
-- sum(CAST(ad_spend_manual AS NUMERIC(18,2))) AS ad_spend_manual
from all_fields
group by all
)


, prefinal as (
    SELECT
        concat(
        ACCOUNT_KEY,
        REGION,
        MARKETPLACE_KEY,
        DATE_DAY,
        CHANNEL_PRODUCT_ID,
        SKU) as key,
        BRAND,
        ACCOUNT_KEY,
        REGION,
        MARKETPLACE_KEY,
        DATE_DAY,
        CHANNEL_PRODUCT_ID,
        SKU,
        COLOR,
        coalesce(currency,'USD') as currency_original,
        'USD' as CURRENCY,
        rate_to_usd,
        -- region_name,
        internal_sku_category,
        metric_name,
        current_timestamp() as updated_at,
        -- round(amount,2) as amount
        round(amount/coalesce(rate_to_usd,1),2) as amount
    FROM by_month
    UNPIVOT(amount FOR metric_name IN (--gross_sales,
ledger_gross_sales,
-- earned_gross_sales,
ledger_earned_gross_sales,
-- orders,
-- earned_orders,
-- units_sold,
-- earned_units_sold,
-- REIMBURSED_PRODUCT_UNIT,
-- cogs,
-- SPONSORED_PRODUCTS_COST,
-- dist_sponsored_brands_cost,
-- DIST_SPONSORED_BRANDS_VIDEO_COST,
-- DIST_SPONSORED_DISPLAY_cost,
-- total_ad_spend_custom,
-- GIFT_WRAP,
ledger_gift_wrap,
-- REIMBURSED_PRODUCT,
ledger_reimbursed_product,
-- REFUND_COMMISSION,
ledger_REFUND_COMMISSION,
-- REFUNDED_REFERRAL_FEES,
ledger_REFUNDED_REFERRAL_FEES,
-- REIMBURSED_SHIPPING,
ledger_REIMBURSED_SHIPPING,
-- REFUND_PROMOTION,
ledger_REFUND_PROMOTION,
-- REFUND_SHIPPING_CHARGEBACK,
ledger_REFUND_SHIPPING_CHARGEBACK,
-- GOODWILL,
ledger_GOODWILL,
-- REVERSAL_REIMBURSED,
ledger_REVERSAL_REIMBURSED,
-- GIFT_WRAP_CHARGEBACK,
ledger_GIFT_WRAP_CHARGEBACK,
-- SHIPPING,
ledger_shipping,
-- SHIPPING_CHARGEBACK,
ledger_SHIPPING_CHARGEBACK,
-- DIST_INBOUND_TRANSPORTATION,
ledger_inbound_transportation,
-- DIST_FBA_STORAGE_FEE,
ledger_fba_storage_fee,
-- DIST_FBA_INVENTORY_PLACEMENT_SERVICE,
ledger_FBA_INVENTORY_PLACEMENT_SERVICE,
-- WAREHOUSE_DAMAGE,
ledger_WAREHOUSE_DAMAGE,
-- WAREHOUSE_LOST_MANUAL,
ledger_WAREHOUSE_LOST_MANUAL,
-- FBA_PER_UNIT_FULFILMENT_FEE,
ledger_FBA_PER_UNIT_FULFILMENT_FEE,
-- DIST_DISPOSAL_COMPLETE,
ledger_disposal_complete,
-- DIST_REMOVAL_COMPLETE,
ledger_removal_complete,
-- REFERRAL_FEE,
ledger_referral_fee,
-- PROMOTION,
ledger_promotion,
-- SUBSCRIPTION_FEE,
ledger_subscription_fee,
-- tax_principal,
ledger_tax_principal,
-- TAX_SHIPPING,
ledger_tax_shipping,
-- TAX_REIMBURSED,
ledger_tax_reimbursed,
-- TAX_OTHER,
ledger_tax_other,
-- DIST_OTHER_AMOUNT,
ledger_other_amount,
-- RESTOCKING_FEE,
ledger_restocking_fee,
ledger_brandhut_commission--,
-- turner_costs,
-- freight,
-- product_samples,
-- miscellaneous_cost,
-- ad_spend_manual
))
)

select 
prefinal.*
from prefinal
where 1=1 
and prefinal.amount !=0 