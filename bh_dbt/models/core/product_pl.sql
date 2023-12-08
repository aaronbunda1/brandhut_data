{{config(materialized='table')}}



with by_month as (
select
CONCAT(ACCOUNT_KEY,marketplace_key,region,seller_name,CHANNEL_PRODUCT_ID,date_trunc(month,DATE_DAY),SKU,COLOR,CURRENCY) as key,
BRAND,
SELLER_NAME,
ACCOUNT_KEY,
REGION,
MARKETPLACE_KEY,
date_trunc(month,DATE_DAY) as date_day,
CHANNEL_PRODUCT_ID,
SKU,
COLOR,
CURRENCY,
rate_to_usd,
region_name,
internal_sku_category,
sum(CAST(GROSS_SALES AS NUMERIC(18,2))) AS GROSS_SALES,
sum(CAST(ORDERS AS NUMERIC(18,2))) AS ORDERS,
sum(CAST(UNITS_SOLD AS NUMERIC(18,2))) AS UNITS_SOLD,
sum(CAST(UNITS_SOLD_PPC AS NUMERIC(18,2))) AS UNITS_SOLD_PPC,
sum(CAST(COGS AS NUMERIC(18,2))) AS COGS,
sum(CAST(SPONSORED_PRODUCTS_COST AS NUMERIC(18,2))) AS SPONSORED_PRODUCTS_COST,
sum(CAST(DIST_SPONSORED_BRANDS_COST AS NUMERIC(18,2))) AS DIST_SPONSORED_BRANDS_COST,
sum(CAST(DIST_SPONSORED_BRANDS_VIDEO_COST AS NUMERIC(18,2))) AS DIST_SPONSORED_BRANDS_VIDEO_COST,
sum(CAST(DIST_SPONSORED_DISPLAY_COST AS NUMERIC(18,2))) AS DIST_SPONSORED_DISPLAY_COST,
-- CAST(SPONSORED_PRODUCTS_SALES AS NUMERIC(18,2))) AS SPONSORED_PRODUCTS_SALES,
-- CAST(DIST_SPONSORED_BRANDS_SALES AS NUMERIC(18,2))) AS DIST_SPONSORED_BRANDS_SALES,
-- CAST(DIST_SPONSORED_BRANDS_VIDEO_SALES AS NUMERIC(18,2))) AS DIST_SPONSORED_BRANDS_VIDEO_SALES,
-- CAST(DIST_SPONSORED_DISPLAY_SALES AS NUMERIC(18,2))) AS DIST_SPONSORED_DISPLAY_SALES,
sum(cast(GIFT_WRAP AS NUMERIC(18,2))) AS GIFT_WRAP,
sum(cast(REIMBURSED_PRODUCT AS NUMERIC(18,2))) AS REIMBURSED_PRODUCT,
sum(cast(REIMBURSED_PRODUCT_UNIT AS NUMERIC(18,2))) AS REIMBURSED_PRODUCT_UNIT,
sum(cast(REFUND_COMMISSION AS NUMERIC(18,2))) AS REFUND_COMMISSION,
sum(cast(REFUNDED_REFERRAL_FEES AS NUMERIC(18,2))) AS REFUNDED_REFERRAL_FEES,
sum(cast(REIMBURSED_SHIPPING AS NUMERIC(18,2))) AS REIMBURSED_SHIPPING,
sum(cast(REFUND_PROMOTION AS NUMERIC(18,2))) AS REFUND_PROMOTION,
sum(cast(REFUND_SHIPPING_PROMOTION AS NUMERIC(18,2))) AS REFUND_SHIPPING_PROMOTION,
sum(cast(REFUND_SHIPPING_CHARGEBACK AS NUMERIC(18,2))) AS REFUND_SHIPPING_CHARGEBACK,
sum(cast(GOODWILL AS NUMERIC(18,2))) AS GOODWILL,
sum(cast(REVERSAL_REIMBURSED AS NUMERIC(18,2))) AS REVERSAL_REIMBURSED,
sum(cast(GIFT_WRAP_CHARGEBACK AS NUMERIC(18,2))) AS GIFT_WRAP_CHARGEBACK,
sum(cast(SHIPPING AS NUMERIC(18,2))) AS SHIPPING,
sum(cast(SHIPPING_PROMOTION AS NUMERIC(18,2))) AS SHIPPING_PROMOTION,
sum(cast(SHIPPING_CHARGEBACK AS NUMERIC(18,2))) AS SHIPPING_CHARGEBACK,
sum(cast(DIST_INBOUND_TRANSPORTATION AS NUMERIC(18,2))) AS DIST_INBOUND_TRANSPORTATION,
sum(cast(DIST_FBA_STORAGE_FEE AS NUMERIC(18,2))) AS DIST_FBA_STORAGE_FEE,
sum(cast(DIST_FBA_INVENTORY_PLACEMENT_SERVICE AS NUMERIC(18,2))) AS DIST_FBA_INVENTORY_PLACEMENT_SERVICE,
sum(cast(WAREHOUSE_DAMAGE AS NUMERIC(18,2))) AS WAREHOUSE_DAMAGE,
sum(cast(WAREHOUSE_LOST_MANUAL AS NUMERIC(18,2))) AS WAREHOUSE_LOST_MANUAL,
sum(cast(FBA_PER_UNIT_FULFILMENT_FEE AS NUMERIC(18,2))) AS FBA_PER_UNIT_FULFILMENT_FEE,
sum(cast(DIST_DISPOSAL_COMPLETE AS NUMERIC(18,2))) AS DIST_DISPOSAL_COMPLETE,
sum(cast(DIST_REMOVAL_COMPLETE AS NUMERIC(18,2))) AS DIST_REMOVAL_COMPLETE,
sum(cast(REFERRAL_FEE AS NUMERIC(18,2))) AS REFERRAL_FEE,
sum(cast(PROMOTION AS NUMERIC(18,2))) AS PROMOTION,
sum(cast(SUBSCRIPTION_FEE AS NUMERIC(18,2))) AS SUBSCRIPTION_FEE,
sum(cast(TAX_PRINCIPAL AS NUMERIC(18,2))) AS TAX_PRINCIPAL,
sum(cast(TAX_SHIPPING AS NUMERIC(18,2))) AS TAX_SHIPPING,
sum(cast(TAX_REIMBURSED AS NUMERIC(18,2))) AS TAX_REIMBURSED,
sum(cast(TAX_OTHER AS NUMERIC(18,2))) AS TAX_OTHER,
sum(cast(DIST_OTHER_AMOUNT AS NUMERIC(18,2))) AS DIST_OTHER_AMOUNT,
sum(cast(RESTOCKING_FEE AS NUMERIC(18,2))) AS RESTOCKING_FEE,
-- sum(cast(EARNED_GROSS_SALES AS NUMERIC(18,2))) AS EARNED_GROSS_SALES,
-- sum(cast(EARNED_ORDERS AS NUMERIC(18,2))) AS EARNED_ORDERS,
-- sum(cast(EARNED_UNITS_SOLD AS NUMERIC(18,2))) AS EARNED_UNITS_SOLD,
sum(cast(TOTAL_ADVERTISING_SALES AS NUMERIC(18,2))) AS TOTAL_ADVERTISING_SALES,
-- sum(cast(TOTAL_ADVERTISING_COSTS AS NUMERIC(18,2))) AS TOTAL_ADVERTISING_COSTS,
-- sum(cast(TOTAL_REFERRAL_FEES AS NUMERIC(18,2))) AS TOTAL_REFERRAL_FEES,
-- sum(cast(TOTAL_OTHER_MARKETING_COSTS AS NUMERIC(18,2))) AS TOTAL_OTHER_MARKETING_COSTS,
-- sum(cast(TOTAL_WAREHOUSING_COSTS AS NUMERIC(18,2))) AS TOTAL_WAREHOUSING_COSTS,
-- sum(cast(TOTAL_SHIPPING_COSTS AS NUMERIC(18,2))) AS TOTAL_SHIPPING_COSTS,
-- sum(cast(TOTAL_TAXES_ON_SALES AS NUMERIC(18,2))) AS TOTAL_TAXES_ON_SALES,
sum(cast(NET_SALES AS NUMERIC(18,2))) AS NET_SALES,
sum(cast(NET_UNITS_SOLD AS NUMERIC(18,2))) AS NET_UNITS_SOLD,
sum(cast(GROSS_PROFIT AS NUMERIC(18,2))) AS GROSS_PROFIT--,
-- sum(cast(CONTRIBUTION_PROFIT_ONE AS NUMERIC(18,2))) AS CONTRIBUTION_PROFIT_ONE,
-- sum(cast(CONTRIBUTION_PROFIT_TWO AS NUMERIC(18,2))) AS CONTRIBUTION_PROFIT_TWO,
-- sum(cast(EBITDA AS NUMERIC(18,2))) AS EBITDA
, sum(cast(brandhut_commission as numeric(18,2))) as brandhut_commission
, sum(cast(turner_costs as numeric(18,2))) as turner_costs
, sum(cast(freight as numeric(18,2))) as freight
, sum(cast(ad_spend_manual as numeric(18,2))) as ad_spend_manual
, sum(cast(product_samples as numeric(18,2))) as product_samples
, sum(cast(miscellaneous_cost as numeric(18,2))) as miscellaneous_cost
, sum(total_impressions) as impressions
, sum(total_clicks) as clicks
, avg(best_seller_rank) as best_seller_rank
, avg(rating) as rating
from {{ref('product_pl_daily')}}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)
, by_brand as (

    select 
    brand,
    date_day, 
    sum(coalesce(GROSS_SALES,0))
    +sum(coalesce(COGS,0))
    +sum(coalesce(SPONSORED_PRODUCTS_COST,0))
    +sum(coalesce(DIST_SPONSORED_BRANDS_COST,0))
    +sum(coalesce(DIST_SPONSORED_BRANDS_VIDEO_COST,0))
    +sum(coalesce(DIST_SPONSORED_DISPLAY_COST,0))
    +sum(coalesce(GIFT_WRAP,0))
    +sum(coalesce(REIMBURSED_PRODUCT,0))
    +sum(coalesce(REFUND_COMMISSION,0))
    +sum(coalesce(REFUNDED_REFERRAL_FEES,0))
    +sum(coalesce(REIMBURSED_SHIPPING,0))
    +sum(coalesce(REFUND_PROMOTION,0))
    +sum(coalesce(REFUND_SHIPPING_PROMOTION,0))
    +sum(coalesce(REFUND_SHIPPING_CHARGEBACK,0))
    +sum(coalesce(GOODWILL,0))
    +sum(coalesce(REVERSAL_REIMBURSED,0))
    +sum(coalesce(GIFT_WRAP_CHARGEBACK,0))
    +sum(coalesce(SHIPPING,0))
    +sum(coalesce(SHIPPING_PROMOTION,0))
    +sum(coalesce(SHIPPING_CHARGEBACK,0))
    +sum(coalesce(DIST_INBOUND_TRANSPORTATION,0))
    +sum(coalesce(DIST_FBA_STORAGE_FEE,0))
    +sum(coalesce(DIST_FBA_INVENTORY_PLACEMENT_SERVICE,0))
    +sum(coalesce(WAREHOUSE_DAMAGE,0))
    +sum(coalesce(WAREHOUSE_LOST_MANUAL,0))
    +sum(coalesce(FBA_PER_UNIT_FULFILMENT_FEE,0))
    +sum(coalesce(DIST_DISPOSAL_COMPLETE,0))
    +sum(coalesce(DIST_REMOVAL_COMPLETE,0))
    +sum(coalesce(REFERRAL_FEE,0))
    +sum(coalesce(PROMOTION,0))
    +sum(coalesce(SUBSCRIPTION_FEE,0))
    +sum(coalesce(TAX_PRINCIPAL,0))
    +sum(coalesce(TAX_SHIPPING,0))
    +sum(coalesce(TAX_REIMBURSED,0))
    +sum(coalesce(TAX_OTHER,0))
    +sum(coalesce(DIST_OTHER_AMOUNT,0))
    +sum(coalesce(RESTOCKING_FEE,0))
    +sum(coalesce(brandhut_commission,0))
    +sum(coalesce(turner_costs,0))
    +sum(coalesce(freight,0))
    +sum(coalesce(ad_spend_manual,0))
    +sum(coalesce(product_samples,0))
    +sum(coalesce(miscellaneous_cost,0))
    as pl
    from by_month
    group by 1,2
)

,true_up_calc as (
select a.brand, a.date_day, a.pl,b.invoice_amount, b.invoice_amount - a.pl as true_up 
from by_brand a 
inner join {{ref('invoice_amounts')}} b
    on a.brand = b.brand 
    and a.date_day = b.month
)

select a.*, 
coalesce(cast(true_up/count(*) over (partition by a.brand,a.date_day) AS NUMERIC(30,2)),0) as dist_true_up,
current_timestamp() as updated_at 
from by_month a
left join true_up_calc b
    on a.brand = b.brand
    and a.date_day = b.date_day
