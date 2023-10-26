{{config(materialized='table')}}


with preagg as (
select distinct 
coalesce(
    case 
        when pl.sku ilike '%ZED%' then 'ZENS'
        when pl.sku ilike 'BP-%' then 'ONANOFF'
        else p.brand end,
        o.brand) as brand,
pl.SELLER_NAME,
pl.ACCOUNT_KEY,
pl.REGION,
pl.MARKETPLACE_KEY,
coalesce(pl.DATE_DAY,o.month) as date_day,
pl.CHANNEL_PRODUCT_ID,
coalesce(pl.SKU,o.sku) as sku,
pl.CURRENCY,
pl.rate_to_usd,
pl.region_name,
case 
    when c.category is null and p.brand = 'ZENS' then 'Zens Legacy'
    when p.brand = 'STORYPHONES' and pl.sku ilike any ('%SS-%','%STSH-%','%shield%') then 'StoryShield'
    when p.brand = 'STORYPHONES' and pl.sku ilike '%storyph%' then 'StoryPhones'
    else c.category 
end as internal_sku_category,
GROSS_SALES,
ORDERS,
UNITS_SOLD,
COGS,
SPONSORED_PRODUCTS_COST,
DIST_SPONSORED_BRANDS_COST,
DIST_SPONSORED_BRANDS_VIDEO_COST,
DIST_SPONSORED_DISPLAY_COST,
-- SPONSORED_PRODUCTS_SALES,
-- DIST_SPONSORED_BRANDS_SALES,
-- DIST_SPONSORED_BRANDS_VIDEO_SALES,
-- DIST_SPONSORED_DISPLAY_SALES,
GIFT_WRAP,
REIMBURSED_PRODUCT,
REIMBURSED_PRODUCT_UNIT,
REFUND_COMMISSION,
REFUNDED_REFERRAL_FEES,
REIMBURSED_SHIPPING,
REFUND_PROMOTION,
REFUND_SHIPPING_PROMOTION,
REFUND_SHIPPING_CHARGEBACK,
GOODWILL,
REVERSAL_REIMBURSED,
GIFT_WRAP_CHARGEBACK,
SHIPPING,
SHIPPING_PROMOTION,
SHIPPING_CHARGEBACK,
DIST_INBOUND_TRANSPORTATION,
DIST_FBA_STORAGE_FEE,
DIST_FBA_INVENTORY_PLACEMENT_SERVICE,
WAREHOUSE_DAMAGE,
WAREHOUSE_LOST_MANUAL,
FBA_PER_UNIT_FULFILMENT_FEE,
DIST_DISPOSAL_COMPLETE,
DIST_REMOVAL_COMPLETE,
REFERRAL_FEE,
PROMOTION,
SUBSCRIPTION_FEE,
TAX_PRINCIPAL,
TAX_SHIPPING,
TAX_REIMBURSED,
TAX_OTHER,
DIST_OTHER_AMOUNT,
RESTOCKING_FEE,
-- EARNED_GROSS_SALES,
-- EARNED_ORDERS,
-- EARNED_UNITS_SOLD,
-- TOTAL_ADVERTISING_SALES,
-- TOTAL_ADVERTISING_COSTS,
-- TOTAL_REFERRAL_FEES,
-- TOTAL_OTHER_MARKETING_COSTS,
-- TOTAL_WAREHOUSING_COSTS,
-- TOTAL_SHIPPING_COSTS,
-- TOTAL_TAXES_ON_SALES,
NET_SALES,
NET_UNITS_SOLD,
GROSS_PROFIT,
-- CONTRIBUTION_PROFIT_ONE,
-- CONTRIBUTION_PROFIT_TWO,
-- EBITDA,
sum(gross_sales) over (partition by date_trunc(month,date_day),p.brand) as monthly_brand_gs,
case 
    when p.brand ilike '%cellini%'
        then -gross_sales*0.1 
    when p.brand ilike any ('%spot%','%zens%')
        then -net_sales*0.15
    when p.brand ilike '%storyphones%'
        then 
        case 
            when pl.sku ilike '%storyph%' 
                then 
                    case
                        when monthly_brand_gs <= 50000 then -gross_sales*.1
                        when monthly_brand_gs < 251000 then -gross_sales *.09
                        else -gross_sales * .06
                    end
            when pl.sku ilike any ('%SS-%','%STSH-%','%shield%')
                then 
                    case
                        when monthly_brand_gs < 251000 then -gross_sales *.12
                        else -gross_sales * .08
                    end
            else -gross_sales*.1
        end 
    else 0 
end as brandhut_commission,
-coalesce(o.turner_costs,0) as turner_costs,
-coalesce(o.freight,0) as freight
from {{var('readable')}}.FINANCE.finance_product_profit_loss pl
left join {{var('readable')}}.reports.report_product_latest_version p
    on p.channel_product_id = pl.channel_product_id
    and p.MARKETPLACE_KEY = pl.MARKETPLACE_KEY
left join {{ref('category')}} c
    on c.channel_product_id = p.channel_product_id
full outer join {{ref('manual_metrics_by_brand_and_month')}} o 
    on o.brand = p.brand
    and pl.sku = o.sku
    and pl.date_day = o.month
)

, by_month as (
    select
BRAND,
        SELLER_NAME,
        ACCOUNT_KEY,
        REGION,
        MARKETPLACE_KEY,
        date_trunc(month,DATE_DAY) as date_day,
        CHANNEL_PRODUCT_ID,
        SKU,
        CURRENCY,
        rate_to_usd,
        region_name,
        internal_sku_category,
        sum(CAST(GROSS_SALES AS NUMERIC(18,2))) AS GROSS_SALES,
        sum(CAST(ORDERS AS NUMERIC(18,2))) AS ORDERS,
        sum(CAST(UNITS_SOLD AS NUMERIC(18,2))) AS UNITS_SOLD,
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
        -- sum(cast(TOTAL_ADVERTISING_SALES AS NUMERIC(18,2))) AS TOTAL_ADVERTISING_SALES,
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
from preagg
group by 1,2,3,4,5,6,7,8,9,10,11,12
)

select * from by_month