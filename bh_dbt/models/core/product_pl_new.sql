{{config(materialized='table')}}

with non_sp_sd as (
    select
    seller_name,
    account_key,
    marketplace_key,
    brand,
    date_day,
    sum(SponsoredBrandsVideo) as SponsoredBrandsVideo,
     sum(SponsoredBrands) as SponsoredBrands
    from {{ref('ad_spend')}}
    pivot(sum(ad_spend) for sponsored_type in ('SponsoredBrandsVideo','SponsoredBrands'))
        as a (seller_name,
    account_key,
    marketplace_key,
    brand,
    date_day,
    SponsoredBrandsVideo,
    SponsoredBrands)
    GROUP BY ALL
)

, sp_sd as (
select 
date_day,
-- account_key,
marketplace_key,
case 
when campaign_name ilike '%storyph%' then 'Onanoff 2'
when campaign_name ilike '%cellini%' then 'Cellini'
when campaign_name ilike '%fokus%' then 'Fokus'
when campaign_name ilike '%pablo%' then 'Pablo Artists'' Choice'
when campaign_name ilike '%POP%' then 'POP'
when campaign_name ilike '%SPOT%' then 'SPOT'
when campaign_name ilike '%ZENS%' then 'ZENS'
when campaign_name ilike any ('%onanoff%','%sp%','%sb%') then 'ONANOFF'
when campaign_name ilike any ('%73%','%roku%') then '73&Sunny'
else 'Other'
end as brand,
coalesce(sum(case when sponsored_type = 'SponsoredProducts' then costs end),0) as SponsoredProductsCost,
coalesce(sum(case when sponsored_type = 'SponsoredDisplay' then costs end),0) as SponsoredDisplayCost
from datahawk_share_83514.advertising.advertising_campaign_metrics a
group by all


)

, all_fields as (select 
case when c.category is null and l.brand = 'ZENS' then 'Zens Legacy' else coalesce(l.brand,o.brand) end as brand,
l.account_key as account_key,
l.amazon_region_id as region,
l.marketplace_key as marketplace_key,
coalesce(l.posted_local_date,o.month) as date_day,
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
    when l.sku = 'ZEDC21B/00' then 'ZENS'
    when c.category is null and l.brand = 'ZENS' then 'Zens Legacy'
    when l.brand = 'Onanoff 2' and l.sku ilike any ('%SS-%','%STSH-%','%shield%') then 'StoryShield'
    when l.brand = 'Onanoff 2' and l.sku ilike '%storyph%' then 'Storyphones'
    else c.category 
end as internal_sku_category,
l.gross_sales as ledger_gross_sales,
l.earned_gross_sales as EARNED_GROSS_SALES,
l.GIFT_WRAP as ledger_gift_wrap,
l.REIMBURSED_PRODUCT as ledger_reimbursed_product,
l.refund_comission as ledger_REFUND_COMMISSION,
l.REFUNDED_REFERRAL_FEES as ledger_REFUNDED_REFERRAL_FEES,
l.REIMBURSED_SHIPPING as ledger_REIMBURSED_SHIPPING,
l.REFUND_PROMOTION as ledger_REFUND_PROMOTION,
l.REFUND_SHIPPING_CHARGEBACK as ledger_REFUND_SHIPPING_CHARGEBACK,
l.GOODWILL as ledger_GOODWILL,
l.REVERSAL_REIMBURSED as ledger_REVERSAL_REIMBURSED,
l.GIFT_WRAP_CHARGEBACK as ledger_GIFT_WRAP_CHARGEBACK,
l.shipping as ledger_shipping,
l.SHIPPING_CHARGEBACK as ledger_SHIPPING_CHARGEBACK,
-- l.inbound_transportation as ledger_inbound_transportation,
coalesce(l.FBA_STORAGE_FEE,0)+coalesce(l.fba_long_storage_fee,0) as ledger_fba_storage_fee,
l.FBA_INVENTORY_PLACEMENT_SERVICE as ledger_FBA_INVENTORY_PLACEMENT_SERVICE,
l.WAREHOUSE_DAMAGE as ledger_WAREHOUSE_DAMAGE,
l.WAREHOUSE_LOST_MANUAL as ledger_WAREHOUSE_LOST_MANUAL,
l.FBA_PER_UNIT_FULFILMENT_FEE as ledger_FBA_PER_UNIT_FULFILMENT_FEE,
l.disposal_complete as ledger_disposal_complete,
l.removal_complete as ledger_removal_complete,
l.REFERRAL_FEE as ledger_referral_fee,
l.promotion as ledger_promotion,
-- l.SUBSCRIPTION_FEE as ledger_subscription_fee,
l.tax_principal_collected as ledger_tax_principal,
l.tax_shipping as ledger_tax_shipping,
l.TAX_REIMBURSED as ledger_tax_reimbursed,
l.tax_other as ledger_tax_other,
case when l.asin is null and (l.sku is null) then 0 else l.other_amount end as ledger_other_amount,
l.other_amount_distributable as ledger_other_amount_distributable,
l.other_amount_spot_only,
l.restocking_fee as ledger_restocking_fee,
coalesce(l.gross_sales,0) + coalesce(l.REIMBURSED_PRODUCT,0) + coalesce(l.REVERSAL_REIMBURSED,0) as ledger_net_sales,
coalesce(l.earned_gross_sales,0) + coalesce(l.REIMBURSED_PRODUCT,0) + coalesce(l.REVERSAL_REIMBURSED,0) as earned_net_sales,
sum(l.EARNED_GROSS_SALES) over (partition by date_trunc(month,l.posted_local_date),l.brand) as monthly_brand_gs,
case 
    when l.brand ilike '%cellini%'
        then -ledger_net_sales*0.1 
    when l.brand ilike any ('%spot%','%zens%')
        then -ledger_net_sales*0.15
    when l.brand ilike '%onanoff 2%'
        then 
        case 
            when l.sku ilike '%storyph%' 
                then 
                    case
                        when monthly_brand_gs <= 50000 then -ledger_net_sales*.1
                        when monthly_brand_gs < 251000 then -ledger_net_sales *.09
                        else -ledger_net_sales * .06
                    end
            when l.sku ilike any ('%SS-%','%STSH-%','%shield%')
                then 
                    case
                        when monthly_brand_gs < 251000 then -ledger_net_sales *.12
                        else -ledger_net_sales * .08
                    end
            else -ledger_net_sales*.1
        end 
    when l.brand = 'Pablo Artists'' Choice' then -ledger_net_SALEs *.15
    when l.brand = 'Fokus'
        then 
        case 
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 79.99 then -ledger_net_sales*.08
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 89.99 then -ledger_net_sales*.12
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 100 then -ledger_net_sales*.18
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 109.99 then -ledger_net_sales*.2
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 119.99 then -ledger_net_sales*.22
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 129.99 then -ledger_net_sales*.24
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 149.99 then -ledger_net_sales*.26
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 159.99 then -ledger_net_sales*.26
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 139.99 then -ledger_net_sales*.25
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 169.99 then -ledger_net_sales*.28
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 179.99 then -ledger_net_sales*.28
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 189.99 then -ledger_net_sales*.29
            else -ledger_net_sales*.3
        end
    else 0 
end as ledger_brandhut_commission,
case 
    when l.brand ilike '%cellini%'
        then -earned_net_sales*0.1 
    when l.brand ilike any ('%spot%','%zens%')
        then -earned_net_sales*0.15
    when l.brand ilike '%onanoff 2%'
        then 
        case 
            when l.sku ilike '%storyph%' 
                then 
                    case
                        when monthly_brand_gs <= 50000 then -earned_net_sales*.1
                        when monthly_brand_gs < 251000 then -earned_net_sales *.09
                        else -earned_net_sales * .06
                    end
            when l.sku ilike any ('%SS-%','%STSH-%','%shield%')
                then 
                    case
                        when monthly_brand_gs < 251000 then -earned_net_sales *.12
                        else -earned_net_sales * .08
                    end
            else -earned_net_sales*.1
        end 
    when l.brand = 'Fokus'
        then 
        case 
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 79.99 then -earned_net_sales*.08
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 89.99 then -earned_net_sales*.12
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 100 then -earned_net_sales*.18
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 109.99 then -earned_net_sales*.2
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 119.99 then -earned_net_sales*.22
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 129.99 then -earned_net_sales*.24
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 149.99 then -earned_net_sales*.26
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 159.99 then -earned_net_sales*.26
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 139.99 then -earned_net_sales*.25
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 169.99 then -earned_net_sales*.28
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 179.99 then -earned_net_sales*.28
            when l.EARNED_GROSS_SALES/nullif(greatest(l.earned_units_sold,1),1) <= 189.99 then -earned_net_sales*.29
            else -earned_net_sales*.3
        end
    else 0 
end as EARNED_BRANDHUT_COMMISSION,
coalesce(o.turner_costs,0) as manual_turner_costs,
coalesce(o.freight,0) as manual_freight,
coalesce(o.ad_spend_manual,0) as manual_ad_spend,
coalesce(o.product_samples,0) as manual_product_samples,
coalesce(o.miscellaneous,0) as manual_miscellaneous_cost,
coalesce(o.manual_unallocated_costs,0) as manual_unallocated_costs,
coalesce(o.true_up_invoiced,0) as true_up_invoiced,
coalesce(-pl.net_units_sold*cogs.productcost,pl.COGS) as MANUAL_COGS,
-sp_sd.sponsoredproductscost/count(*) over (partition by l.posted_local_date,l.brand,l.marketplace_key) as SPONSORED_PRODUCTS_COST,
-sp_sd.sponsoreddisplaycost/count(*) over (partition by l.posted_local_date,l.brand,l.marketplace_key) as sponsored_display_cost,
coalesce(-non_sp_sd.sponsoredbrands/count(*) over (partition by l.posted_local_date,l.account_key,l.marketplace_key,l.brand),0) as DIST_SPONSORED_BRANDS_COST,
coalesce(-non_sp_sd.sponsoredbrandsvideo/count(*) over (partition by l.posted_local_date,l.account_key,l.marketplace_key,l.brand),0) as DIST_SPONSORED_BRANDS_VIDEO_COST,
from {{ref('finance_pl_pivot_new')}} l
left join datahawk_share_83514.referential.referential_currency_rate cr on l.posted_local_date = cr.date_day and l.currency  = cr.currency
left join {{ref('category')}} c
    on c.channel_product_id = l.asin
left join non_sp_sd
    on non_sp_sd.marketplace_key = l.marketplace_key
    and non_sp_sd.account_key = l.account_key
    and l.brand = non_sp_sd.brand
    and non_sp_sd.date_day = l.posted_local_date
left join sp_sd
    on sp_sd.brand = l.brand
    and sp_sd.date_day = l.posted_local_date
    and sp_sd.marketplace_key = l.marketplace_key
full outer join {{ref('manual_metrics_by_brand_and_month')}} o 
    on o.brand = l.brand
    and l.sku = o.sku
    and l.posted_local_date = o.month
left join datahawk_share_83514.finance.finance_product_profit_loss pl
    on pl.marketplace_key = l.marketplace_key
    and pl.date_day = l.posted_local_date
    and pl.account_key = l.account_key
    and pl.region = l.amazon_region_id
    and pl.channel_product_id = l.asin
    and pl.currency = l.currency
    and pl.sku = l.sku
left join (select distinct * from {{ref('cogs')}}) cogs
    on cogs.asin = l.asin
    and l.account_key = cogs.accountid
    and l.posted_local_date between cogs.start_date and cogs.end_date
)

, by_month as (
select
BRAND,
ACCOUNT_KEY,
REGION,
MARKETPLACE_KEY,
date_trunc(month,date_day) as date_day,
CHANNEL_PRODUCT_ID,
SKU,
COLOR,
CURRENCY,
internal_sku_category,
avg(rate_to_usd) as rate_to_usd,
sum(CAST(ledger_gross_sales AS NUMERIC(18,2))) AS ledger_gross_sales,
sum(CAST(EARNED_GROSS_SALES AS NUMERIC(18,2))) AS EARNED_GROSS_SALES,
sum(CAST(ledger_gift_wrap AS NUMERIC(18,2))) AS ledger_gift_wrap,
sum(CAST(ledger_reimbursed_product AS NUMERIC(18,2))) AS ledger_reimbursed_product,
sum(CAST(ledger_REFUND_COMMISSION AS NUMERIC(18,2))) AS ledger_REFUND_COMMISSION,
sum(CAST(ledger_REFUNDED_REFERRAL_FEES AS NUMERIC(18,2))) AS ledger_REFUNDED_REFERRAL_FEES,
sum(CAST(ledger_REIMBURSED_SHIPPING AS NUMERIC(18,2))) AS ledger_REIMBURSED_SHIPPING,
sum(CAST(ledger_REFUND_PROMOTION AS NUMERIC(18,2))) AS ledger_REFUND_PROMOTION,
sum(CAST(ledger_REFUND_SHIPPING_CHARGEBACK AS NUMERIC(18,2))) AS ledger_REFUND_SHIPPING_CHARGEBACK,
sum(CAST(ledger_GOODWILL AS NUMERIC(18,2))) AS ledger_GOODWILL,
sum(CAST(ledger_REVERSAL_REIMBURSED AS NUMERIC(18,2))) AS ledger_REVERSAL_REIMBURSED,
sum(CAST(ledger_GIFT_WRAP_CHARGEBACK AS NUMERIC(18,2))) AS ledger_GIFT_WRAP_CHARGEBACK,
sum(CAST(ledger_shipping AS NUMERIC(18,2))) AS ledger_shipping,
sum(CAST(ledger_SHIPPING_CHARGEBACK AS NUMERIC(18,2))) AS ledger_SHIPPING_CHARGEBACK,
-- sum(CAST(ledger_inbound_transportation AS NUMERIC(18,2))) AS ledger_inbound_transportation,
sum(CAST(ledger_fba_storage_fee AS NUMERIC(18,2))) AS ledger_fba_storage_fee,
sum(CAST(ledger_FBA_INVENTORY_PLACEMENT_SERVICE AS NUMERIC(18,2))) AS ledger_FBA_INVENTORY_PLACEMENT_SERVICE,
sum(CAST(ledger_WAREHOUSE_DAMAGE AS NUMERIC(18,2))) AS ledger_WAREHOUSE_DAMAGE,
sum(CAST(ledger_WAREHOUSE_LOST_MANUAL AS NUMERIC(18,2))) AS ledger_WAREHOUSE_LOST_MANUAL,
sum(CAST(ledger_FBA_PER_UNIT_FULFILMENT_FEE AS NUMERIC(18,2))) AS ledger_FBA_PER_UNIT_FULFILMENT_FEE,
sum(CAST(ledger_disposal_complete AS NUMERIC(18,2))) AS ledger_disposal_complete,
sum(CAST(ledger_removal_complete AS NUMERIC(18,2))) AS ledger_removal_complete,
sum(CAST(ledger_referral_fee AS NUMERIC(18,2))) AS ledger_referral_fee,
sum(CAST(ledger_promotion AS NUMERIC(18,2))) AS ledger_promotion,
-- sum(CAST(ledger_subscription_fee AS NUMERIC(18,2))) AS ledger_subscription_fee,
sum(CAST(ledger_tax_principal AS NUMERIC(18,2))) AS ledger_tax_principal,
sum(CAST(ledger_tax_shipping AS NUMERIC(18,2))) AS ledger_tax_shipping,
sum(CAST(ledger_tax_reimbursed AS NUMERIC(18,2))) AS ledger_tax_reimbursed,
sum(CAST(ledger_tax_other AS NUMERIC(18,2))) AS ledger_tax_other,
sum(CAST(ledger_other_amount AS NUMERIC(18,2))) AS ledger_other_amount,
sum(CAST(ledger_other_amount_distributable AS NUMERIC(18,2))) AS ledger_other_amount_distributable,
sum(CAST(other_amount_spot_only AS NUMERIC(18,2))) AS other_amount_spot_only,
sum(CAST(ledger_restocking_fee AS NUMERIC(18,2))) AS ledger_restocking_fee,
sum(CAST(ledger_brandhut_commission AS NUMERIC(18,2))) AS ledger_brandhut_commission,
sum(CAST(EARNED_BRANDHUT_COMMISSION AS NUMERIC(18,2))) AS EARNED_BRANDHUT_COMMISSION,
SUM(CAST(manual_turner_costs AS NUMERIC(18,2))) AS manual_turner_costs,
SUM(CAST(manual_freight AS NUMERIC(18,2))) AS manual_freight,
SUM(CAST(manual_product_samples AS NUMERIC(18,2))) AS manual_product_samples,
SUM(CAST(manual_miscellaneous_cost AS NUMERIC(18,2))) AS manual_miscellaneous_cost,
SUM(CAST(manual_unallocated_costs AS NUMERIC(18,2))) AS manual_unallocated_costs,
SUM(CAST(true_up_invoiced AS NUMERIC(18,2))) AS true_up_invoiced,
SUM(CAST(MANUAL_COGS AS NUMERIC(18,2))) AS MANUAL_COGS,
SUM(CAST(SPONSORED_PRODUCTS_COST AS NUMERIC(18,2))) AS SPONSORED_PRODUCTS_COST,
SUM(CAST(DIST_SPONSORED_BRANDS_COST AS NUMERIC(18,2))) AS DIST_SPONSORED_BRANDS_COST,
SUM(CAST(DIST_SPONSORED_BRANDS_VIDEO_COST AS NUMERIC(18,2))) AS DIST_SPONSORED_BRANDS_VIDEO_COST,
SUM(CAST(SPONSORED_DISPLAY_COST AS NUMERIC(18,2))) AS SPONSORED_DISPLAY_COST,
SUM(CAST(SPONSORED_PRODUCTS_COST AS NUMERIC(18,2)))+SUM(CAST(DIST_SPONSORED_BRANDS_COST AS NUMERIC(18,2)))+SUM(CAST(DIST_SPONSORED_BRANDS_VIDEO_COST AS NUMERIC(18,2)))+
SUM(CAST(SPONSORED_DISPLAY_COST AS NUMERIC(18,2))) as ad_spend_total,
SUM(CAST(manual_ad_spend AS NUMERIC(18,2))) as manual_ad_spend_temp
from all_fields
group by all
)

, fix as (
    select 
    *,
    cast(nullif(sum(ledger_other_amount_distributable) over (partition by date_day,currency)*(ledger_gross_sales/sum(ledger_gross_sales) over (partition by date_day,currency)),0) as NUMERIC(30,2)) as dist_ledger_other_amount,
    cast(nullif(sum(other_amount_spot_only) over (partition by date_day,currency)*(IFF(brand='SPOT',ledger_gross_sales,0)/nullif(sum(IFF(brand='SPOT',ledger_gross_sales,0)) over (partition by date_day,currency),0)),0) as NUMERIC(30,2)) as dist_other_amount_spot_only,
    case when coalesce(sum(ad_spend_total) over (partition by date_day, brand),0) = 0 then manual_ad_spend_temp else 0 end as manual_ad_spend
    from by_month

)


, prefinal as (
    SELECT
        concat(
        coalesce(BRAND,''),
        coalesce(ACCOUNT_KEY,''),
        coalesce(cast(REGION as varchar(12)),''),
        coalesce(MARKETPLACE_KEY,''),
        coalesce(cast(DATE_DAY as varchar(12)),''),
        coalesce(CHANNEL_PRODUCT_ID,''),
        coalesce(SKU,''),
        coalesce(metric_name,''),
        coalesce(currency,''))
        as key,
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
        internal_sku_category,
        metric_name,
        current_timestamp() as updated_at,
        -- round(amount/coalesce(rate_to_usd,1),2) as amount
        round(amount,2) as amount
    FROM fix
    UNPIVOT(amount FOR metric_name IN (
ledger_gross_sales,
EARNED_GROSS_SALES,
ledger_gift_wrap,
ledger_reimbursed_product,
ledger_REFUND_COMMISSION,
ledger_REFUNDED_REFERRAL_FEES,
ledger_REIMBURSED_SHIPPING,
ledger_REFUND_PROMOTION,
ledger_REFUND_SHIPPING_CHARGEBACK,
ledger_GOODWILL,
ledger_REVERSAL_REIMBURSED,
ledger_GIFT_WRAP_CHARGEBACK,
ledger_shipping,
ledger_SHIPPING_CHARGEBACK,
-- ledger_inbound_transportation,
ledger_fba_storage_fee,
ledger_FBA_INVENTORY_PLACEMENT_SERVICE,
ledger_WAREHOUSE_DAMAGE,
ledger_WAREHOUSE_LOST_MANUAL,
ledger_FBA_PER_UNIT_FULFILMENT_FEE,
ledger_disposal_complete,
ledger_removal_complete,
ledger_referral_fee,
ledger_promotion,
-- ledger_subscription_fee,
-- ledger_tax_principal,
-- ledger_tax_shipping,
-- ledger_tax_reimbursed,
-- ledger_tax_other,
ledger_other_amount,
dist_ledger_other_amount,
dist_other_amount_spot_only,
ledger_restocking_fee,
ledger_brandhut_commission,
EARNED_BRANDHUT_COMMISSION,
manual_turner_costs,
manual_freight,
manual_ad_spend,
manual_product_samples,
manual_miscellaneous_cost,
manual_unallocated_costs,
true_up_invoiced,
MANUAL_COGS,
SPONSORED_PRODUCTS_COST,
DIST_SPONSORED_BRANDS_COST,
DIST_SPONSORED_BRANDS_VIDEO_COST,
SPONSORED_DISPLAY_COST
))
)

, final_without_true_up as (
select 
prefinal.*, 
case 
when metric_name in (
    'MANUAL_AD_SPEND',
'DIST_SPONSORED_BRANDS_COST',
'SPONSORED_DISPLAY_COST',
'DIST_SPONOSORED_VIDEO_COST',
'SPONSORED_PRODUCTS_COST',
'LEDGER_BRANDHUT_COMMISSION',
'EARNED_BRANDHUT_COMMISSION',
'MANUAL_COGS',
'MANUAL_MISCELLANEOUS_COST',
'MANUAL_UNALLOCATED_COSTS',
'TRUE_UP_INVOICED',
-- 'LEDGER_SUBSCRIPTION_FEE',
'DIST_LEDGER_OTHER_AMOUNT',
'DIST_OTHER_AMOUNT_SPOT_ONLY',
'LEDGER_OTHER_AMOUNT',
'MANUAL_PRODUCT_SAMPLES',
'LEDGER_GIFT_WRAP',
'LEDGER_GIFT_WRAP_CHARGEBACK',
'LEDGER_GOODWILL',
'LEDGER_PROMOTION',
'LEDGER_REFUND_PROMOTION',
'LEDGER_REFERRAL_FEE',
'LEDGER_REFUND_COMMISSION',
'LEDGER_REFUNDED_REFERRAL_FEES',
'LEDGER_FBA_PER_UNIT_FULFILMENT_FEE',
-- 'LEDGER_INBOUND_TRANSPORTATION',
'LEDGER_REFUND_SHIPPING_CHARGEBACK',
'LEDGER_REIMBURSED_SHIPPING',
'LEDGER_SHIPPING',
'LEDGER_SHIPPING_CHARGEBACK',
'MANUAL_FREIGHT',
'LEDGER_TAX_OTHER',
'LEDGER_TAX_PRINCIPAL',
'LEDGER_TAX_REIMBURSED',
'LEDGER_TAX_SHIPPING',
'MANUAL_TURNER_COSTS',
'LEDGER_FBA_INVENTORY_PLACEMENT_SERVICE',
'LEDGER_FBA_STORAGE_FEE',
'LEDGER_RESTOCKING_FEE',
'LEDGER_WAREHOUSE_DAMAGE',
'LEDGER_WAREHOUSE_LOST_MANUAL',
'LEDGER_DISPOSAL_COMPLETE',
'LEDGER_REMOVAL_COMPLETE'
)
 then 'Expenses'
when metric_name in (
'EARNED_GROSS_SALES',
'LEDGER_GROSS_SALES',
'LEDGER_REIMBURSED_PRODUCT',
'LEDGER_REVERSAL_REIMBURSED'
)
then 'Net Sales'
end as metric_group_1, 
case 
    when metric_name in (
        'MANUAL_AD_SPEND',
    'DIST_SPONSORED_BRANDS_COST',
    'SPONSORED_DISPLAY_COST',
    'DIST_SPONOSORED_VIDEO_COST',
    'SPONSORED_PRODUCTS_COST'
    )
then 'Ad Spend'
when metric_name in (
'LEDGER_BRANDHUT_COMMISSION',
'EARNED_BRANDHUT_COMMISSION'
)
then 'Brandhut Commission'
when metric_name in ('EARNED_GROSS_SALES',
'LEDGER_GROSS_SALES')
then 'Gross Sales'
when metric_name IN (
    'MANUAL_COGS' ,
    'MANUAL_MISCELLANEOUS_COST',
    'MANUAL_UNALLOCATED_COSTS',
    'TRUE_UP_INVOICED',
'DIST_LEDGER_OTHER_AMOUNT',
'DIST_OTHER_AMOUNT_SPOT_ONLY',
'LEDGER_OTHER_AMOUNT',
'MANUAL_PRODUCT_SAMPLES',
'MANUAL_TURNER_COSTS',
'LEDGER_REMOVAL_COMPLETE',
'LEDGER_DISPOSAL_COMPLETE'
)
then 'Other Expenses'
when metric_name in (
'LEDGER_GIFT_WRAP',
'LEDGER_GIFT_WRAP_CHARGEBACK',
'LEDGER_GOODWILL',
'LEDGER_PROMOTION',
'LEDGER_REFUND_PROMOTION'
)
then 'Other Marketing'
when metric_name in (

'LEDGER_REFERRAL_FEE',
'LEDGER_REFUND_COMMISSION',
'LEDGER_REFUNDED_REFERRAL_FEES'
) then 'Referral Fees'
when metric_name in (
    'LEDGER_REIMBURSED_PRODUCT',
'LEDGER_REVERSAL_REIMBURSED'
)
then 'Returns'
when metric_name in (
    'LEDGER_FBA_PER_UNIT_FULFILMENT_FEE',
'LEDGER_REFUND_SHIPPING_CHARGEBACK',
'LEDGER_REIMBURSED_SHIPPING',
'LEDGER_SHIPPING',
'LEDGER_SHIPPING_CHARGEBACK',
'MANUAL_FREIGHT'
)
then 'Shipping'
when metric_name in ('LEDGER_TAX_OTHER',
'LEDGER_TAX_PRINCIPAL',
'LEDGER_TAX_REIMBURSED',
'LEDGER_TAX_SHIPPING')
then 'Taxes'
when metric_name in (
    -- 'LEDGER_INBOUND_TRANSPORTATION',
'LEDGER_FBA_INVENTORY_PLACEMENT_SERVICE',
'LEDGER_FBA_STORAGE_FEE',
'LEDGER_RESTOCKING_FEE',
'LEDGER_WAREHOUSE_DAMAGE',
'LEDGER_WAREHOUSE_LOST_MANUAL'
)
then 'Warehousing'
end as metric_group_2
from prefinal
where 1=1 
and prefinal.amount !=0 
)

, pl_brand_month as (
    select date_day,
    brand,
    sum(amount) as pl
    from final_without_true_up f
    where metric_group_1 in ('Net Sales','Expenses') and metric_name NOT IN ('EARNED_GROSS_SALES','EARNED_BRANDHUT_COMMISSION')
    group by all
)


, data_movements as (
select
concat(i.brand,i.month,'DATA_MOVEMENTS') as key,
        p.BRAND,
        null as ACCOUNT_KEY,
        null as REGION,
        null as MARKETPLACE_KEY,
        p.DATE_DAY as date_day,
        null as CHANNEL_PRODUCT_ID,
        null as SKU,
        null as COLOR,
        null as currency_original,
        'USD' as CURRENCY,
        null as rate_to_usd,
        null as internal_sku_category,
        'DATA_MOVEMENTS' as metric_name,
        current_timestamp() as updated_at,
        i.invoice_amount - p.pl as amount,
        'Expenses' as metric_group_1,
        'Other Expenses' as metric_group_2
from pl_brand_month p
left join {{ref('invoice_amounts')}} i
    on i.month = p.date_day
    and i.brand = p.brand
where i.brand is not null 
and date_day >= '2024-01-01'
)

, final_without_true_up_with_data_movements as (
select * 
from final_without_true_up
union all
select * 
from data_movements
)


, pl_brand_month_with_data_movements as (
    select date_day,
    brand,
    sum(amount) as pl
    from final_without_true_up_with_data_movements f
    where metric_group_1 in ('Net Sales','Expenses') and metric_name NOT IN ('EARNED_GROSS_SALES','EARNED_BRANDHUT_COMMISSION','DATA_MOVEMENTS','TRUE_UP_INVOICED','MANUAL_UNALLOCATED_COSTS')
    group by all
)

, true_up_live as (
select
concat(i.brand,i.month,'TRUE_UP') as key,
        p.BRAND,
        null as ACCOUNT_KEY,
        null as REGION,
        null as MARKETPLACE_KEY,
        dateadd(month,1,max(i.month) over (partition by i.brand)) as date_day,
        null as CHANNEL_PRODUCT_ID,
        null as SKU,
        null as COLOR,
        null as currency_original,
        'USD' as CURRENCY,
        null as rate_to_usd,
        null as internal_sku_category,
        'TRUE_UP_LIVE_CALCULATED' as metric_name,
        current_timestamp() as updated_at,
        -(i.invoice_amount - p.pl) as amount,
        'Expenses' as metric_group_1,
        'Other Expenses' as metric_group_2
from pl_brand_month_with_data_movements p
left join {{ref('invoice_amounts')}} i
    on i.month = p.date_day
    and i.brand = p.brand
where i.brand is not null 
and p.date_day >= '2024-01-01'
and p.date_day < date_trunc(month,current_date())

)

, units as (
    select
     concat(b.brand,
     date_trunc(month,posted_local_date),
     case metric 
        when 'gross_sales' then 'Units for Settled Gross Sales'
        when 'reimbursed_product' then 'Units for Settled Refunds' 
    end,
    coalesce(channel_product_id,''),
    coalesce(sku,''),
    coalesce(currency,''),
    coalesce(marketplace_key,''),
    coalesce(amazon_region_id,''),
    coalesce(account_key,'')
    ) as key, 
    b.brand,
    account_key,
    amazon_region_id as region,
    marketplace_key,
    date_trunc(month,posted_local_date) as date_day,
    coalesce(asin,'') as channel_product_id,
    l.sku,
    null as color,
    currency as currency_original,
    null as currency,
    null as rate_to_usd,
    b.internal_sku_category,
    case metric 
        when 'gross_sales' then 'Units for Settled Gross Sales'
        when 'reimbursed_product' then 'Units for Settled Refunds' 
    end as metric_name,
    current_timestamp() as updated_at,
    sum(quantity) as amount,
    'Other' as metric_group_1,
    'Other' as metric_group_2,
    from DATAHAWK_SHARE_83514.CUSTOM_83514.finance_profit_ledger l
    left join (select distinct brand, channel_product_id,max(internal_sku_category) as internal_sku_category from final_without_true_up_with_data_movements group by all) b
        on coalesce(l.asin,'') = coalesce(b.channel_product_id,'')
    where metric IN ('gross_sales','reimbursed_product')
    group by all
)
, orders as (
    select
    concat(b.brand,
    date_trunc(month,posted_local_date),
    'Orders for Settled Gross Sales',
    coalesce(channel_product_id,''),
    coalesce(sku,''),
    coalesce(currency,''),
    coalesce(marketplace_key,''),
    coalesce(amazon_region_id,''),
    coalesce(account_key,'')
    ) as key, 
    b.brand,
    account_key,
    amazon_region_id as region,
    marketplace_key,
    date_trunc(month,posted_local_date) as date_day,
    coalesce(asin,'') as channel_product_id,
    l.sku,
    null as color,
    currency as currency_original,
    null as currency,
    null as rate_to_usd,
    b.internal_sku_category,
    'Orders for Settled Gross Sales' metric_name,
    current_timestamp() as updated_at,
    count(distinct order_id) as amount,
    'Other' as metric_group_1,
    'Other' as metric_group_2,
    from DATAHAWK_SHARE_83514.CUSTOM_83514.finance_profit_ledger l
    left join (select distinct brand, channel_product_id,max(internal_sku_category) as internal_sku_category from final_without_true_up_with_data_movements group by all) b
        on coalesce(l.asin,'') = coalesce(b.channel_product_id,'')
    where metric IN ('gross_sales')
    group by all
)


select * 
from final_without_true_up_with_data_movements
where amount != 0
union all
select * 
from true_up_live
where amount != 0
union all
select * from units
where amount != 0
union all
select * from orders
where amount != 0
