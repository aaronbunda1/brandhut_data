
with sp as (select 
channel_product_id,
date_trunc(month,date_day) as impression_month,
sum(impressions) as sp_impressions,
sum(clicks) as sp_clicks,
sum(units_sold) as unit_sales_ppc
from datahawk_share_83514.advertising.advertising_product_campaign_metrics
group by 1,2
)


, other_ad_spend as (
select
date_trunc(month,date_day) as impression_month,
case when campaign_name ilike any ('%onanoff%','%buddyph%') then 'ONANOFF'
when campaign_name ilike '%zens%' then 'ZENS'
when campaign_name ilike '%storyph%' then 'Storyphones'
when campaign_name ilike '%cellini%' then 'Cellini'
when campaign_name ilike '%spot%' then 'Spot'
when campaign_name ilike '%qisten%' then 'Qisten'
else 'other brand' 
end as brand,
sum(clicks) as clicks,
sum(impressions) as impressions
from datahawk_share_83514.advertising.advertising_campaign_metrics 
where sponsored_type != 'SponsoredProducts'
group by 1,2)


select distinct 
coalesce(p.brand,o.brand) as brand,
pl.SELLER_NAME,
pl.ACCOUNT_KEY,
pl.REGION,
pl.MARKETPLACE_KEY,
coalesce(pl.DATE_DAY,o.month) as date_day,
pl.CHANNEL_PRODUCT_ID,
min(coalesce(pl.SKU,o.sku)) over (partition by pl.CHANNEL_PRODUCT_ID) as sku,
case when coalesce(pl.SKU,o.sku) ilike '%blue%' then 'Blue'
when coalesce(pl.SKU,o.sku) ilike '%red%' then 'Red'
when coalesce(pl.SKU,o.sku) ilike any ('%white%','%wht%') then 'White'
when coalesce(pl.SKU,o.sku) ilike any ('%black%','%blk%') then 'Black'
when coalesce(pl.SKU,o.sku) ilike '%green%' then 'Green'
when coalesce(pl.SKU,o.sku) ilike '%orange%' then 'Orange'
when coalesce(pl.SKU,o.sku) ilike '%Brown%' then 'Brown'
when coalesce(pl.SKU,o.sku) ilike any ('%gry%','%Grey%') then 'Grey'
when coalesce(pl.SKU,o.sku) ilike '%Yellow%' then 'Yellow'
when coalesce(pl.SKU,o.sku) ilike '%black%' then 'Black'
when coalesce(pl.SKU,o.sku) ilike '%purple%' then 'Purple'
when coalesce(pl.SKU,o.sku) ilike '%pink%' then 'Pink'
else 'Other'
end as color,
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
SPONSORED_PRODUCTS_SALES,
DIST_SPONSORED_BRANDS_SALES,
DIST_SPONSORED_BRANDS_VIDEO_SALES,
DIST_SPONSORED_DISPLAY_SALES,
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
TOTAL_ADVERTISING_SALES,
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
coalesce(o.turner_costs,0) as turner_costs,
coalesce(o.freight,0) as freight,
coalesce(case when o.month <date_trunc(month,dateadd(MONTH,-4,current_date()))
then o.ad_spend_manual end,0) as ad_spend_manual,
coalesce(o.product_samples,0) as product_samples,
coalesce(o.miscellaneous,0) as miscellaneous_cost,
sp_impressions,
sp_clicks,
oas.impressions / count(*) over (partition by coalesce(
    case 
        when pl.sku ilike '%ZED%' then 'ZENS'
        when pl.sku ilike 'BP-%' then 'ONANOFF'
        else p.brand end,
        o.brand), date_trunc(month,pl.date_day))
as other_impressions,
oas.clicks / count(*) over (partition by coalesce(
    case 
        when pl.sku ilike '%ZED%' then 'ZENS'
        when pl.sku ilike 'BP-%' then 'ONANOFF'
        else p.brand end,
        o.brand), date_trunc(month,pl.date_day))
as other_clicks,
sp_impressions+other_impressions as total_impressions,
sp_clicks+other_clicks as total_clicks,
bsr.rank as best_seller_rank,
bsr.rating
from {{var('readable')}}.FINANCE.finance_product_profit_loss pl
left join {{ref('brand_asin') p
    on p.channel_product_id = pl.channel_product_id
    -- and p.MARKETPLACE_KEY = pl.MARKETPLACE_KEY
left join {{ref('category')}} c
    on c.channel_product_id = p.channel_product_id
full outer join {{ref('manual_metrics_by_brand_and_month')}} o 
    on o.brand = p.brand
    and pl.sku = o.sku
    and pl.date_day = o.month
left join sp 
    on pl.channel_product_id = sp.channel_product_id
    and pl.date_day = sp.impression_month
left join other_ad_spend  oas
    on oas.brand = coalesce(
    case 
        when pl.sku ilike '%ZED%' then 'ZENS'
        when pl.sku ilike 'BP-%' then 'ONANOFF'
        else p.brand end,
        o.brand)
    and pl.date_day = oas.impression_month
left join
(select * from DATAHAWK_SHARE_83514.MARKET.MARKET_BEST_SELLER_RANK 
qualify rank() over (partition by channel_product_id order by observation_date desc) =1) bsr
    on bsr.channel_product_id = pl.channel_product_id
