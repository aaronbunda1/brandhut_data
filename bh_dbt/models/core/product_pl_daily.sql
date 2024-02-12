
with sp as (select 
channel_product_id,
date_trunc(month,date_day) as impression_month,
sum(impressions) as sp_impressions,
sum(clicks) as sp_clicks,
sum(units_sold) as unit_sales_ppc
from {{var('readable')['hawkspace']}}.advertising.advertising_product_campaign_metrics
group by 1,2
)

, non_sp as (
    select
    seller_name,
    account_key,
    marketplace_key,
    brand,
    date_day,
     SponsoredBrandsVideo,
     SponsoredBrands,
     sponsoreddisplay
    from {{ref('ad_spend')}}
    pivot(sum(ad_spend) for sponsored_type in ('SponsoredBrandsVideo','SponsoredBrands','SponsoredDisplay'))
        as a (seller_name,
    account_key,
    marketplace_key,
    brand,
    date_day,
    SponsoredBrandsVideo,
    SponsoredBrands,
    SponsoredDisplay)
)

, other_ad_spend as (
select
date_trunc(month,date_day) as impression_month,
case when campaign_name ilike any ('%onanoff%','%buddyph%') then 'ONANOFF'
when campaign_name ilike '%zens%' then 'ZENS'
when campaign_name ilike '%storyph%' then 'Onanoff 2'
when campaign_name ilike '%fokus%' then 'Fokus'
when campaign_name ilike '%cellini%' then 'Cellini'
when campaign_name ilike '%spot%' then 'SPOT'
when campaign_name ilike '%qisten%' then 'Qisten'
else 'other brand' 
end as brand,
sum(clicks) as clicks,
sum(impressions) as impressions
from {{var('readable')['hawkspace']}}.advertising.advertising_campaign_metrics 
where sponsored_type != 'SponsoredProducts'
group by 1,2)


, prefinal as (select distinct 
case when c.category is null and p.brand = 'ZENS' then 'Zens Legacy' else coalesce(p.brand,o.brand) end as brand,
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
    when p.brand = 'Onanoff 2' and pl.sku ilike any ('%SS-%','%STSH-%','%shield%') then 'StoryShield'
    when p.brand = 'Onanoff 2' and pl.sku ilike '%storyph%' then 'Storyphones'
    else c.category 
end as internal_sku_category,
GROSS_SALES,
ORDERS,
UNITS_SOLD,
coalesce(-pl.net_units_sold*cogs.productcost,pl.COGS) as cogs, 
SPONSORED_PRODUCTS_COST,
coalesce(-non_sp.sponsoredbrands/count(*) over (partition by pl.date_day,pl.account_key,pl.marketplace_key,p.brand),0) as DIST_SPONSORED_BRANDS_COST,
coalesce(-non_sp.sponsoredbrandsvideo/count(*) over (partition by pl.date_day,pl.account_key,pl.marketplace_key,p.brand),0) as DIST_SPONSORED_BRANDS_VIDEO_COST,
coalesce(-non_sp.sponsoreddisplay/count(*) over (partition by pl.date_day,pl.account_key,pl.marketplace_key,p.brand),0) as DIST_SPONSORED_DISPLAY_COST,
SPONSORED_PRODUCTS_COST+
coalesce(-non_sp.sponsoredbrands/count(*) over (partition by pl.date_day,pl.account_key,pl.marketplace_key,p.brand),0) +
coalesce(-non_sp.sponsoredbrandsvideo/count(*) over (partition by pl.date_day,pl.account_key,pl.marketplace_key,p.brand),0) +
coalesce(-non_sp.sponsoreddisplay/count(*) over (partition by pl.date_day,pl.account_key,pl.marketplace_key,p.brand),0) 
as total_ad_spend_custom,
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
case when coalesce(p.brand,o.brand) = 'ZENS' then 0 else DIST_INBOUND_TRANSPORTATION end as DIST_INBOUND_TRANSPORTATION,
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
case when coalesce(p.brand,o.brand) IN ('SPOT','Storyphones','Onanoff 2','ZENS','Cellini') then 0 else TAX_PRINCIPAL end as tax_principal,
TAX_SHIPPING,
TAX_REIMBURSED,
TAX_OTHER,
case when coalesce(p.brand,o.brand) = 'ZENS' then 0 else DIST_OTHER_AMOUNT end as DIST_OTHER_AMOUNT,
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
sum(gross_sales) over (partition by date_trunc(month,pl.date_day),p.brand) as monthly_brand_gs,
case 
    when p.brand ilike '%cellini%'
        then -net_sales*0.1 
    when p.brand ilike any ('%spot%','%zens%')
        then -net_sales*0.15
    when p.brand ilike '%onanoff 2%'
        then 
        case 
            when pl.sku ilike '%storyph%' 
                then 
                    case
                        when monthly_brand_gs <= 50000 then -net_sales*.1
                        when monthly_brand_gs < 251000 then -net_sales *.09
                        else -net_sales * .06
                    end
            when pl.sku ilike any ('%SS-%','%STSH-%','%shield%')
                then 
                    case
                        when monthly_brand_gs < 251000 then -net_sales *.12
                        else -net_sales * .08
                    end
            else -net_sales*.1
        end 
    else 0 
end as brandhut_commission,
coalesce(o.turner_costs,0) as turner_costs,
coalesce(o.freight,0) as freight,
coalesce(o.ad_spend_manual,0) as ad_spend_manual_pre,
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
sp.unit_sales_ppc as units_sold_ppc,
bsr.rank as best_seller_rank,
bsr.rating
from {{var('readable')['hawkspace']}}.FINANCE.finance_product_profit_loss pl
left join {{ref('brand_asin')}} p
    on p.channel_product_id = pl.channel_product_id
    and p.MARKETPLACE_KEY = pl.MARKETPLACE_KEY
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
    on oas.brand = coalesce(p.brand,o.brand)
    and pl.date_day = oas.impression_month
left join
(select * from {{var('readable')['hawkspace']}}.MARKET.MARKET_BEST_SELLER_RANK 
qualify rank() over (partition by channel_product_id order by observation_date desc) =1) bsr
    on bsr.channel_product_id = pl.channel_product_id
left join non_sp
    on non_sp.marketplace_key = pl.marketplace_key
    and non_sp.account_key = pl.account_key
    and p.brand = non_sp.brand
    and non_sp.date_day = pl.date_day
left join {{ref('cogs')}} cogs
    on cogs.asin = pl.channel_product_id
    and pl.account_key = cogs.accountid
    and pl.date_day BETWEEN cogs.start_date and cogs.end_date)

select *,
case when sum(total_ad_spend_custom) over (partition by brand,date_trunc(month,date_day)) = 0 then ad_spend_manual_pre else 0 end as ad_spend_manual
from prefinal