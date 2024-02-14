
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
    account_key,
    marketplace_key,
    brand,
    date_day,
     SponsoredBrandsVideo,
     SponsoredBrands,
     sponsoreddisplay
    from {{ref('ad_spend')}}
    pivot(sum(ad_spend) for sponsored_type in ('SponsoredBrandsVideo','SponsoredBrands','SponsoredDisplay'))
        as a (
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
pl.ACCOUNT_KEY,
pl.MARKETPLACE_KEY,
coalesce(pl.posted_local_date,o.month) as date_day,
pl.channel_product_id as CHANNEL_PRODUCT_ID,
min(coalesce(pl.SKU,o.sku)) over (partition by pl.channel_product_id) as sku,
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
pl2.rate_to_usd,
case 
    when c.category is null and p.brand = 'ZENS' then 'Zens Legacy'
    when p.brand = 'Onanoff 2' and pl.sku ilike any ('%SS-%','%STSH-%','%shield%') then 'StoryShield'
    when p.brand = 'Onanoff 2' and pl.sku ilike '%storyph%' then 'Storyphones'
    else c.category 
end as internal_sku_category,
pl.GROSS_SALES,
pl.ORDERS,
pl.UNITS_SOLD,
coalesce(-pl2.net_units_sold*cogs.productcost,pl2.COGS) as cogs, 
pl.SPONSORED_PRODUCTS_COST,
-- coalesce(-non_sp.sponsoredbrands/count(*) over (partition by pl.posted_local_date,pl.account_key,pl.marketplace_key,p.brand),0) as DIST_SPONSORED_BRANDS_COST,
-- coalesce(-non_sp.sponsoredbrandsvideo/count(*) over (partition by pl.posted_local_date,pl.account_key,pl.marketplace_key,p.brand),0) as DIST_SPONSORED_BRANDS_VIDEO_COST,
-- coalesce(-non_sp.sponsoreddisplay/count(*) over (partition by pl.posted_local_date,pl.account_key,pl.marketplace_key,p.brand),0) as DIST_SPONSORED_DISPLAY_COST,
-- pl.SPONSORED_PRODUCTS_COST+
-- coalesce(-non_sp.sponsoredbrands/count(*) over (partition by pl.posted_local_date,pl.account_key,pl.marketplace_key,p.brand),0) +
-- coalesce(-non_sp.sponsoredbrandsvideo/count(*) over (partition by pl.posted_local_date,pl.account_key,pl.marketplace_key,p.brand),0) +
-- coalesce(-non_sp.sponsoreddisplay/count(*) over (partition by pl.posted_local_date,pl.account_key,pl.marketplace_key,p.brand),0) 
-- as total_ad_spend_custom,
pl2.SPONSORED_PRODUCTS_SALES,
DIST_SPONSORED_BRANDS_SALES,
DIST_SPONSORED_BRANDS_VIDEO_SALES,
DIST_SPONSORED_DISPLAY_SALES,
pl.GIFT_WRAP,
pl.REIMBURSED_PRODUCT,
pl2.REIMBURSED_PRODUCT_UNIT,
pl.refund_comission,
pl.REFUNDED_REFERRAL_FEES,
pl.REIMBURSED_SHIPPING,
pl.REFUND_PROMOTION,
pl.REFUND_SHIPPING_PROMOTION,
pl.REFUND_SHIPPING_CHARGEBACK,
pl.GOODWILL,
pl.REVERSAL_REIMBURSED,
pl.GIFT_WRAP_CHARGEBACK,
pl.SHIPPING,
pl.SHIPPING_PROMOTION,
pl.SHIPPING_CHARGEBACK,
pl.INBOUND_TRANSPORTATION,
pl.FBA_STORAGE_FEE,
pl.FBA_INVENTORY_PLACEMENT_SERVICE,
pl.WAREHOUSE_DAMAGE,
pl.WAREHOUSE_LOST_MANUAL,
pl.FBA_PER_UNIT_FULFILMENT_FEE,
pl.DISPOSAL_COMPLETE,
pl.REMOVAL_COMPLETE,
pl.REFERRAL_FEE,
pl.PROMOTION,
pl.SUBSCRIPTION_FEE,
pl.tax_principal+pl.tax_principal_collected as tax_principal,
pl.TAX_SHIPPING,
pl.TAX_REIMBURSED,
pl.TAX_OTHER,
pl.OTHER_AMOUNT,
pl.RESTOCKING_FEE,
-- EARNED_GROSS_SALES,
-- EARNED_ORDERS,
-- EARNED_UNITS_SOLD,
pl2.TOTAL_ADVERTISING_SALES,
-- TOTAL_ADVERTISING_COSTS,
-- TOTAL_REFERRAL_FEES,
-- TOTAL_OTHER_MARKETING_COSTS,
-- TOTAL_WAREHOUSING_COSTS,
-- TOTAL_SHIPPING_COSTS,
-- TOTAL_TAXES_ON_SALES,
pl.NET_SALES,
pl2.NET_UNITS_SOLD,
-- GROSS_PROFIT,
-- CONTRIBUTION_PROFIT_ONE,
-- CONTRIBUTION_PROFIT_TWO,
-- EBITDA,
sum(pl.gross_sales) over (partition by date_trunc(month,pl.posted_local_date),p.brand) as monthly_brand_gs,
case 
    when p.brand ilike '%cellini%'
        then -pl.net_sales*0.1 
    when p.brand ilike any ('%spot%','%zens%')
        then -pl.net_sales*0.15
    when p.brand ilike '%onanoff 2%'
        then 
        case 
            when pl.sku ilike '%storyph%' 
                then 
                    case
                        when monthly_brand_gs <= 50000 then -pl.net_sales*.1
                        when monthly_brand_gs < 251000 then -pl.net_sales *.09
                        else -pl.net_sales * .06
                    end
            when pl.sku ilike any ('%SS-%','%STSH-%','%shield%')
                then 
                    case
                        when monthly_brand_gs < 251000 then -pl.net_sales *.12
                        else -pl.net_sales * .08
                    end
            else -pl.net_sales*.1
        end 
    when p.brand = 'Fokus'
        then 
        case 
            when pl.gross_sales/pl.units_sold <= 79.99 then -pl.net_sales*.08
            when pl.gross_sales/pl.units_sold <= 89.99 then -pl.net_sales*.12
            when pl.gross_sales/pl.units_sold <= 100 then -pl.net_sales*.18
            when pl.gross_sales/pl.units_sold <= 109.99 then -pl.net_sales*.2
            when pl.gross_sales/pl.units_sold <= 119.99 then -pl.net_sales*.22
            when pl.gross_sales/pl.units_sold <= 129.99 then -pl.net_sales*.24
            when pl.gross_sales/pl.units_sold <= 139.99 then -pl.net_sales*.25
            when pl.gross_sales/pl.units_sold <= 149.99 then -pl.net_sales*.26
            when pl.gross_sales/pl.units_sold <= 159.99 then -pl.net_sales*.26
            when pl.gross_sales/pl.units_sold <= 169.99 then -pl.net_sales*.28
            when pl.gross_sales/pl.units_sold <= 179.99 then -pl.net_sales*.28
            when pl.gross_sales/pl.units_sold <= 189.99 then -pl.net_sales*.29
            else -pl.net_sales*.3
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
        o.brand), date_trunc(month,pl.posted_local_date))
as other_impressions,
oas.clicks / count(*) over (partition by coalesce(
    case 
        when pl.sku ilike '%ZED%' then 'ZENS'
        when pl.sku ilike 'BP-%' then 'ONANOFF'
        else p.brand end,
        o.brand), date_trunc(month,pl.posted_local_date))
as other_clicks,
sp_impressions+other_impressions as total_impressions,
sp_clicks+other_clicks as total_clicks,
sp.unit_sales_ppc as units_sold_ppc,
bsr.rank as best_seller_rank,
bsr.rating
from {{ref('finance_pl_pivot_new')}} pl
left join {{ref('brand_asin')}} p
    on p.channel_product_id = pl.channel_product_id
    and p.MARKETPLACE_KEY = pl.MARKETPLACE_KEY
left join {{ref('category')}} c
    on c.channel_product_id = p.channel_product_id
full outer join {{ref('manual_metrics_by_brand_and_month')}} o 
    on o.brand = p.brand
    and pl.sku = o.sku
    and pl.posted_local_date = o.month
left join sp 
    on pl.channel_product_id = sp.channel_product_id
    and pl.posted_local_date = sp.impression_month
left join other_ad_spend  oas
    on oas.brand = coalesce(p.brand,o.brand)
    and pl.posted_local_date = oas.impression_month
left join
(select * from {{var('readable')['hawkspace']}}.MARKET.MARKET_BEST_SELLER_RANK 
qualify rank() over (partition by channel_product_id order by observation_date desc) =1) bsr
    on bsr.channel_product_id = pl.channel_product_id
left join non_sp
    on non_sp.marketplace_key = pl.marketplace_key
    and non_sp.account_key = pl.account_key
    and p.brand = non_sp.brand
    and non_sp.date_day = pl.posted_local_date
left join {{ref('cogs')}} cogs
    on cogs.asin = pl.channel_product_id
    and pl.account_key = cogs.accountid
    and pl.posted_local_date BETWEEN cogs.start_date and cogs.end_date
left join datahawk_share_83514.finance.finance_product_profit_loss pl2
    on pl.account_key = pl2.account_key
    and pl.amazon_region_id = pl2.region
    and pl.marketplace_key = pl2.marketplace_key
    and pl.posted_local_date = pl2.date_day
    and pl.channel_product_id = pl2.channel_product_id
    and pl.currency = pl.currency
)

select *--,
-- case when sum(total_ad_spend_custom) over (partition by brand,date_trunc(month,date_day)) = 0 then ad_spend_manual_pre else 0 end as ad_spend_manual
from prefinal