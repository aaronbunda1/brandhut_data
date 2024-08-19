with spine as (
    -- the purpose here is to spread costs to all ASINs for a given brand for which there was ever postive gross sales 
    select 
    DISTINCT 
    brand,
    internal_sku_category as category,
    account_key,
    marketplace_key,
    channel_product_id as asin
    from {{ref('product_pl_new')}}
    where metric_name = 'LEDGER_GROSS_SALES'
    and amount>0
)
, acm_with_brand as (
select acm.*, 
{{get_brand_from_sku('acm.campaign_name')}} as brand
from datahawk_share_83514.advertising.advertising_campaign_metrics acm
where sponsored_type = 'SponsoredBrands'
)

, earned_gross_sales as (
    select 
    account_key,
    marketplace_key,
    date_day,
    channel_product_id as asin,
    sum(earned_gross_sales) as earned_gross_sales
    from {{ref('product_pl_daily')}} pl
    group by all
)

select 
acm.account_key,
acm.marketplace_key,
s.brand,
s.category,
s.ASIN,
acm.campaign_id,
acm.campaign_name,
acm.sponsored_type,
acm.date_day,
count(distinct asin) over (partition by brand) as number_of_asins_represented_by_campaign,
acm.costs as total_ad_spend_for_campaign_for_day,
round(acm.costs/number_of_asins_represented_by_campaign,100) AS assumed_ad_spend_for_asin_for_day,
acm.sales as total_ad_sales_for_campaign_for_day,
case 
    when sum(egs.earned_gross_sales) OVER (partition by date_day,campaign_id) >0 
    then round(acm.sales*(egs.earned_gross_sales/sum(egs.earned_gross_sales) OVER (partition by date_day,campaign_id)),100)
    else round(acm.sales/number_of_asins_represented_by_campaign,100)
    end AS assumed_ad_sales_for_asin_for_day,
round(egs.earned_gross_sales,100) as earned_gross_sales_for_asin_for_day,
acm.orders as total_ad_orders_for_campaign_for_day,
round(acm.orders/number_of_asins_represented_by_campaign,100) AS assumed_ad_orders_for_asin_for_day,
acm.units_sold as total_ad_units_for_campaign_for_day,
round(acm.units_sold/number_of_asins_represented_by_campaign,100) AS assumed_ad_units_for_asin_for_day,
acm.impressions as total_impressions_for_campaign_for_day,
round(acm.impressions/number_of_asins_represented_by_campaign,100) AS assumed_impressions_for_asin_for_day,
acm.clicks as total_clicks_for_campaign_for_day,
round(acm.clicks/number_of_asins_represented_by_campaign,100) AS assumed_clicks_for_asin_for_day
from acm_with_brand acm
join spine s
    USING(brand,account_key,marketplace_key)
LEFT JOIN earned_gross_sales egs
    USING(account_key,marketplace_key,asin,date_day)

