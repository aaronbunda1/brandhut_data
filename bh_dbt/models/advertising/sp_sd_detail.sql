WITH earned_gross_sales AS (
    select 
    account_key,
    marketplace_key,
    date_day,
    channel_product_id,
    sum(earned_gross_sales) as earned_gross_sales
    from datahawk_share_83514.finance.finance_product_profit_loss pl
    group by all
)

select 
acm.account_key,
acm.marketplace_key,
b.brand,
b.category,
cp.channel_product_id as ASIN,
acm.campaign_id,
acm.campaign_name,
acm.sponsored_type,
acm.date_day,
count(distinct cp.channel_product_id) OVER (partition by campaign_id) AS number_of_asins_represented_by_campaign,
acm.costs as total_ad_spend_for_campaign_for_day,
round(acm.costs/number_of_asins_represented_by_campaign,100) AS assumed_ad_spend_for_asin_for_day,
acm.sales as total_ad_sales_for_campaign_for_day,
case 
    when sum(egs.earned_gross_sales) OVER (partition by date_day,campaign_id) >0 
    then round(acm.sales*(egs.earned_gross_sales/sum(egs.earned_gross_sales) OVER (partition by date_day,campaign_id)),100)
    else round(acm.sales/number_of_asins_represented_by_campaign,100)
    end AS assumed_ad_sales_for_asin_for_day,
nullif(round(egs.earned_gross_sales,100)/count(*) over (partition by date_day,channel_product_id)) as earned_gross_sales_for_asin_for_day,
acm.orders as total_ad_orders_for_campaign_for_day,
round(acm.orders/number_of_asins_represented_by_campaign,100) AS assumed_ad_orders_for_asin_for_day,
acm.units_sold as total_ad_units_for_campaign_for_day,
round(acm.units_sold/number_of_asins_represented_by_campaign,100) AS assumed_ad_units_for_asin_for_day,
acm.impressions as total_impressions_for_campaign_for_day,
round(acm.impressions/number_of_asins_represented_by_campaign,100) AS assumed_impressions_for_asin_for_day,
acm.clicks as total_clicks_for_campaign_for_day,
round(acm.clicks/number_of_asins_represented_by_campaign,100) AS assumed_clicks_for_asin_for_day
from datahawk_share_83514.advertising.advertising_campaign_metrics acm
join {{ref('campaign_to_product')}} cp
    USING(account_key,marketplace_key,campaign_name)
LEFT JOIN (SELECT distinct brand,internal_sku_category as category,channel_product_id from {{ref('product_pl_new')}}) b
    USING(channel_product_id)
LEFT JOIN earned_gross_sales egs
    USING(account_key,marketplace_key,channel_product_id,date_day)