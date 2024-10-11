with asins as (
select distinct COALESCE(channel_product_id,'na') as channel_product_id,campaign_id
from datahawk_share_83514.advertising.advertising_product_campaign_metrics
where sponsored_type = 'SponsoredProducts'
and costs>0
)

, prefinal as (select 
account_key,
b.brand as brand,
NULL as region,
marketplace_key,
date_trunc(month,acm.date_day) as date_day,
channel_product_id,
NULL as sku,
NULL as color,
'USD' as currency_original,
'USD' as currency_rate,
1 as rate_to_usd,
c.category as internal_sku_category,
'SPONSORED_PRODUCTS_COST' as metric_name,
current_date() as updated_at,
-sum(round(costs/denominator,2)) as amount
from (
    select acm.*,
    channel_product_id,
    count(distinct channel_product_id) over (partition by acm.campaign_id) as denominator
    from (
        select date_trunc(month,date_day) as date_day,
        campaign_id,
        marketplace_key,
        account_key,
        sponsored_type,
        sum(costs) as costs
        from datahawk_share_83514.advertising.advertising_campaign_metrics
        group by all
        ) acm
    left join asins using(campaign_id)
) acm
left join {{ref('category')}} c using(channel_product_id)
left join (select distinct brand, channel_product_id from {{ref('brand_asin')}}) b using(channel_product_id)
where sponsored_type = 'SponsoredProducts'
and costs >0
GROUP BY ALL
)

select 
concat(
    coalesce(brand,''),
    coalesce(account_key,''),
    coalesce(marketplace_key,''),
    coalesce(date_day,''),
    coalesce(channel_product_id,''),
    coalesce(sku,''),
    'SPONSORED_PRODUCTS_COST'
) as key,
* 
from prefinal