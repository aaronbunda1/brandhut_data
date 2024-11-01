with asins as (
select distinct COALESCE(channel_product_id,'na') as channel_product_id,campaign_id
from datahawk_share_83514.advertising.advertising_product_campaign_metrics
where sponsored_type = 'SponsoredProducts'
and costs>0
)

, prefinal as (
select 
b.brand as brand,
account_key,
marketplace_key,
acm.date_day,
channel_product_id,
campaign_id,
c.category as internal_sku_category,
costs,
denominator,
sum(costs/denominator) as sponsored_products_cost
from (
    select acm.*,
    channel_product_id,
    count(distinct channel_product_id) over (partition by acm.campaign_id) as denominator
    from (
        select 
        date_day,
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
left join datahawk_writable_83514.brandhut.category c using(channel_product_id)
left join (select distinct brand, channel_product_id from datahawk_writable_83514.brandhut.brand_asin) b using(channel_product_id)
where sponsored_type = 'SponsoredProducts'
and costs >0
GROUP BY ALL
)

select * 
from prefinal