{{ config(materialized='table') }}

with campaign_asins as (
    select 
        distinct coalesce(channel_product_id, 'na') as channel_product_id,
        campaign_id
    from {{ source('dh_ads', 'advertising_product_campaign_metrics') }}
    where sponsored_type = 'SponsoredProducts'
      and costs > 0
),

campaign_monthly as (
    select 
        date_trunc(month, date_day) as date_day,
        campaign_id,
        marketplace_key,
        account_key,
        sponsored_type,
        sum(costs) as costs
    from {{ source('dh_ads', 'advertising_campaign_metrics') }}
    group by all
),

denominated_costs as (
    select 
        acm.*,
        cp.channel_product_id,
        count(distinct cp.channel_product_id) over (partition by acm.campaign_id) as denominator
    from campaign_monthly acm
    left join campaign_asins cp using(campaign_id)
),

prefinal as (
    select 
        ba.brand as brand,
        d.account_key,
        null as region,
        d.marketplace_key,
        date_trunc(month, d.date_day) as date_day,
        d.channel_product_id,
        null as sku,
        null as color,
        'USD' as currency_original,
        'USD' as currency_rate,
        1 as rate_to_usd,
        c.category as internal_sku_category,
        'SPONSORED_PRODUCTS_COST' as metric_name,
        current_date() as updated_at,
        -sum(round(d.costs / d.denominator, 2)) as amount
    from denominated_costs d
    left join {{ ref('category') }} c using(channel_product_id)
    left join (
        select distinct brand, channel_product_id
        from {{ ref('brand_asin') }}
    ) ba using(channel_product_id)
    where d.sponsored_type = 'SponsoredProducts'
      and d.costs > 0
    group by all
)

select 
    md5(concat_ws('|',
        coalesce(brand, ''),
        coalesce(account_key, ''),
        coalesce(marketplace_key, ''),
        coalesce(cast(date_day as varchar), ''),
        coalesce(channel_product_id, ''),
        coalesce(sku, ''),
        'SPONSORED_PRODUCTS_COST'
    )) as key,
    *
from prefinal
