with campaign_metrics as (
    select 
        {{ fuzzy_match_brand('acm.campaign_name') }} as brand,
        account_key,
        marketplace_key,
        date_trunc(month, date_day) as date_day,
        sum(case when sponsored_type = 'SponsoredBrands' then costs else 0 end) as sponsored_brands_cost,
        sum(case when sponsored_type = 'SponsoredDisplay' then costs else 0 end) as sponsored_display_cost
    from {{ source('dh_ads', 'advertising_campaign_metrics') }} acm
    where account_key in ('A6XVUKTIG2TMF','A2QZG9U5PYAA59')
      and marketplace_key is not null
    group by all
),

unpivoted as (
    select 
        md5(concat_ws('|',
            coalesce(brand, ''),
            coalesce(account_key, ''),
            coalesce(marketplace_key, ''),
            coalesce(cast(date_day as varchar), ''),
            '', '',  -- channel_product_id, sku
            metric_name,
            'USD'
        )) as key,
        brand,
        account_key,
        null as region,
        marketplace_key,
        date_day,
        null as channel_product_id,
        null as sku,
        null as color,
        'USD' as currency_original,
        'USD' as currency_rate,
        1 as rate_to_usd,
        null as internal_sku_category,
        metric_name,
        current_date() as updated_at,
        amount
    from campaign_metrics
    unpivot (amount for metric_name in (
        sponsored_brands_cost,
        sponsored_display_cost
    ))
)

select * from unpivoted
