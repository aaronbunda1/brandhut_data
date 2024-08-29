SELECT 
    COALESCE(COALESCE(ad.account_key, a.account_key),pl.account_key) AS account_key,
    COALESCE(COALESCE(ad.marketplace_key, a.marketplace_key),pl.marketplace_key) AS marketplace_key,
    COALESCE(COALESCE(COALESCE(ad.brand, a.brand),pl.brand),{{get_brand_from_sku('ad.brand')}}) AS brand,
    COALESCE(COALESCE(ad.category, a.category),pl.category) AS category,
    COALESCE(COALESCE(ad.ASIN, a.asin),pl.asin) AS asin,
    COALESCE(COALESCE(ad.date_day, a.date_day),pl.date_day) AS date_day,
    ad.campaign_id,
    ad.campaign_name,
    ad.sponsored_type,
    ad.ad_spend,
    ad.ad_sales,
    ad.ad_orders,
    ad.ad_units,
    ad.clicks,
    ad.impressions,
    a.sessions,
    a.page_views,
    pl.earned_gross_sales
FROM (
    select * from {{ref('sb_new')}}
    union all
    select * from {{ref('sp_sd_new')}}
) ad
FULL OUTER JOIN {{ref('activities')}} a
    ON ad.account_key = a.account_key 
    AND ad.marketplace_key = a.marketplace_key
    AND ad.asin = a.asin
    AND ad.date_day = a.date_day
    and ad.brand is null
FULL OUTER JOIN (
    select account_key,
    marketplace_key,
    channel_product_id as asin,
    internal_sku_category as category,
    brand,
     date_day, 
     sum(amount) as earned_gross_sales
    from {{ref('product_pl_new')}} 
    where metric_name = 'EARNED_GROSS_SALES'
    group by all) pl  
        ON ad.account_key = pl.account_key 
        AND ad.marketplace_key = pl.marketplace_key
        AND ad.asin = pl.asin
        AND ad.date_day = pl.date_day
        AND pl.brand IS NULL