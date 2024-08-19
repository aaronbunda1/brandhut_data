SELECT 
    COALESCE(ad.account_key, a.account_key) AS account_key,
    COALESCE(ad.marketplace_key, a.marketplace_key) AS marketplace_key,
    COALESCE(ad.brand, a.brand) AS brand,
    COALESCE(ad.category, a.category) AS category,
    COALESCE(ad.ASIN, a.asin) AS asin,
    COALESCE(ad.date_day, a.date_day) AS date_day,
    ad.campaign_id,
    ad.campaign_name,
    ad.sponsored_type,
    ad.number_of_asins_represented_by_campaign,
    ad.total_ad_spend_for_campaign_for_day,
    ad.assumed_ad_spend_for_asin_for_day,
    ad.total_ad_sales_for_campaign_for_day,
    ad.assumed_ad_sales_for_asin_for_day,
    ad.earned_gross_sales_for_asin_for_day,
    ad.total_ad_orders_for_campaign_for_day,
    ad.assumed_ad_orders_for_asin_for_day,
    ad.total_ad_units_for_campaign_for_day,
    ad.assumed_ad_units_for_asin_for_day,
    ad.total_impressions_for_campaign_for_day,
    ad.assumed_impressions_for_asin_for_day,
    ad.total_clicks_for_campaign_for_day,
    ad.assumed_clicks_for_asin_for_day,
    a.sessions,
    a.page_views,
FROM (
    select * from {{ref('sb')}}
    union all
    select * from {{ref('sp_sd_detail')}}
) ad
FULL OUTER JOIN activities a
    ON ad.account_key = a.account_key 
    AND ad.marketplace_key = a.marketplace_key
    AND ad.asin = a.asin
    AND ad.date_day = a.date_day