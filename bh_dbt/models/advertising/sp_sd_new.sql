WITH product_metrics AS (
    SELECT 
        campaign_id,
        campaign_name,
        sponsored_type,
        account_key,
        marketplace_key,
        date_day,
        channel_product_id,
        costs AS incorrect_costs,
        SUM(costs) OVER (PARTITION BY campaign_id, account_key, marketplace_key, date_day) AS total_product_costs,
        sales as incorrect_sales,
        SUM(sales) OVER (PARTITION BY campaign_id, account_key, marketplace_key, date_day) AS total_ad_sales,
        orders,
        units_sold,
        clicks,
        impressions
    FROM 
        datahawk_share_83514.advertising.advertising_product_campaign_metrics
),
campaign_metrics AS (
    SELECT 
        campaign_id,
        campaign_name,
        sponsored_type,
        account_key,
        marketplace_key,
        date_day,
        costs AS correct_campaign_costs,
        sales as correct_campaign_sales
    FROM 
        datahawk_share_83514.advertising.advertising_campaign_metrics
),
adjusted_product_metrics AS (
    SELECT 
        p.campaign_id,
        p.campaign_name,
        p.sponsored_type,
        p.account_key,
        p.marketplace_key,
        p.date_day,
        p.channel_product_id,
        p.incorrect_costs,
        p.incorrect_sales,
        p.total_product_costs,
        c.correct_campaign_costs,
        CASE 
            WHEN p.total_product_costs = 0 THEN 0
            ELSE (p.incorrect_costs / p.total_product_costs) * c.correct_campaign_costs
        END AS adjusted_costs,
        c.correct_campaign_sales,
        CASE 
            WHEN p.total_ad_sales = 0 THEN 0
            ELSE (p.incorrect_sales / p.total_ad_sales) * c.correct_campaign_sales
        END AS adjusted_ad_sales,
        p.orders,
        p.units_sold,
        p.clicks,
        p.impressions
    FROM 
        product_metrics p
    JOIN 
        campaign_metrics c
    ON 
        p.campaign_id = c.campaign_id
        AND p.account_key = c.account_key
        AND p.marketplace_key = c.marketplace_key
        AND p.date_day = c.date_day
)
SELECT 
    account_key,
    marketplace_key,
    b.brand,
    b.category,
    channel_product_id as asin,
    campaign_id,
    campaign_name,
    sponsored_type,
    date_day,
    adjusted_costs AS ad_spend,
    adjusted_ad_sales as ad_sales,
    orders as ad_orders,
    units_sold as ad_units,
    clicks,
    impressions
FROM adjusted_product_metrics a
LEFT JOIN (SELECT distinct brand,internal_sku_category as category,channel_product_id from {{ref('product_pl_new')}}) b
    USING(channel_product_id)

