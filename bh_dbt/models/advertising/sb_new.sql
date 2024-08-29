WITH daily_revenue_products AS (
    SELECT
        account_key,
        marketplace_key,
        brand,
        channel_product_id,
        date_day,
        SUM(amount) AS total_gross_sales
    FROM
        {{ref('product_pl_new')}}
    WHERE metric_name = 'EARNED_GROSS_SALES'
    GROUP BY
        account_key,
        marketplace_key,
        brand,
        channel_product_id,
        date_day
    HAVING
        SUM(amount) > 0
),
monthly_revenue_products AS (
    SELECT
        account_key,
        marketplace_key,
        brand,
        channel_product_id,
        TO_CHAR(date_day, 'YYYY-MM') AS month,
        SUM(amount) AS total_gross_sales
    FROM
        {{ref('product_pl_new')}}
    GROUP BY
        account_key,
        marketplace_key,
        brand,
        channel_product_id,
        TO_CHAR(date_day, 'YYYY-MM')
    HAVING
        SUM(amount) > 0
),
sponsored_brands_campaigns AS (
    SELECT
        campaign_id,
        account_key,
        marketplace_key,
        campaign_name,
        {{get_brand_from_sku('campaign_name')}} as brand,
        sponsored_type,
        date_day,
        costs AS daily_costs,
        sales as daily_sales,
        orders as daily_orders,
        units_sold as daily_units,
        clicks as daily_clicks,
        impressions as daily_impressions
    FROM
        datahawk_share_83514.advertising.advertising_campaign_metrics
    WHERE
        sponsored_type = 'SponsoredBrands'
),
daily_distribution AS (
    SELECT
        s.account_key,
        s.marketplace_key,
        b.brand,
        b.category,
        d.channel_product_id AS asin,
        s.campaign_id,
        s.campaign_name,
        s.sponsored_type,
        s.date_day,
        s.daily_costs / COUNT(d.channel_product_id) OVER (PARTITION BY s.brand,s.campaign_id, s.account_key, s.marketplace_key, s.date_day) AS distributed_costs,
        s.daily_sales / COUNT(d.channel_product_id) OVER (PARTITION BY s.brand,s.campaign_id, s.account_key, s.marketplace_key, s.date_day) AS distributed_sales,
        s.daily_orders / COUNT(d.channel_product_id) OVER (PARTITION BY s.brand,s.campaign_id, s.account_key, s.marketplace_key, s.date_day) AS distributed_orders,
        s.daily_units / COUNT(d.channel_product_id) OVER (PARTITION BY s.brand,s.campaign_id, s.account_key, s.marketplace_key, s.date_day) AS distributed_units,
        s.daily_clicks / COUNT(d.channel_product_id) OVER (PARTITION BY s.brand,s.campaign_id, s.account_key, s.marketplace_key, s.date_day) AS distributed_clicks,
        s.daily_impressions / COUNT(d.channel_product_id) OVER (PARTITION BY s.brand,s.campaign_id, s.account_key, s.marketplace_key, s.date_day) AS distributed_impressions
 
 
 
    FROM
        sponsored_brands_campaigns s
    JOIN
        daily_revenue_products d
    ON
        s.account_key = d.account_key
        AND s.marketplace_key = d.marketplace_key
        and s.brand = d.brand
        AND s.date_day = d.date_day
    LEFT JOIN
        (SELECT DISTINCT brand, internal_sku_category AS category, channel_product_id FROM {{ref('product_pl_new')}}) b
    USING(channel_product_id)
),
monthly_distribution AS (
    SELECT
        s.account_key,
        s.marketplace_key,
        b.brand,
        b.category,
        m.channel_product_id AS asin,
        s.campaign_id,
        s.campaign_name,
        s.sponsored_type,
        s.date_day,
        s.daily_costs / COUNT(m.channel_product_id) OVER (PARTITION BY s.brand,s.campaign_id, s.account_key, s.marketplace_key, TO_CHAR(s.date_day, 'YYYY-MM')) AS distributed_costs,
        s.daily_sales / COUNT(m.channel_product_id) OVER (PARTITION BY s.brand,s.campaign_id, s.account_key, s.marketplace_key, TO_CHAR(s.date_day, 'YYYY-MM')) AS distributed_sales,
        s.daily_orders / COUNT(m.channel_product_id) OVER (PARTITION BY s.brand,s.campaign_id, s.account_key, s.marketplace_key, TO_CHAR(s.date_day, 'YYYY-MM')) AS distributed_orders,
        s.daily_units / COUNT(m.channel_product_id) OVER (PARTITION BY s.brand,s.campaign_id, s.account_key, s.marketplace_key, TO_CHAR(s.date_day, 'YYYY-MM')) AS distributed_units,
        s.daily_clicks / COUNT(m.channel_product_id) OVER (PARTITION BY s.brand,s.campaign_id, s.account_key, s.marketplace_key, TO_CHAR(s.date_day, 'YYYY-MM')) AS distributed_clicks,
        s.daily_impressions / COUNT(m.channel_product_id) OVER (PARTITION BY s.brand,s.campaign_id, s.account_key, s.marketplace_key, TO_CHAR(s.date_day, 'YYYY-MM')) AS distributed_impressions
    FROM
        sponsored_brands_campaigns s
    JOIN
        monthly_revenue_products m
    ON
        s.account_key = m.account_key
        AND s.marketplace_key = m.marketplace_key
        AND TO_CHAR(s.date_day, 'YYYY-MM') = m.month
        and s.brand = m.brand
    LEFT JOIN
        (SELECT DISTINCT brand, internal_sku_category AS category, channel_product_id FROM {{ref('product_pl_new')}}) b
    USING(channel_product_id)
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM daily_revenue_products d
            WHERE s.account_key = d.account_key
              AND s.marketplace_key = d.marketplace_key
              AND s.date_day = d.date_day
              and s.brand=d.brand
        )
)
-- Final result with the specified fields and order
    SELECT 
        account_key,
        marketplace_key,
        brand,
        category,
        asin,
        campaign_id,
        campaign_name,
        sponsored_type,
        date_day,
        distributed_costs as ad_spend,
        distributed_sales as ad_sales,
        distributed_orders as ad_orders,
        distributed_units as ad_units,
        distributed_orders as clicks,
        distributed_impressions as impressions
    FROM 
        daily_distribution
    UNION ALL
    SELECT 
        account_key,
        marketplace_key,
        brand,
        category,
        asin,
        campaign_id,
        campaign_name,
        sponsored_type,
        date_day,
        distributed_costs as ad_spend,
        distributed_sales as ad_sales,
        distributed_orders as ad_orders,
        distributed_units as ad_units,
        distributed_orders as clicks,
        distributed_impressions as impressions
    FROM 
        monthly_distribution
