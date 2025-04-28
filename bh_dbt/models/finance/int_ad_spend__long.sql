
select
    ad_data.key,
    ad_data.brand,
    ad_data.account_key,
    ad_data.region,
    ad_data.marketplace_key,
    ad_data.date_day,
    ad_data.channel_product_id,
    ad_data.sku,
    CASE 
        WHEN ad_data.marketplace_key = 'Amazon-GB' THEN 'GBP'
        WHEN ad_data.marketplace_key = 'Amazon-US' THEN 'USD'
        WHEN ad_data.marketplace_key = 'Amazon-CA' THEN 'CAD' 
    END AS currency_original,
    ad_data.internal_sku_category,
    ad_data.metric_name,
    ad_data.amount / r.rate AS amount
from 
    (
        select * from {{ref('stg_ad_spend__sponsored_products')}}
        UNION ALL 
        select * from {{ref('stg_ad_spend__sponsored_brands_and_display')}}
    ) ad_data
left join {{source('dh_referential','referential_currency_rate')}} r
    ON ad_data.date_day = r.date_day
    AND r.base = 'USD'
    AND r.currency = 
        CASE 
            WHEN ad_data.marketplace_key = 'Amazon-GB' THEN 'GBP'
            WHEN ad_data.marketplace_key = 'Amazon-US' THEN 'USD'
            WHEN ad_data.marketplace_key = 'Amazon-CA' THEN 'CAD' 
        END

