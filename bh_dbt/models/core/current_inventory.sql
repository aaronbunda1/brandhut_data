SELECT 
COALESCE(brand,{{get_brand_from_sku('sku')}}) as brand,
category,
a.*
FROM datahawk_share_83514.raw_inventory.raw_inventory_fba_history a
LEFT JOIN datahawk_writable_83514.brandhut.brand_asin b ON a.asin = b.channel_product_id 
LEFT JOIN datahawk_writable_83514.brandhut.category c ON a.asin = c.channel_product_id 
QUALIFY ROW_NUMBER() OVER (partition by marketplace_key,sku,asin,account_key order by observation_time DESC) =1
