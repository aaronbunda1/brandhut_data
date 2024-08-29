
select 
account_key,
marketplace_key,
brand,
category,
asin,
report_date as date_day,
browser_page_views,
browser_sessions,
mobile_app_page_views,
mobile_app_sessions,
page_views,
sessions
from datahawk_share_83514.preview_raw_seller.raw_traffic_by_asin t
left join (select distinct brand, channel_product_id as asin, internal_sku_category as category from datahawk_writable_83514.brandhut.product_pl_new ) b using(asin)