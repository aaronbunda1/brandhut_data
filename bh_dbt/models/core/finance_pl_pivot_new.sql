select 
l.*,
b.brand
from datahawk_share_83514.custom_83514.finance_product_profit l
left join {{ref('brand_asin')}} b
    on b.channel_product_id = l.asin
    and b.marketplace_key = l.marketplace_key