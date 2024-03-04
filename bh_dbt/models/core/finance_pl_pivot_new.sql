select 
l.*,
b.brand
from {{ref('finance_product_profit_dh')}} l
left join {{ref('brand_asin')}} b
    on b.channel_product_id = l.asin
    and b.marketplace_key = l.marketplace_key