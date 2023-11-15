select 
pl.sku
, c.category as internal_sku_category
,pd.*   
from {{var('readable')['hawkspace']}}.product.product_detailed pd
left join {{var('readable')['hawkspace']}}.reports.report_product_latest_version p
    using(product_key)
left join (select distinct channel_product_id,sku from {{ref('product_pl')}}) pl
    on pl.channel_product_id = p.channel_product_id
left join {{ref('brand_asin')}} b
    on b.channel_product_id = pd.channel_product_id
left join {{ref('category')}} c
    on c.channel_product_id = pd.channel_product_id
-- select the row per channel_product_id with the maximum observation_date within its month
qualify rank() over (partition by pd.channel_product_id, date_trunc(month,observation_date) order by observation_date desc) =1
