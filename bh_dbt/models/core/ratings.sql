select 
pl.sku
,pd.*   
from {{var('readable')['hawkspace']}}.product.product_detailed pd
left join {{var('readable')['hawkspace']}}.reports.report_product_latest_version p
    using(product_key)
left join (select distinct channel_product_id,sku from {{ref('product_pl')}}) pl
    on pl.channel_product_id = p.channel_product_id

