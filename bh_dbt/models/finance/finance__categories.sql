select 
distinct asin as channel_product_id, 
category,
last_updated::datetime as creation_date
from {{ref('manual_product_categories')}}
left join (
select distinct sku, asin 
from  {{ref('stg_finance__ledger_pivoted')}} 
where asin is not null 
) using(sku)
where not category ilike any ('Other%','Brandhut-managed','SPOT','Zens Legacy')
QUALIFY row_number() over (partition by channel_product_id order by creation_date) = 1
