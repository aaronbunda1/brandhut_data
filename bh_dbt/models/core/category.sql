with onanoff as (
select distinct product_key
from {{var('readable')}}.REFERENTIAL.REFERENTIAL_PROJECT
where project_name IN ('Onanoff','Spot','Cellini')
)

, datahawk_categories as (select distinct
p.brand, p.channel_product_id, project_name as category
from {{var('readable')}}.REFERENTIAL.REFERENTIAL_PROJECT
join onanoff using(product_key)
join {{var('readable')}}.reports.report_product_latest_version p using(product_key) 
where NOT project_name IN ('Onanoff','Spot','Cellini')

)

, with_manual as (select *
from datahawk_categories

union all

select distinct brand, channel_product_id,category
from  {{ref('manual_product_categories')}} 
join (select distinct sku,channel_product_id from {{var('readable')}}.FINANCE.finance_product_profit_loss)
using(sku)
)

select * from with_manual

