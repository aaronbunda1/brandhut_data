select 
channel_product_id,
project_name,
replace(project_name,' (from projects)','') as category,
creation_date
from datahawk_share_83514.REFERENTIAL.REFERENTIAL_PROJECT
where channel_product_id is not null and project_name not IN  
('Onanoff (from projects)','BuddyPhones','Cellini (from projects)','Zens','Zens Other','cellini','Onanoff','Spot','spot.','Storyphones','Fokus','Pablo Artists'' Choice',
'Zens MagSafe')


union all

select 
distinct asin as channel_product_id, 
category as project_name,
category,
last_updated::datetime as creation_date
from datahawk_writable_83514.brandhut.manual_product_categories
left join (
select distinct sku, asin 
from datahawk_share_83514.custom_83514.finance_profit_ledger l 
where asin is not null
) using(sku)

order by channel_product_id,creation_date