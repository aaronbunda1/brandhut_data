select 
channel_product_id,
project_name,
replace(project_name,' (from projects)','') as category,
creation_date
from datahawk_share_83514.REFERENTIAL.REFERENTIAL_PROJECT
where channel_product_id is not null and project_name not IN  
('Onanoff (from projects)','BuddyPhones','Cellini (from projects)','Zens','Zens Other','cellini','Onanoff','Spot','spot.','Storyphones','Fokus','Pablo Artists'' Choice')
order by channel_product_id,creation_date