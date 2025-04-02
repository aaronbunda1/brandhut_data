select 
{{get_brand_from_sku('acm.campaign_name')}} as brand,
account_key,
marketplace_key,
date_trunc(month,date_day) as date_day,
sum(case when sponsored_type = 'SponsoredBrands' then costs else 0 end) as sponsored_brands_cost,
sum(case when sponsored_type = 'SponsoredDisplay' then costs else 0 end) as sponsored_display_cost
from datahawk_share_83514.advertising.advertising_campaign_metrics acm
where account_key  IN ('A6XVUKTIG2TMF','A2QZG9U5PYAA59')
and marketplace_key IS NOT NULL 
group by all

