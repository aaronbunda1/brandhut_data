--check that ad spend matches
with acm_agg as (
select 
account_key,
marketplace_key,
date_day,
sponsored_type,
sum(costs) as ad_spend_acm
from datahawk_share_83514.advertising.advertising_campaign_metrics acm
group by all
)

,ad_agg as (
select 
account_key,
marketplace_key,
date_day,
sponsored_type,
sum(ad_spend) as ad_spend_ad_report
from {{ref('ad_report_new')}} ad
group by all
)

select 
*
, coalesce(ad_spend_ad_report,0)-coalesce(ad_spend_acm,0) as diff
from ad_agg
full outer join acm_agg 
    using(account_key,marketplace_key,date_day,sponsored_type)
where abs(diff) >1