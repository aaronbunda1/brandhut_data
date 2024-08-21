
--check on revenue being correct

WITH earned_gross_sales_truth as (
SELECT 
account_key,marketplace_key,date_trunc(month,date_day) as month,channel_product_id,sum(earned_gross_sales) as earned_gross_sales
FROM datahawk_share_83514.finance.finance_product_profit_loss
group by all
)

, ad_report_agg AS (
SELECT
account_key,marketplace_key,date_trunc(month,date_day) as month,
asin as channel_product_id,
sum(a.earned_gross_sales_for_asin_for_day) as earned_gross_sales_ad_report,
FROM {{ref('ad_report_complete')}} a
group by all
)

select 
month,
sum(earned_gross_sales_ad_report) as earned_gross_sales_ad_report,
sum(earned_gross_sales) as earned_gross_sales_pl,
sum(coalesce(earned_gross_sales,0)-coalesce(earned_gross_sales_ad_report,0)) as diff
from ad_report_agg a 
left join earned_gross_sales_truth pl
    USING(account_key,marketplace_key,month,channel_product_id)
group by month
having diff!=0
order by month desc;

select * 
from  datahawk_writable_83514.dev_brandhut_advertising.ad_report_complete