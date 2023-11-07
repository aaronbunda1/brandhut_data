{{config(materialized='table')}}
--actuals
select
brand,
internal_sku_category,
sku, 
year(date_day)::string as period,
month(date_day)::string as sub_period,
sum(units_sold) as units_sold
from {{ref('product_pl_daily')}}
group by 1,2,3,4,5

union all 

select
brand,
internal_sku_category,
sku, 
'L30D' as period,
NULL as sub_period,
sum(units_sold)/(30/365) as units_sold
from {{ref('product_pl_daily')}}
where date_day between current_date()-31 and current_date()-1
group by 1,2,3,4,5

union all 

select
brand,
internal_sku_category,
sku, 
'Previous 30D' as period,
NULL as sub_period,
sum(units_sold)/(30/365) as units_sold
from {{ref('product_pl_daily')}}
where date_day between current_date()-62 and current_date()-32
group by 1,2,3,4,5

union all 

select
brand,
internal_sku_category,
sku, 
'L90D' as period,
NULL as sub_period,
sum(units_sold)/(90/365) as units_sold
from {{ref('product_pl_daily')}}
where date_day between current_date()-91 and current_date()-1
group by 1,2,3,4,5

union all 

select
brand,
internal_sku_category,
sku, 
'Previous 90D' as period,
NULL as sub_period,
sum(units_sold)/(90/365) as units_sold
from {{ref('product_pl_daily')}}
where date_day between current_date()-122 and current_date()-92
group by 1,2,3,4,5

union all 

select
brand,
internal_sku_category,
sku, 
'LY 90D' as period,
NULL as sub_period,
sum(units_sold)/(90/365) as units_sold
from {{ref('product_pl_daily')}}
where date_day between current_date()-91-365 and current_date()-1-365
group by 1,2,3,4,5

union all 

select
brand,
internal_sku_category,
sku, 
'YTD Run Rate' as period,
NULL as sub_period,
sum(units_sold)/(datediff(day,date_trunc(year,current_date()),current_date()-1)/365) as units_sold
from {{ref('product_pl_daily')}}
where date_day between date_trunc(year,current_date()) and current_date()-1
group by 1,2,3,4,5


union all 

select
brand,
internal_sku_category,
sku, 
'Prior Year' as period,
NULL as sub_period,
sum(units_sold) as units_sold
from {{ref('product_pl_daily')}}
where date_trunc(year,date_day) = date_trunc(year,date_day-365-1)
group by 1,2,3,4,5





