{{config(materialized='table')}}

with p as (
    select
    dbt_updated_at,
    metric_group_1,
    metric_group_2,
    metric_name,
    brand,
    internal_sku_category,
    channel_product_id,
    sku,
    date_day as month,
    sum(amount) as amount
    from (
        select * from datahawk_writable_83514.snapshots.product_pl_new_snapshot
        qualify rank() over (partition by brand,account_key,region,marketplace_key,date_day,channel_product_id,sku,currency_original, date(dbt_updated_at) order by dbt_updated_at desc) =1
        ) 
    where date_day >= '2024-01-01'
    and dbt_updated_at >=  dateadd(month,1,date_day)
    group by all
)

select * from p

;
,datespine as (
    select 
        d.date as dbt_updated_at_p1,
        d2.date as dbt_updated_at_p2
    from (select distinct date(dbt_updated_at) date from p order by 1) d
    left join (select distinct date(dbt_updated_at) date from p order by 1) d2

)

,final as (
select 
coalesce(p1.metric_group_1,p2.metric_group_1) as metric_group_1,
coalesce(p1.metric_group_2,p2.metric_group_2) as metric_group_2,
coalesce(p1.metric_name,p2.metric_name) as metric_name,
coalesce(p1.brand,p2.brand) as brand,
coalesce(p1.month,p2.month) as month,
ds.dbt_updated_at_p1,
ds.dbt_updated_at_p2,
coalesce(p1.amount,0) as amount_p1,
coalesce(p2.amount,0) as amount_p2,
amount_p2-amount_p1 as delta
from datespine ds
left join p p1
    on p1.dbt_updated_at::DATE = ds.dbt_updated_at_p1
left join p p2
    on p1.dbt_updated_at::DATE = ds.dbt_updated_at_p2
order by 
    ds.dbt_updated_at_p1,
    ds.dbt_updated_at_p2
)

select * from final 