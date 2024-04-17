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
