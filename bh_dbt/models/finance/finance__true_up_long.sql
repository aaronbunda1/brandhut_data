WITH pl_brand_month as (
        select report_month,
        brand,
        sum(amount) as pl
        from {{ref('finance__unified_actuals_long')}} f
        where metric_group_1 in ('Net Sales','Expenses')
        group by all
    )


, data_movements as (
    select
    md5(concat(i.brand,i.month,'DATA_MOVEMENTS')) as key,
            p.BRAND,
            null as ACCOUNT_KEY,
            null as REGION,
            null as MARKETPLACE_KEY,
            p.report_month,
            null as CHANNEL_PRODUCT_ID,
            null as SKU,
            'USD' as original_currency,
            null as internal_sku_category,
            'DATA_MOVEMENTS' as metric_name,
            i.invoice_amount - p.pl as amount,
            'Expenses' as metric_group_1,
            'Other Expenses' as metric_group_2
    from pl_brand_month p
    left join {{ref('invoice_amounts')}} i
        on i.month = p.report_month
        and i.brand = p.brand
    where i.brand is not null 
    and p.report_month >= '2024-01-01'
)

, final_without_true_up_with_data_movements as (
select * 
from {{ref('finance__unified_actuals_long')}}
union all
select * 
from data_movements
)


, pl_brand_month_with_data_movements as (
    select report_month,
    brand,
    sum(amount) as pl
    from final_without_true_up_with_data_movements f
    where metric_group_1 in ('Net Sales','Expenses') and metric_name NOT IN ('EARNED_GROSS_SALES','EARNED_BRANDHUT_COMMISSION','DATA_MOVEMENTS','TRUE_UP_INVOICED','MANUAL_UNALLOCATED_COSTS')
    group by all
)

, true_up_live as (
    select
    md5(concat(i.brand,i.month,'TRUE_UP')) as key,
            p.BRAND,
            null as ACCOUNT_KEY,
            null as REGION,
            null as MARKETPLACE_KEY,
            dateadd(month,1,max(i.month) over (partition by i.brand)) as report_month,
            null as CHANNEL_PRODUCT_ID,
            null as SKU,
            'USD' as original_currency,
            null as internal_sku_category,
            'TRUE_UP_LIVE_CALCULATED' as metric_name,
            -(i.invoice_amount - p.pl) as amount,
            'Expenses' as metric_group_1,
            'Other Expenses' as metric_group_2
    from pl_brand_month_with_data_movements p
    left join {{ref('invoice_amounts')}} i
        on i.month = p.report_month
        and i.brand = p.brand
    where i.brand is not null 
    and p.report_month >= '2024-01-01'
    and p.report_month < date_trunc(month,current_date())

)

SELECT * FROM true_up_live
UNION
SELECT * FROM data_movements