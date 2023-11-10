with last_30_day_unit_sales as (

    select 
    channel_product_id as asin,
    sum(units_sold) as units_sold_l30d
    from {{var('readable')['hawkspace']}}.finance.finance_product_profit_loss pl
    where date_day between current_date()-31 and current_date()-1
    group by 1
)

, inventory_current as (
    select 
    b.brand,
    fnsku as asin, 
    sku,
    date as as_of_date,
    sum(ending_warehouse_balance) as ending_warehouse_balance
    from (
        select *, 
        min(msku) over (partition by fnsku) as sku
        from {{var('readable')['hawkspace']}}.raw_inventory.raw_inventory_ledger_summary) ls
    left join (select distinct channel_product_id as asin, brand from {{ref('brand_asin')}}) b
        on b.asin = ls.fnsku 
    where disposition = 'SELLABLE'
    group by 1,2,3,4
)

select 
inventory_current.*,
units_sold_l30d
from inventory_current
left join last_30_day_unit_sales
    using(asin)
qualify rank() over (partition by asin order by as_of_date desc) =1