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
    coalesce(b.brand,{{get_brand_from_sku('sku')}}) as brand,
    -- fnsku as asin, 
    ls.asin,
    sku,
    date as as_of_date,
    sum(ending_warehouse_balance) as ending_warehouse_balance
    from (
        select *, 
        min(msku) over (partition by fnsku) as sku
        from {{var('readable')['hawkspace']}}.raw_inventory.raw_inventory_ledger_summary) ls
    left join (select distinct channel_product_id as asin, brand from {{ref('brand_asin')}}) b
        on b.asin = ls.asin 
    where disposition = 'SELLABLE'
    group by 1,2,3,4
)

select 
inventory_current.*,
units_sold_l30d,
coalesce(c.category, case 
    when sku ilike any ('%SS-%','%STSH-%','%shield%') then 'Storyshield' 
    when inventory_current.brand = 'Storyphones' then 'Storyphones' 
    when inventory_current.brand = 'ZENS' then 'ZENS Legacy' 
    end) as internal_sku_category
from inventory_current
left join last_30_day_unit_sales
    using(asin)
left join {{ref('category')}} c
    on c.channel_product_id = inventory_current.asin
where ending_warehouse_balance > 0
qualify rank() over (partition by asin order by as_of_date desc) =1