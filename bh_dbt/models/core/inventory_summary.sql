with last_30_day_unit_sales as (

    select 
    channel_product_id as asin,
    sum(units_sold) as units_sold_l30d
    from {{var('readable')}}.finance.finance_product_profit_loss pl
    where date_day between current_date()-31 and current_date()-1
    group by 1
)

select 
brand,ls.*,
units_sold_l30d,
ending_warehouse_balance/nullif((nullif(units_sold_l30d,0)/30),0) as days_supply
from {{var('readable')}}.raw_inventory.raw_inventory_ledger_summary ls
left join (select distinct channel_product_id as asin, brand from {{var('readable')}}.reports.report_product_latest_version) bs
    using(asin)
left join last_30_day_unit_sales
    using(asin)
qualify date = max(date) over (partition by 1=1)