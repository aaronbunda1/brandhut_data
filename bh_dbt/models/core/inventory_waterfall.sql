
with inventory_data as (
select 
a.account_key,
a.region_seller_name,
date_trunc(month,a.date) as month,
a.asin,
b.brand,
c.category,
a.msku,
a.disposition,
sum(case when a.date = a.starting_balance_date then a.starting_warehouse_balance end) as starting_warehouse_balance,
sum(a.receipts) as receipts,
sum(a.customer_shipments) as customer_shipments,
sum(a.vendor_returns) as vendor_returns,
sum(a.warehouse_transfer_in_out) as warehouse_transfer_in_out,
sum(a.found) as found,
sum(a.lost) as lost,
sum(a.damaged) as damaged,
sum(a.disposed) as disposed,
sum(a.other_events) as other,
sum(a.unknown_events) as unknown
from 
(select *, min(date) over (partition by date_trunc(month,date),asin,disposition) as starting_balance_date,
max(date) over (partition by date_trunc(month,date),asin,disposition) as ending_balance_date 
from datahawk_share_83514.raw_inventory.raw_inventory_ledger_summary) a
left join (select distinct brand, channel_product_id from datahawk_writable_83514.brandhut.brand_asin) b 
    on b.channel_product_id = a.asin
left join {{ref('category')}} c
    on c.channel_product_id = a.asin
group by all)

select 
*
from inventory_data 
unpivot(amount for movement_type in (STARTING_WAREHOUSE_BALANCE,RECEIPTS,CuSTOMER_SHIPMENTS,VENDOR_RETURNS,WAREHOUSE_TRANSFER_IN_OUT,FOUND,LOST,DAMAGED,DISPOSED,OTHER,UNKNOWN))