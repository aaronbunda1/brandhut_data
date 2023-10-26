with event_data as (
    select 
    p.brand,
    e.event_date,
    o.purchase_date,
    e.order_id, 
    o.sku,
    p.name,
    e.sales_channel,
    o.fulfillment_channel,
    o.tax_collection_model,
    o.ship_city,
    o.ship_state,
    o.ship_postal_code,
    o.ship_country,
    e.metric,
    e.currency,
    sum(e.amount) as amount,
    sum(e.units_sold) as units_sold
    from {{var('readable')}}.finance.finance_events e
    left join {{var('readable')}}.finance.finance_orders_success o
        on o.order_id = e.order_id
    left join {{var('readable')}}.reports.report_product_latest_version p
        on p.product_key = o.product_key
    and o.purchase_date < date_trunc(month,current_date())
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)

select * from event_data