with cte as (
    SELECT
    DATE_TRUNC(MONTH,date_day) AS pl_month,
    brand,
    sum(gross_sales) as gross_sales,
    sum(COGS) as cogs,
    sum(SPONSORED_PRODUCTS_COST)
    +sum(DIST_SPONSORED_BRANDS_COST)
    +sum(DIST_SPONSORED_BRANDS_VIDEO_COST)
    +sum(DIST_SPONSORED_DISPLAY_COST)
    as ad_spend,
    sum(SHIPPING)
    +sum(SHIPPING_PROMOTION)
    +sum(SHIPPING_CHARGEBACK)
    +sum(freight)
    as shipping_costs,
    sum(REIMBURSED_PRODUCT) 
    +sum(REVERSAL_REIMBURSED)
    as REIMBURSED_PRODUCT,
    sum(REFUND_COMMISSION)
    +sum(REFUNDED_REFERRAL_FEES)
    +sum(DIST_INBOUND_TRANSPORTATION)
    +sum(DIST_FBA_STORAGE_FEE)
    +sum(DIST_FBA_INVENTORY_PLACEMENT_SERVICE)
    +sum(WAREHOUSE_DAMAGE)
    +sum(WAREHOUSE_LOST_MANUAL)
    +sum(FBA_PER_UNIT_FULFILMENT_FEE)
    +sum(DIST_DISPOSAL_COMPLETE)
    +sum(DIST_REMOVAL_COMPLETE)
    +sum(REFERRAL_FEE)
    +sum(RESTOCKING_FEE)
    +sum(turner_costs)
    AS amazon_costs,
    sum(promotion)
    +sum(REFUND_PROMOTION)
    +sum(goodwill)
    +sum(gift_wrap)
    +sum(GIFT_WRAP_CHARGEBACK)
    +sum(subscription_fee)
    as other_marketing_costs,
    sum(TAX_PRINCIPAL)
    +sum(TAX_SHIPPING)
    +sum(TAX_REIMBURSED)
    +sum(TAX_OTHER)
    as taxes
    FROM {{ref('product_pl')}}
    group by 1,2
)

select 
pl_month,
brand,
gross_sales,
metric_name,
amount,
amount/nullif(gross_sales,0) as percent_gross_sales
from cte
unpivot(amount for metric_name in (
    cogs,ad_spend,shipping_costs,amazon_costs,other_marketing_costs,taxes
))