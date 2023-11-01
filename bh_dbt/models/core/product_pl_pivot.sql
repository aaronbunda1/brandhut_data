{{config(materialized='table')}}
WITH prefinal as (
    SELECT
        BRAND,
        SELLER_NAME,
        ACCOUNT_KEY,
        REGION,
        MARKETPLACE_KEY,
        DATE_DAY,
        CHANNEL_PRODUCT_ID,
        SKU,
        COLOR,
        'USD' as CURRENCY,
        rate_to_usd,
        region_name,
        internal_sku_category,
        metric_name,
        round(amount/coalesce(rate_to_usd,1),2) as amount
    FROM {{ref('product_pl')}}
    UNPIVOT(amount FOR metric_name IN (
            GROSS_SALES,
    ORDERS,
    UNITS_SOLD,
    COGS,
    SPONSORED_PRODUCTS_COST,
    DIST_SPONSORED_BRANDS_COST,
    DIST_SPONSORED_BRANDS_VIDEO_COST,
    DIST_SPONSORED_DISPLAY_COST,
    -- SPONSORED_PRODUCTS_SALES,
    -- DIST_SPONSORED_BRANDS_SALES,
    -- DIST_SPONSORED_BRANDS_VIDEO_SALES,
    -- DIST_SPONSORED_DISPLAY_SALES,
    GIFT_WRAP,
    REIMBURSED_PRODUCT,
    REIMBURSED_PRODUCT_UNIT,
    REFUND_COMMISSION,
    REFUNDED_REFERRAL_FEES,
    REIMBURSED_SHIPPING,
    REFUND_PROMOTION,
    REFUND_SHIPPING_PROMOTION,
    REFUND_SHIPPING_CHARGEBACK,
    GOODWILL,
    REVERSAL_REIMBURSED,
    GIFT_WRAP_CHARGEBACK,
    SHIPPING,
    SHIPPING_PROMOTION,
    SHIPPING_CHARGEBACK,
    DIST_INBOUND_TRANSPORTATION,
    DIST_FBA_STORAGE_FEE,
    DIST_FBA_INVENTORY_PLACEMENT_SERVICE,
    WAREHOUSE_DAMAGE,
    WAREHOUSE_LOST_MANUAL,
    FBA_PER_UNIT_FULFILMENT_FEE,
    DIST_DISPOSAL_COMPLETE,
    DIST_REMOVAL_COMPLETE,
    REFERRAL_FEE,
    PROMOTION,
    SUBSCRIPTION_FEE,
    TAX_PRINCIPAL,
    TAX_SHIPPING,
    TAX_REIMBURSED,
    TAX_OTHER,
    DIST_OTHER_AMOUNT,
    RESTOCKING_FEE,
    -- EARNED_GROSS_SALES,
    -- EARNED_ORDERS,
    -- EARNED_UNITS_SOLD,
    -- TOTAL_ADVERTISING_SALES,
    -- TOTAL_ADVERTISING_COSTS,
    -- TOTAL_REFERRAL_FEES,
    -- TOTAL_OTHER_MARKETING_COSTS,
    -- TOTAL_WAREHOUSING_COSTS,
    -- TOTAL_SHIPPING_COSTS,
    -- TOTAL_TAXES_ON_SALES,
    NET_SALES,
    NET_UNITS_SOLD,
    GROSS_PROFIT
    -- CONTRIBUTION_PROFIT_ONE,
    -- CONTRIBUTION_PROFIT_TWO,
    -- EBITDA,
                , brandhut_commission
                , turner_costs
                , freight
                , ad_spend_manual
                , product_samples
                , miscellaneous_cost
        )))

select * 
from prefinal
where 1=1 
and amount !=0 