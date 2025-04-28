{{config(materialized='ephemeral')}}
WITH base AS (
    SELECT
        month,
        brand,
        marketplace_key,
        freight,
        turner_costs,
        ad_spend_manual,
        product_samples,
        miscellaneous,
        manual_unallocated_costs,
        OrderCouponCouponRedemptionFee,
        OtherItemFeeSellerDealPayment,
        OtherServiceFeeEventVineFee,
        OtherServiceFeeEventAmazonUpstreamProcessingFee,
        OtherServiceFeeEventAmazonUpstreamStorageTransportationFee,
        OtherServiceFeeEventFBAInboundTransportationFee,
        OtherServiceFeeEventCustomerReturnHRRUnitFee,
        OtherServiceFeeEventGlobalInboundTransportationDuty,
        OtherServiceFeeEventGlobalInboundTransportationFreight,
        OtherServiceFeeEventSTARStorageBilling,
        OtherServiceFeeEventFBAInboundConvenienceFee
    FROM {{ref('stg_finance__manual_metrics')}}
)

SELECT 
md5(concat_ws('|', 
    month,
    brand,
    marketplace_key,
    metric_name
)) as key,
brand,
'' AS ACCOUNT_KEY,
NULL AS REGION,
marketplace_key,
month as report_month,
'' AS CHANNEL_PRODUCT_ID,
'' AS SKU,
'USD' AS currency_original,
'' AS internal_sku_category,
metric_name,
round(amount,2) as amount
FROM base
UNPIVOT(amount FOR metric_name IN (
freight,
        turner_costs,
        ad_spend_manual,
        product_samples,
        miscellaneous,
        manual_unallocated_costs,
        OrderCouponCouponRedemptionFee,
        OtherItemFeeSellerDealPayment,
        OtherServiceFeeEventVineFee,
        OtherServiceFeeEventAmazonUpstreamProcessingFee,
        OtherServiceFeeEventAmazonUpstreamStorageTransportationFee,
        OtherServiceFeeEventFBAInboundTransportationFee,
        OtherServiceFeeEventCustomerReturnHRRUnitFee,
        OtherServiceFeeEventGlobalInboundTransportationDuty,
        OtherServiceFeeEventGlobalInboundTransportationFreight,
        OtherServiceFeeEventSTARStorageBilling,
        OtherServiceFeeEventFBAInboundConvenienceFee
    )
)