SELECT
  month,
  brand,
  marketplace_key,

  cast(freight as numeric(18,2)) as freight,
  cast(turner_costs as numeric(18,2)) as turner_costs,
  cast(ad_spend_manual as numeric(18,2)) as ad_spend_manual,
  cast(product_samples as numeric(18,2)) as product_samples,
  cast(total_invoice_amount as numeric(18,2)) as total_invoice_amount,
  cast(true_up_invoiced as numeric(18,2)) as true_up_invoiced,
  cast(miscellaneous as numeric(18,2)) as miscellaneous,
  cast(manual_unallocated_costs as numeric(18,2)) as manual_unallocated_costs,
  cast(OrderCouponCouponRedemptionFee as numeric(18,2)) as OrderCouponCouponRedemptionFee,
  cast(OtherItemFeeSellerDealPayment as numeric(18,2)) as OtherItemFeeSellerDealPayment,
  cast(OtherServiceFeeEventVineFee as numeric(18,2)) as OtherServiceFeeEventVineFee,
  cast(OtherServiceFeeEventAmazonUpstreamProcessingFee as numeric(18,2)) as OtherServiceFeeEventAmazonUpstreamProcessingFee,
  cast(OtherServiceFeeEventAmazonUpstreamStorageTransportationFee as numeric(18,2)) as OtherServiceFeeEventAmazonUpstreamStorageTransportationFee,
  cast(OtherServiceFeeEventFBAInboundTransportationFee as numeric(18,2)) as OtherServiceFeeEventFBAInboundTransportationFee,
  cast(OtherServiceFeeEventCustomerReturnHRRUnitFee as numeric(18,2)) as OtherServiceFeeEventCustomerReturnHRRUnitFee,
  cast(OtherServiceFeeEventGlobalInboundTransportationDuty as numeric(18,2)) as OtherServiceFeeEventGlobalInboundTransportationDuty,
  cast(OtherServiceFeeEventGlobalInboundTransportationFreight as numeric(18,2)) as OtherServiceFeeEventGlobalInboundTransportationFreight,
  cast(OtherServiceFeeEventSTARStorageBilling as numeric(18,2)) as OtherServiceFeeEventSTARStorageBilling,
  cast(OtherServiceFeeEventFBAInboundConvenienceFee as numeric(18,2)) as OtherServiceFeeEventFBAInboundConvenienceFee,

  current_date() as updated_at
FROM {{ ref('manual_metrics_by_brand_and_month') }}
