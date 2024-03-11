with source as (
  select
      account_key,
      amazon_region_id,
      marketplace_key,
      posted_local_date,
      asin,
      sku,
      currency,
      case when original_description = 'Other-ServiceFeeEvent-PaidServicesFee' then 'other_amount_distributable' else metric end as metric, 
      amount_usd as amount
    from DATAHAWK_SHARE_83514.CUSTOM_83514.finance_profit_ledger
),

pivoting as (
  select * 
  from source pivot(sum(amount) for metric in (  
      'gross_sales',
--      'orders',
      'gift_wrap',
      'units_sold',
      'reimbursed_product',
      'refund_commission',
      'refunded_referral_fees',
      'reimbursed_shipping',
      'refund_promotion',
      'refund_shipping_promotion',
      'refund_shipping_chargeback',
      'goodwill',
      'reversal_reimbursed',
      'gift_wrap_chargeback',

      'shipping',
      'shipping_promotion',
      'shipping_chargeback',
      'inbound_transportation',

      'fba_storage_fee',
      'fba_long_storage_fee',
      'fba_inventory_placement_service',
      'warehouse_damage',
      'warehouse_lost_manual',

      'fba_per_unit_fulfilment_fee',
      'disposal_complete',
      'removal_complete',

      'sponsored_display_cost',
      'sponsored_products_cost',
      'sponsored_brands_cost',

      'referral_fee',
      'promotion',
    --   'subscription_fee',

      'tax_principal',
      'tax_principal_collected',
      'tax_shipping',
      'tax_reimbursed',
      'tax_other',

      'other_amount',
      'other_amount_distributable',
      'restocking_fee'

        )) 
    as p(  
      account_key,
      amazon_region_id,
      marketplace_key,
      posted_local_date,
      asin,
      sku,
      currency,
      gross_sales,
      gift_wrap,
        units_sold,
      reimbursed_product,
      refund_commission,
      refunded_referral_fees,
      reimbursed_shipping,
      refund_promotion,
      refund_shipping_promotion,
      refund_shipping_chargeback,
      goodwill,
      reversal_reimbursed,
      gift_wrap_chargeback,
      shipping,
      shipping_promotion,
      shipping_chargeback,
      inbound_transportation,
      fba_storage_fee,
      fba_long_storage_fee,
      fba_inventory_placement_service,
      warehouse_damage,
      warehouse_lost_manual,
      fba_per_unit_fulfilment_fee,
      disposal_complete,
      removal_complete,
      sponsored_display_cost,
      sponsored_products_cost,
      sponsored_brands_cost,
      referral_fee,
      promotion,
    --   subscription_fee,
      tax_principal,
      tax_principal_collected,
      tax_shipping,
      tax_reimbursed,
      tax_other,
      other_amount,
      other_amount_distributable,
      restocking_fee
  )

),

sales_data as (
  select 
    account_key, 
    marketplace_key,
    date_local_day as purchase_local_date,
    channel_product_id,
    sku,
    currency,
    sales,
    units_sold
  from DATAHAWK_SHARE_83514.FINANCE.finance_product_metrics_daily
  where marketplace_key <> 'Unknown' and workspace_id = '83514'
)

select 
      coalesce(d.account_key,s.account_key) as account_key,
      d.amazon_region_id as amazon_region_id,
      coalesce(d.marketplace_key, s.marketplace_key) as marketplace_key,
      coalesce(d.posted_local_date,s.purchase_local_date) as posted_local_date,
      coalesce(d.asin,s.channel_product_id) as asin,
      case when coalesce(d.sku,s.sku) ilike '%uncommingled%' then NULL else coalesce(d.sku,s.sku) end as sku, 
      coalesce(d.currency,s.currency) as  currency,
      max(s.sales) as earned_gross_sales,
      max(s.units_sold) as earned_units_sold,
      max(gross_sales) as gross_sales,
      max(case 
          when 
              amazon_region_id != 1
              and coalesce(gross_sales, tax_principal_collected) is not null 
              then 
                  coalesce(gross_sales, 0) + 
                  coalesce(tax_principal_collected, 0) 
              else gross_sales 
      end) as gross_sales_with_tax,
      max(gift_wrap) as gift_wrap,
      max(reimbursed_product) as reimbursed_product,
      max(refund_commission) as refund_comission,
      max(refunded_referral_fees) as refunded_referral_fees,
      max(reimbursed_shipping) as reimbursed_shipping,
      max(refund_promotion) as refund_promotion,
      max(refund_shipping_promotion) as refund_shipping_promotion,
      max(refund_shipping_chargeback) as refund_shipping_chargeback,
      max(goodwill) as goodwill,
      max(reversal_reimbursed) as reversal_reimbursed,
      max(gift_wrap_chargeback) as gift_wrap_chargeback,
      max(shipping) as shipping,
      max(shipping_promotion) as shipping_promotion,
      max(shipping_chargeback) as shipping_chargeback,
      max(inbound_transportation) as inbound_transportation,
      max(fba_storage_fee) as fba_storage_fee,
      max(fba_long_storage_fee) as fba_long_storage_fee,
      max(fba_inventory_placement_service) as fba_inventory_placement_service,
      max(warehouse_damage) as warehouse_damage,
      max(warehouse_lost_manual) as warehouse_lost_manual,
      max(fba_per_unit_fulfilment_fee) as fba_per_unit_fulfilment_fee,
      max(disposal_complete) as disposal_complete,
      max(removal_complete) as removal_complete,
      max(sponsored_display_cost) as sponsored_display_cost,
      max(sponsored_products_cost) as sponsored_products_cost,
      max(sponsored_brands_cost) as sponsored_brands_cost,
      max(referral_fee) as referral_fee,
      max(promotion) as promotion,
    --   max(subscription_fee) as subscription_fee,
      max(tax_principal) as tax_principal,
      max(tax_principal_collected) as tax_principal_collected,
      max(tax_shipping) as tax_shipping,
      max(tax_reimbursed) as tax_reimbursed,
      max(tax_other) as tax_other,
      max(other_amount) as other_amount,
      max(other_amount_distributable) as other_amount_distributable,
      max(restocking_fee) as restocking_fee
    from pivoting d
    full outer join sales_data s
    on s.channel_product_id = d.asin
    and s.sku = d.sku 
    and s.marketplace_key = d.marketplace_key
    and s.purchase_local_date = d.posted_local_date
    and s.account_key = d.account_key
    and s.currency = d.currency 
group by 1,2,3,4,5,6,7