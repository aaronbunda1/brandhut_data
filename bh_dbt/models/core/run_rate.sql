{{config(materialized='table')}}

--actuals
select
brand,
internal_sku_category,
sku, 
year(date_day)::string as period,
month(date_day)::string as sub_period,
sum(units_sold) as units_sold,
sum(earned_gross_sales) as gross_sales,
--(sum(net_sales)+(sum(promotion)+sum(refund_promotion)))/nullif(sum(units_sold),0) as asp_with_promotions,
sum(reimbursed_product)+sum(REVERSAL_REIMBURSED) as refund,
sum(TOTAL_ADVERTISING_SALES) as TOTAL_ADVERTISING_SALES,
(sum(ad_spend_manual)+sum(sponsored_products_cost)+sum(dist_sponsored_brands_cost)+sum(dist_sponsored_brands_video_cost)+sum(dist_sponsored_display_cost)) as total_ad_spend,
----total_ad_spend/nullif(sum(gross_sales),0) as total_advertising_cost_of_sales,
--total_ad_spend/nullif(sum(total_advertising_sales),0) as advertising_cost_of_advertising_sales, 
--sum(total_advertising_sales)/nullif(total_ad_spend,0) as return_on_ad_spend,
(sum(promotion)+sum(refund_promotion)) as promotion, 
--(sum(promotion)+sum(refund_promotion))/nullif(sum(gross_sales),0) as promotion_cost_of_gross_sales,
sum(total_impressions) as impressions,
sum(total_clicks) as clicks,
sum(cogs) as cogs,
sum(BRANDHUT_COMMISSION) as brandhut_commission,
SUM(REFERRAL_FEE)+
SUM(REFUND_COMMISSION)+
SUM(REFUNDED_REFERRAL_FEES)+
SUM(FBA_PER_UNIT_FULFILMENT_FEE)+
SUM(FREIGHT)+
SUM(REFUND_SHIPPING_CHARGEBACK)+
SUM(REIMBURSED_SHIPPING)+
SUM(SHIPPING)+
SUM(SHIPPING_CHARGEBACK)+
SUM(TAX_OTHER)+
SUM(TAX_PRINCIPAL)+
SUM(TAX_REIMBURSED)+
SUM(TAX_SHIPPING)+
SUM(DIST_DISPOSAL_COMPLETE)+
SUM(DIST_FBA_INVENTORY_PLACEMENT_SERVICE)+
SUM(DIST_FBA_STORAGE_FEE)+
SUM(DIST_INBOUND_TRANSPORTATION)+
SUM(DIST_REMOVAL_COMPLETE)+
SUM(RESTOCKING_FEE)+
SUM(WAREHOUSE_DAMAGE)+
SUM(WAREHOUSE_LOST_MANUAL)+
SUM(GIFT_WRAP)+
SUM(GIFT_WRAP_CHARGEBACK)+
(sum(promotion)+sum(refund_promotion))+
SUM(REFUND_PROMOTION)+
SUM(AD_SPEND_MANUAL)+
SUM(DIST_SPONSORED_BRANDS_COST)+
SUM(DIST_SPONSORED_BRANDS_VIDEO_COST)+
SUM(DIST_SPONSORED_DISPLAY_COST)+
SUM(SPONSORED_PRODUCTS_COST)+
SUM(TURNER_COSTS)+
SUM(DIST_OTHER_AMOUNT)+
SUM(MISCELLANEOUS_COST)+
sum(product_samples)+sum(GIFT_WRAP)+sum(GIFT_WRAP_chargeback) as amazon_and_other_costs, 
case when brand = 'ONANOFF' then sum(AD_SPEND_MANUAL) + 
sum(BRANDHUT_COMMISSION) + 
sum(COGS) + 
sum(DIST_OTHER_AMOUNT) + 
sum(DIST_SPONSORED_BRANDS_COST) + 
sum(DIST_SPONSORED_DISPLAY_COST) + 
sum(FBA_PER_UNIT_FULFILMENT_FEE) + 
sum(FREIGHT) + 
sum(GIFT_WRAP) + 
sum(GIFT_WRAP_CHARGEBACK) + 
sum(GOODWILL) + 
sum(GROSS_SALES) + 
sum(MISCELLANEOUS_COST) + 
sum(PRODUCT_SAMPLES) + 
sum(PROMOTION) + 
sum(REFERRAL_FEE) + 
sum(REFUND_COMMISSION) + 
sum(REFUND_PROMOTION) + 
sum(REFUND_SHIPPING_CHARGEBACK) + 
sum(REFUNDED_REFERRAL_FEES) + 
sum(REIMBURSED_PRODUCT) + 
sum(REIMBURSED_SHIPPING) + 
sum(RESTOCKING_FEE) + 
sum(SHIPPING) + 
sum(SHIPPING_CHARGEBACK) + 
sum(SPONSORED_PRODUCTS_COST) + 
sum(TAX_OTHER) + 
sum(TAX_PRINCIPAL) + 
sum(TAX_REIMBURSED) + 
sum(TAX_SHIPPING) + 
sum(TURNER_COSTS) 
else sum(brandhut_commission)
end as brandhut_profit
from {{ref('product_pl_daily')}}
group by 1,2,3,4,5

union all 

select
brand,
internal_sku_category,
sku, 
'L30D' as period,
NULL as sub_period,
sum(units_sold) as units_sold,
sum(gross_sales) as gross_sales,
--(sum(net_sales)+(sum(promotion)+sum(refund_promotion)))/nullif(sum(units_sold),0) as asp_with_promotions,
sum(reimbursed_product)+sum(REVERSAL_REIMBURSED) as refund,
sum(TOTAL_ADVERTISING_SALES) as TOTAL_ADVERTISING_SALES,
(sum(ad_spend_manual)+sum(sponsored_products_cost)+sum(dist_sponsored_brands_cost)+sum(dist_sponsored_brands_video_cost)+sum(dist_sponsored_display_cost)) as total_ad_spend,
--total_ad_spend/nullif(sum(gross_sales),0) as total_advertising_cost_of_sales,
--total_ad_spend/nullif(sum(total_advertising_sales),0) as advertising_cost_of_advertising_sales, 
--sum(total_advertising_sales)/nullif(total_ad_spend,0) as return_on_ad_spend,
(sum(promotion)+sum(refund_promotion)) as promotion, 
--(sum(promotion)+sum(refund_promotion))/nullif(sum(gross_sales),0) as promotion_cost_of_gross_sales,
sum(total_impressions) as impressions,
sum(total_clicks) as clicks,
sum(cogs) as cogs,
sum(BRANDHUT_COMMISSION) as brandhut_commission,
SUM(REFERRAL_FEE)+
SUM(REFUND_COMMISSION)+
SUM(REFUNDED_REFERRAL_FEES)+
SUM(FBA_PER_UNIT_FULFILMENT_FEE)+
SUM(FREIGHT)+
SUM(REFUND_SHIPPING_CHARGEBACK)+
SUM(REIMBURSED_SHIPPING)+
SUM(SHIPPING)+
SUM(SHIPPING_CHARGEBACK)+
SUM(TAX_OTHER)+
SUM(TAX_PRINCIPAL)+
SUM(TAX_REIMBURSED)+
SUM(TAX_SHIPPING)+
SUM(DIST_DISPOSAL_COMPLETE)+
SUM(DIST_FBA_INVENTORY_PLACEMENT_SERVICE)+
SUM(DIST_FBA_STORAGE_FEE)+
SUM(DIST_INBOUND_TRANSPORTATION)+
SUM(DIST_REMOVAL_COMPLETE)+
SUM(RESTOCKING_FEE)+
SUM(WAREHOUSE_DAMAGE)+
SUM(WAREHOUSE_LOST_MANUAL)+
SUM(GIFT_WRAP)+
SUM(GIFT_WRAP_CHARGEBACK)+
(sum(promotion)+sum(refund_promotion))+
SUM(REFUND_PROMOTION)+
SUM(AD_SPEND_MANUAL)+
SUM(DIST_SPONSORED_BRANDS_COST)+
SUM(DIST_SPONSORED_BRANDS_VIDEO_COST)+
SUM(DIST_SPONSORED_DISPLAY_COST)+
SUM(SPONSORED_PRODUCTS_COST)+
SUM(TURNER_COSTS)+
SUM(DIST_OTHER_AMOUNT)+
SUM(MISCELLANEOUS_COST)+
sum(product_samples)+sum(GIFT_WRAP)+sum(GIFT_WRAP_chargeback) as amazon_and_other_costs, 
case when brand = 'ONANOFF' then sum(AD_SPEND_MANUAL) + 
sum(BRANDHUT_COMMISSION) + 
sum(COGS) + 
sum(DIST_OTHER_AMOUNT) + 
sum(DIST_SPONSORED_BRANDS_COST) + 
sum(DIST_SPONSORED_DISPLAY_COST) + 
sum(FBA_PER_UNIT_FULFILMENT_FEE) + 
sum(FREIGHT) + 
sum(GIFT_WRAP) + 
sum(GIFT_WRAP_CHARGEBACK) + 
sum(GOODWILL) + 
sum(GROSS_SALES) + 
sum(MISCELLANEOUS_COST) + 
sum(PRODUCT_SAMPLES) + 
sum(PROMOTION) + 
sum(REFERRAL_FEE) + 
sum(REFUND_COMMISSION) + 
sum(REFUND_PROMOTION) + 
sum(REFUND_SHIPPING_CHARGEBACK) + 
sum(REFUNDED_REFERRAL_FEES) + 
sum(REIMBURSED_PRODUCT) + 
sum(REIMBURSED_SHIPPING) + 
sum(RESTOCKING_FEE) + 
sum(SHIPPING) + 
sum(SHIPPING_CHARGEBACK) + 
sum(SPONSORED_PRODUCTS_COST) + 
sum(TAX_OTHER) + 
sum(TAX_PRINCIPAL) + 
sum(TAX_REIMBURSED) + 
sum(TAX_SHIPPING) + 
sum(TURNER_COSTS) 
else sum(brandhut_commission)
end as brandhut_profit

from {{ref('product_pl_daily')}}
where date_day between current_date()-31 and current_date()-1
group by 1,2,3,4,5

union all 

select
brand,
internal_sku_category,
sku, 
'L90D' as period,
NULL as sub_period,
sum(units_sold) as units_sold,
sum(gross_sales) as gross_sales,
--(sum(net_sales)+(sum(promotion)+sum(refund_promotion)))/nullif(sum(units_sold),0) as asp_with_promotions,
sum(reimbursed_product)+sum(REVERSAL_REIMBURSED) as refund,
sum(TOTAL_ADVERTISING_SALES) as TOTAL_ADVERTISING_SALES,
(sum(ad_spend_manual)+sum(sponsored_products_cost)+sum(dist_sponsored_brands_cost)+sum(dist_sponsored_brands_video_cost)+sum(dist_sponsored_display_cost)) as total_ad_spend,
--total_ad_spend/nullif(sum(gross_sales),0) as total_advertising_cost_of_sales,
--total_ad_spend/nullif(sum(total_advertising_sales),0) as advertising_cost_of_advertising_sales, 
--sum(total_advertising_sales)/nullif(total_ad_spend,0) as return_on_ad_spend,
(sum(promotion)+sum(refund_promotion)) as promotion, 
--(sum(promotion)+sum(refund_promotion))/nullif(sum(gross_sales),0) as promotion_cost_of_gross_sales,
sum(total_impressions) as impressions,
sum(total_clicks) as clicks,
sum(cogs) as cogs,
sum(BRANDHUT_COMMISSION) as brandhut_commission,
SUM(REFERRAL_FEE)+
SUM(REFUND_COMMISSION)+
SUM(REFUNDED_REFERRAL_FEES)+
SUM(FBA_PER_UNIT_FULFILMENT_FEE)+
SUM(FREIGHT)+
SUM(REFUND_SHIPPING_CHARGEBACK)+
SUM(REIMBURSED_SHIPPING)+
SUM(SHIPPING)+
SUM(SHIPPING_CHARGEBACK)+
SUM(TAX_OTHER)+
SUM(TAX_PRINCIPAL)+
SUM(TAX_REIMBURSED)+
SUM(TAX_SHIPPING)+
SUM(DIST_DISPOSAL_COMPLETE)+
SUM(DIST_FBA_INVENTORY_PLACEMENT_SERVICE)+
SUM(DIST_FBA_STORAGE_FEE)+
SUM(DIST_INBOUND_TRANSPORTATION)+
SUM(DIST_REMOVAL_COMPLETE)+
SUM(RESTOCKING_FEE)+
SUM(WAREHOUSE_DAMAGE)+
SUM(WAREHOUSE_LOST_MANUAL)+
SUM(GIFT_WRAP)+
SUM(GIFT_WRAP_CHARGEBACK)+
(sum(promotion)+sum(refund_promotion))+
SUM(REFUND_PROMOTION)+
SUM(AD_SPEND_MANUAL)+
SUM(DIST_SPONSORED_BRANDS_COST)+
SUM(DIST_SPONSORED_BRANDS_VIDEO_COST)+
SUM(DIST_SPONSORED_DISPLAY_COST)+
SUM(SPONSORED_PRODUCTS_COST)+
SUM(TURNER_COSTS)+
SUM(DIST_OTHER_AMOUNT)+
SUM(MISCELLANEOUS_COST)+
sum(product_samples)+sum(GIFT_WRAP)+sum(GIFT_WRAP_chargeback) as amazon_and_other_costs, 
case when brand = 'ONANOFF' then sum(AD_SPEND_MANUAL) + 
sum(BRANDHUT_COMMISSION) + 
sum(COGS) + 
sum(DIST_OTHER_AMOUNT) + 
sum(DIST_SPONSORED_BRANDS_COST) + 
sum(DIST_SPONSORED_DISPLAY_COST) + 
sum(FBA_PER_UNIT_FULFILMENT_FEE) + 
sum(FREIGHT) + 
sum(GIFT_WRAP) + 
sum(GIFT_WRAP_CHARGEBACK) + 
sum(GOODWILL) + 
sum(GROSS_SALES) + 
sum(MISCELLANEOUS_COST) + 
sum(PRODUCT_SAMPLES) + 
sum(PROMOTION) + 
sum(REFERRAL_FEE) + 
sum(REFUND_COMMISSION) + 
sum(REFUND_PROMOTION) + 
sum(REFUND_SHIPPING_CHARGEBACK) + 
sum(REFUNDED_REFERRAL_FEES) + 
sum(REIMBURSED_PRODUCT) + 
sum(REIMBURSED_SHIPPING) + 
sum(RESTOCKING_FEE) + 
sum(SHIPPING) + 
sum(SHIPPING_CHARGEBACK) + 
sum(SPONSORED_PRODUCTS_COST) + 
sum(TAX_OTHER) + 
sum(TAX_PRINCIPAL) + 
sum(TAX_REIMBURSED) + 
sum(TAX_SHIPPING) + 
sum(TURNER_COSTS) 
else sum(brandhut_commission)
end as brandhut_profit

from {{ref('product_pl_daily')}}
where date_day between current_date()-91 and current_date()-1
group by 1,2,3,4,5

union all 

select
brand,
internal_sku_category,
sku, 
'LY L90D' as period,
NULL as sub_period,
sum(units_sold) as units_sold,
sum(gross_sales) as gross_sales,
--(sum(net_sales)+(sum(promotion)+sum(refund_promotion)))/nullif(sum(units_sold),0) as asp_with_promotions,
sum(reimbursed_product)+sum(REVERSAL_REIMBURSED) as refund,
sum(TOTAL_ADVERTISING_SALES) as TOTAL_ADVERTISING_SALES,
(sum(ad_spend_manual)+sum(sponsored_products_cost)+sum(dist_sponsored_brands_cost)+sum(dist_sponsored_brands_video_cost)+sum(dist_sponsored_display_cost)) as total_ad_spend,
--total_ad_spend/nullif(sum(gross_sales),0) as total_advertising_cost_of_sales,
--total_ad_spend/nullif(sum(total_advertising_sales),0) as advertising_cost_of_advertising_sales, 
--sum(total_advertising_sales)/nullif(total_ad_spend,0) as return_on_ad_spend,
(sum(promotion)+sum(refund_promotion)) as promotion, 
--(sum(promotion)+sum(refund_promotion))/nullif(sum(gross_sales),0) as promotion_cost_of_gross_sales,
sum(total_impressions) as impressions,
sum(total_clicks) as clicks,
sum(cogs) as cogs,
sum(BRANDHUT_COMMISSION) as brandhut_commission,
SUM(REFERRAL_FEE)+
SUM(REFUND_COMMISSION)+
SUM(REFUNDED_REFERRAL_FEES)+
SUM(FBA_PER_UNIT_FULFILMENT_FEE)+
SUM(FREIGHT)+
SUM(REFUND_SHIPPING_CHARGEBACK)+
SUM(REIMBURSED_SHIPPING)+
SUM(SHIPPING)+
SUM(SHIPPING_CHARGEBACK)+
SUM(TAX_OTHER)+
SUM(TAX_PRINCIPAL)+
SUM(TAX_REIMBURSED)+
SUM(TAX_SHIPPING)+
SUM(DIST_DISPOSAL_COMPLETE)+
SUM(DIST_FBA_INVENTORY_PLACEMENT_SERVICE)+
SUM(DIST_FBA_STORAGE_FEE)+
SUM(DIST_INBOUND_TRANSPORTATION)+
SUM(DIST_REMOVAL_COMPLETE)+
SUM(RESTOCKING_FEE)+
SUM(WAREHOUSE_DAMAGE)+
SUM(WAREHOUSE_LOST_MANUAL)+
SUM(GIFT_WRAP)+
SUM(GIFT_WRAP_CHARGEBACK)+
(sum(promotion)+sum(refund_promotion))+
SUM(REFUND_PROMOTION)+
SUM(AD_SPEND_MANUAL)+
SUM(DIST_SPONSORED_BRANDS_COST)+
SUM(DIST_SPONSORED_BRANDS_VIDEO_COST)+
SUM(DIST_SPONSORED_DISPLAY_COST)+
SUM(SPONSORED_PRODUCTS_COST)+
SUM(TURNER_COSTS)+
SUM(DIST_OTHER_AMOUNT)+
SUM(MISCELLANEOUS_COST)+
sum(product_samples)+sum(GIFT_WRAP)+sum(GIFT_WRAP_chargeback) as amazon_and_other_costs, 
case when brand = 'ONANOFF' then sum(AD_SPEND_MANUAL) + 
sum(BRANDHUT_COMMISSION) + 
sum(COGS) + 
sum(DIST_OTHER_AMOUNT) + 
sum(DIST_SPONSORED_BRANDS_COST) + 
sum(DIST_SPONSORED_DISPLAY_COST) + 
sum(FBA_PER_UNIT_FULFILMENT_FEE) + 
sum(FREIGHT) + 
sum(GIFT_WRAP) + 
sum(GIFT_WRAP_CHARGEBACK) + 
sum(GOODWILL) + 
sum(GROSS_SALES) + 
sum(MISCELLANEOUS_COST) + 
sum(PRODUCT_SAMPLES) + 
sum(PROMOTION) + 
sum(REFERRAL_FEE) + 
sum(REFUND_COMMISSION) + 
sum(REFUND_PROMOTION) + 
sum(REFUND_SHIPPING_CHARGEBACK) + 
sum(REFUNDED_REFERRAL_FEES) + 
sum(REIMBURSED_PRODUCT) + 
sum(REIMBURSED_SHIPPING) + 
sum(RESTOCKING_FEE) + 
sum(SHIPPING) + 
sum(SHIPPING_CHARGEBACK) + 
sum(SPONSORED_PRODUCTS_COST) + 
sum(TAX_OTHER) + 
sum(TAX_PRINCIPAL) + 
sum(TAX_REIMBURSED) + 
sum(TAX_SHIPPING) + 
sum(TURNER_COSTS) 
else sum(brandhut_commission)
end as brandhut_profit
from {{ref('product_pl_daily')}}
where date_day between current_date()-91-365 and current_date()-1-365
group by 1,2,3,4,5

union all 

select
brand,
internal_sku_category,
sku, 
'30D Run Rate' as period,
NULL as sub_period,
sum(units_sold)/(7/30) as units_sold,
sum(gross_sales)/(7/30)  as gross_sales,
--(sum(net_sales)+(sum(promotion)+sum(refund_promotion)))/nullif(sum(units_sold),0) as asp_with_promotions,
sum(reimbursed_product)/(7/30)  as reimbursed_product,
sum(TOTAL_ADVERTISING_SALES)/(7/30)  as TOTAL_ADVERTISING_SALES,
(sum(ad_spend_manual)+sum(sponsored_products_cost)+sum(dist_sponsored_brands_cost)+sum(dist_sponsored_brands_video_cost)+sum(dist_sponsored_display_cost))/(7/30)  as total_ad_spend,
--total_ad_spend/nullif(sum(pl.gross_sales/(7/30)),0) as total_advertising_cost_of_sales,
--total_ad_spend/nullif(sum(pl.total_advertising_sales/(7/30)),0) as advertising_cost_of_advertising_sales, 
-- sum(pl.total_advertising_sales/(7/30) )/nullif(total_ad_spend,0) as return_on_ad_spend,
(sum(promotion)+sum(refund_promotion))/(7/30)  as promotion, 
-- sum(pl.promotion/(7/30) )/nullif(sum(pl.gross_sales/(7/30) ),0) as promotion_cost_of_gross_sales,
sum(total_impressions)/(7/30)  as impressions,
sum(total_clicks)/(7/30)  as clicks,
sum(cogs)/(7/30)  as cogs,
sum(BRANDHUT_COMMISSION)/(7/30) as brandhut_commission,
(SUM(REFERRAL_FEE)+
SUM(REFUND_COMMISSION)+
SUM(REFUNDED_REFERRAL_FEES)+
SUM(FBA_PER_UNIT_FULFILMENT_FEE)+
SUM(FREIGHT)+
SUM(REFUND_SHIPPING_CHARGEBACK)+
SUM(REIMBURSED_SHIPPING)+
SUM(SHIPPING)+
SUM(SHIPPING_CHARGEBACK)+
SUM(TAX_OTHER)+
SUM(TAX_PRINCIPAL)+
SUM(TAX_REIMBURSED)+
SUM(TAX_SHIPPING)+
SUM(DIST_DISPOSAL_COMPLETE)+
SUM(DIST_FBA_INVENTORY_PLACEMENT_SERVICE)+
SUM(DIST_FBA_STORAGE_FEE)+
SUM(DIST_INBOUND_TRANSPORTATION)+
SUM(DIST_REMOVAL_COMPLETE)+
SUM(RESTOCKING_FEE)+
SUM(WAREHOUSE_DAMAGE)+
SUM(WAREHOUSE_LOST_MANUAL)+
SUM(GIFT_WRAP)+
SUM(GIFT_WRAP_CHARGEBACK)+
(sum(promotion)+sum(refund_promotion))+
SUM(REFUND_PROMOTION)+
SUM(AD_SPEND_MANUAL)+
SUM(DIST_SPONSORED_BRANDS_COST)+
SUM(DIST_SPONSORED_BRANDS_VIDEO_COST)+
SUM(DIST_SPONSORED_DISPLAY_COST)+
SUM(SPONSORED_PRODUCTS_COST)+
SUM(TURNER_COSTS)+
SUM(DIST_OTHER_AMOUNT)+
SUM(MISCELLANEOUS_COST)+
sum(product_samples)+sum(GIFT_WRAP)+sum(GIFT_WRAP_chargeback))/(7/30) as amazon_and_other_costs, 
case when brand = 'ONANOFF' then sum(AD_SPEND_MANUAL) + 
sum(BRANDHUT_COMMISSION) + 
sum(COGS) + 
sum(DIST_OTHER_AMOUNT) + 
sum(DIST_SPONSORED_BRANDS_COST) + 
sum(DIST_SPONSORED_DISPLAY_COST) + 
sum(FBA_PER_UNIT_FULFILMENT_FEE) + 
sum(FREIGHT) + 
sum(GIFT_WRAP) + 
sum(GIFT_WRAP_CHARGEBACK) + 
sum(GOODWILL) + 
sum(GROSS_SALES) + 
sum(MISCELLANEOUS_COST) + 
sum(PRODUCT_SAMPLES) + 
sum(PROMOTION) + 
sum(REFERRAL_FEE) + 
sum(REFUND_COMMISSION) + 
sum(REFUND_PROMOTION) + 
sum(REFUND_SHIPPING_CHARGEBACK) + 
sum(REFUNDED_REFERRAL_FEES) + 
sum(REIMBURSED_PRODUCT) + 
sum(REIMBURSED_SHIPPING) + 
sum(RESTOCKING_FEE) + 
sum(SHIPPING) + 
sum(SHIPPING_CHARGEBACK) + 
sum(SPONSORED_PRODUCTS_COST) + 
sum(TAX_OTHER) + 
sum(TAX_PRINCIPAL) + 
sum(TAX_REIMBURSED) + 
sum(TAX_SHIPPING) + 
sum(TURNER_COSTS) 
else sum(brandhut_commission)
end/(7/30) as brandhut_profit
from {{ref('product_pl_daily')}} pl
where date_day between current_date()-8 and current_date()-1
group by 1,2,3,4,5

union all 

select
brand,
internal_sku_category,
sku, 
'90D Run Rate' as period,
NULL as sub_period,
sum(units_sold)/(7/90) as units_sold,
sum(gross_sales)/(7/90)  as gross_sales,
--(sum(net_sales)+(sum(promotion)+sum(refund_promotion)))/nullif(sum(units_sold),0) as asp_with_promotions,
sum(reimbursed_product)/(7/90)  as reimbursed_product,
sum(TOTAL_ADVERTISING_SALES)/(7/90)  as TOTAL_ADVERTISING_SALES,
(sum(ad_spend_manual)+sum(sponsored_products_cost)+sum(dist_sponsored_brands_cost)+sum(dist_sponsored_brands_video_cost)+sum(dist_sponsored_display_cost))/(7/90)  as total_ad_spend,
--total_ad_spend/nullif(sum(pl.gross_sales/(7/90)),0) as total_advertising_cost_of_sales,
--total_ad_spend/nullif(sum(pl.total_advertising_sales/(7/90)),0) as advertising_cost_of_advertising_sales, 
-- sum(pl.total_advertising_sales/(7/90) )/nullif(total_ad_spend,0) as return_on_ad_spend,
(sum(promotion)+sum(refund_promotion))/(7/90)  as promotion, 
-- sum(pl.promotion/(7/90) )/nullif(sum(pl.gross_sales/(7/90) ),0) as promotion_cost_of_gross_sales,
sum(total_impressions)/(7/90)  as impressions,
sum(total_clicks)/(7/90)  as clicks,
sum(cogs)/(7/90)  as cogs,
sum(BRANDHUT_COMMISSION)/(7/90) as brandhut_commission,
(SUM(REFERRAL_FEE)+
SUM(REFUND_COMMISSION)+
SUM(REFUNDED_REFERRAL_FEES)+
SUM(FBA_PER_UNIT_FULFILMENT_FEE)+
SUM(FREIGHT)+
SUM(REFUND_SHIPPING_CHARGEBACK)+
SUM(REIMBURSED_SHIPPING)+
SUM(SHIPPING)+
SUM(SHIPPING_CHARGEBACK)+
SUM(TAX_OTHER)+
SUM(TAX_PRINCIPAL)+
SUM(TAX_REIMBURSED)+
SUM(TAX_SHIPPING)+
SUM(DIST_DISPOSAL_COMPLETE)+
SUM(DIST_FBA_INVENTORY_PLACEMENT_SERVICE)+
SUM(DIST_FBA_STORAGE_FEE)+
SUM(DIST_INBOUND_TRANSPORTATION)+
SUM(DIST_REMOVAL_COMPLETE)+
SUM(RESTOCKING_FEE)+
SUM(WAREHOUSE_DAMAGE)+
SUM(WAREHOUSE_LOST_MANUAL)+
SUM(GIFT_WRAP)+
SUM(GIFT_WRAP_CHARGEBACK)+
(sum(promotion)+sum(refund_promotion))+
SUM(REFUND_PROMOTION)+
SUM(AD_SPEND_MANUAL)+
SUM(DIST_SPONSORED_BRANDS_COST)+
SUM(DIST_SPONSORED_BRANDS_VIDEO_COST)+
SUM(DIST_SPONSORED_DISPLAY_COST)+
SUM(SPONSORED_PRODUCTS_COST)+
SUM(TURNER_COSTS)+
SUM(DIST_OTHER_AMOUNT)+
SUM(MISCELLANEOUS_COST)+
sum(product_samples)+sum(GIFT_WRAP)+sum(GIFT_WRAP_chargeback))/(7/90) as amazon_and_other_costs, 
case when brand = 'ONANOFF' then sum(AD_SPEND_MANUAL) + 
sum(BRANDHUT_COMMISSION) + 
sum(COGS) + 
sum(DIST_OTHER_AMOUNT) + 
sum(DIST_SPONSORED_BRANDS_COST) + 
sum(DIST_SPONSORED_DISPLAY_COST) + 
sum(FBA_PER_UNIT_FULFILMENT_FEE) + 
sum(FREIGHT) + 
sum(GIFT_WRAP) + 
sum(GIFT_WRAP_CHARGEBACK) + 
sum(GOODWILL) + 
sum(GROSS_SALES) + 
sum(MISCELLANEOUS_COST) + 
sum(PRODUCT_SAMPLES) + 
sum(PROMOTION) + 
sum(REFERRAL_FEE) + 
sum(REFUND_COMMISSION) + 
sum(REFUND_PROMOTION) + 
sum(REFUND_SHIPPING_CHARGEBACK) + 
sum(REFUNDED_REFERRAL_FEES) + 
sum(REIMBURSED_PRODUCT) + 
sum(REIMBURSED_SHIPPING) + 
sum(RESTOCKING_FEE) + 
sum(SHIPPING) + 
sum(SHIPPING_CHARGEBACK) + 
sum(SPONSORED_PRODUCTS_COST) + 
sum(TAX_OTHER) + 
sum(TAX_PRINCIPAL) + 
sum(TAX_REIMBURSED) + 
sum(TAX_SHIPPING) + 
sum(TURNER_COSTS) 
else sum(brandhut_commission)
end/(7/90) as brandhut_profit
from {{ref('product_pl_daily')}} pl
where date_day between current_date()-8 and current_date()-1
group by 1,2,3,4,5

union all

select
brand,
internal_sku_category,
sku, 
'YTD Run Rate' as period,
NULL as sub_period,
sum(units_sold)/(datediff(day,date_trunc(year,current_date()),current_date()-1)/365) as units_sold,
sum(gross_sales)/(datediff(day,date_trunc(year,current_date()),current_date()-1)/365)  as gross_sales,
--(sum(net_sales)+(sum(promotion)+sum(refund_promotion)))/nullif(sum(units_sold),0) as asp_with_promotions,
sum(reimbursed_product)/(datediff(day,date_trunc(year,current_date()),current_date()-1)/365)  as reimbursed_product,
sum(TOTAL_ADVERTISING_SALES)/(datediff(day,date_trunc(year,current_date()),current_date()-1)/365)  as TOTAL_ADVERTISING_SALES,
(sum(ad_spend_manual)+sum(sponsored_products_cost)+sum(dist_sponsored_brands_cost)+sum(dist_sponsored_brands_video_cost)+sum(dist_sponsored_display_cost))/(datediff(day,date_trunc(year,current_date()),current_date()-1)/365)  as total_ad_spend,
--total_ad_spend/nullif(sum(pl.gross_sales/(datediff(day,date_trunc(year,current_date()),current_date()-1)/365)),0) as total_advertising_cost_of_sales,
--total_ad_spend/nullif(sum(pl.total_advertising_sales/(datediff(day,date_trunc(year,current_date()),current_date()-1)/365)),0) as advertising_cost_of_advertising_sales, 
-- sum(pl.total_advertising_sales/(datediff(day,date_trunc(year,current_date()),current_date()-1)/365) )/nullif(total_ad_spend,0) as return_on_ad_spend,
(sum(promotion)+sum(refund_promotion))/(datediff(day,date_trunc(year,current_date()),current_date()-1)/365)  as promotion, 
-- sum(pl.promotion/(datediff(day,date_trunc(year,current_date()),current_date()-1)/365) )/nullif(sum(pl.gross_sales/(datediff(day,date_trunc(year,current_date()),current_date()-1)/365) ),0) as promotion_cost_of_gross_sales,
sum(total_impressions)/(datediff(day,date_trunc(year,current_date()),current_date()-1)/365)  as impressions,
sum(total_clicks)/(datediff(day,date_trunc(year,current_date()),current_date()-1)/365)  as clicks,
sum(cogs)/(datediff(day,date_trunc(year,current_date()),current_date()-1)/365)  as cogs,
sum(BRANDHUT_COMMISSION)/(datediff(day,date_trunc(year,current_date()),current_date()-1)/365) as brandhut_commission,
(SUM(REFERRAL_FEE)+
SUM(REFUND_COMMISSION)+
SUM(REFUNDED_REFERRAL_FEES)+
SUM(FBA_PER_UNIT_FULFILMENT_FEE)+
SUM(FREIGHT)+
SUM(REFUND_SHIPPING_CHARGEBACK)+
SUM(REIMBURSED_SHIPPING)+
SUM(SHIPPING)+
SUM(SHIPPING_CHARGEBACK)+
SUM(TAX_OTHER)+
SUM(TAX_PRINCIPAL)+
SUM(TAX_REIMBURSED)+
SUM(TAX_SHIPPING)+
SUM(DIST_DISPOSAL_COMPLETE)+
SUM(DIST_FBA_INVENTORY_PLACEMENT_SERVICE)+
SUM(DIST_FBA_STORAGE_FEE)+
SUM(DIST_INBOUND_TRANSPORTATION)+
SUM(DIST_REMOVAL_COMPLETE)+
SUM(RESTOCKING_FEE)+
SUM(WAREHOUSE_DAMAGE)+
SUM(WAREHOUSE_LOST_MANUAL)+
SUM(GIFT_WRAP)+
SUM(GIFT_WRAP_CHARGEBACK)+
(sum(promotion)+sum(refund_promotion))+
SUM(REFUND_PROMOTION)+
SUM(AD_SPEND_MANUAL)+
SUM(DIST_SPONSORED_BRANDS_COST)+
SUM(DIST_SPONSORED_BRANDS_VIDEO_COST)+
SUM(DIST_SPONSORED_DISPLAY_COST)+
SUM(SPONSORED_PRODUCTS_COST)+
SUM(TURNER_COSTS)+
SUM(DIST_OTHER_AMOUNT)+
SUM(MISCELLANEOUS_COST)+
sum(product_samples)+sum(GIFT_WRAP)+sum(GIFT_WRAP_chargeback))/(datediff(day,date_trunc(year,current_date()),current_date()-1)/365) as amazon_and_other_costs, 
case when brand = 'ONANOFF' then sum(AD_SPEND_MANUAL) + 
sum(BRANDHUT_COMMISSION) + 
sum(COGS) + 
sum(DIST_OTHER_AMOUNT) + 
sum(DIST_SPONSORED_BRANDS_COST) + 
sum(DIST_SPONSORED_DISPLAY_COST) + 
sum(FBA_PER_UNIT_FULFILMENT_FEE) + 
sum(FREIGHT) + 
sum(GIFT_WRAP) + 
sum(GIFT_WRAP_CHARGEBACK) + 
sum(GOODWILL) + 
sum(GROSS_SALES) + 
sum(MISCELLANEOUS_COST) + 
sum(PRODUCT_SAMPLES) + 
sum(PROMOTION) + 
sum(REFERRAL_FEE) + 
sum(REFUND_COMMISSION) + 
sum(REFUND_PROMOTION) + 
sum(REFUND_SHIPPING_CHARGEBACK) + 
sum(REFUNDED_REFERRAL_FEES) + 
sum(REIMBURSED_PRODUCT) + 
sum(REIMBURSED_SHIPPING) + 
sum(RESTOCKING_FEE) + 
sum(SHIPPING) + 
sum(SHIPPING_CHARGEBACK) + 
sum(SPONSORED_PRODUCTS_COST) + 
sum(TAX_OTHER) + 
sum(TAX_PRINCIPAL) + 
sum(TAX_REIMBURSED) + 
sum(TAX_SHIPPING) + 
sum(TURNER_COSTS) 
else sum(brandhut_commission)
end/(datediff(day,date_trunc(year,current_date()),current_date()-1)/365) as brandhut_profit
from {{ref('product_pl_daily')}} pl
where date_day between date_trunc(year,current_date()) and current_date()-1
group by 1,2,3,4,5


union all 

select
brand,
internal_sku_category,
sku, 
'LY' as period,
NULL as sub_period,
sum(units_sold) as units_sold,
sum(gross_sales) as gross_sales,
--(sum(net_sales)+(sum(promotion)+sum(refund_promotion)))/nullif(sum(units_sold),0) as asp_with_promotions,
sum(reimbursed_product)+sum(REVERSAL_REIMBURSED) as refund,
sum(TOTAL_ADVERTISING_SALES) as TOTAL_ADVERTISING_SALES,
(sum(ad_spend_manual)+sum(sponsored_products_cost)+sum(dist_sponsored_brands_cost)+sum(dist_sponsored_brands_video_cost)+sum(dist_sponsored_display_cost)) as total_ad_spend,
--total_ad_spend/nullif(sum(gross_sales),0) as total_advertising_cost_of_sales,
--total_ad_spend/nullif(sum(total_advertising_sales),0) as advertising_cost_of_advertising_sales, 
--sum(total_advertising_sales)/nullif(total_ad_spend,0) as return_on_ad_spend,
(sum(promotion)+sum(refund_promotion)) as promotion, 
--(sum(promotion)+sum(refund_promotion))/nullif(sum(gross_sales),0) as promotion_cost_of_gross_sales,
sum(total_impressions) as impressions,
sum(total_clicks) as clicks,
sum(cogs) as cogs,
sum(BRANDHUT_COMMISSION) as brandhut_commission,
SUM(REFERRAL_FEE)+
SUM(REFUND_COMMISSION)+
SUM(REFUNDED_REFERRAL_FEES)+
SUM(FBA_PER_UNIT_FULFILMENT_FEE)+
SUM(FREIGHT)+
SUM(REFUND_SHIPPING_CHARGEBACK)+
SUM(REIMBURSED_SHIPPING)+
SUM(SHIPPING)+
SUM(SHIPPING_CHARGEBACK)+
SUM(TAX_OTHER)+
SUM(TAX_PRINCIPAL)+
SUM(TAX_REIMBURSED)+
SUM(TAX_SHIPPING)+
SUM(DIST_DISPOSAL_COMPLETE)+
SUM(DIST_FBA_INVENTORY_PLACEMENT_SERVICE)+
SUM(DIST_FBA_STORAGE_FEE)+
SUM(DIST_INBOUND_TRANSPORTATION)+
SUM(DIST_REMOVAL_COMPLETE)+
SUM(RESTOCKING_FEE)+
SUM(WAREHOUSE_DAMAGE)+
SUM(WAREHOUSE_LOST_MANUAL)+
SUM(GIFT_WRAP)+
SUM(GIFT_WRAP_CHARGEBACK)+
(sum(promotion)+sum(refund_promotion))+
SUM(REFUND_PROMOTION)+
SUM(AD_SPEND_MANUAL)+
SUM(DIST_SPONSORED_BRANDS_COST)+
SUM(DIST_SPONSORED_BRANDS_VIDEO_COST)+
SUM(DIST_SPONSORED_DISPLAY_COST)+
SUM(SPONSORED_PRODUCTS_COST)+
SUM(TURNER_COSTS)+
SUM(DIST_OTHER_AMOUNT)+
SUM(MISCELLANEOUS_COST)+
sum(product_samples)+sum(GIFT_WRAP)+sum(GIFT_WRAP_chargeback) as amazon_and_other_costs, 
case when brand = 'ONANOFF' then sum(AD_SPEND_MANUAL) + 
sum(BRANDHUT_COMMISSION) + 
sum(COGS) + 
sum(DIST_OTHER_AMOUNT) + 
sum(DIST_SPONSORED_BRANDS_COST) + 
sum(DIST_SPONSORED_DISPLAY_COST) + 
sum(FBA_PER_UNIT_FULFILMENT_FEE) + 
sum(FREIGHT) + 
sum(GIFT_WRAP) + 
sum(GIFT_WRAP_CHARGEBACK) + 
sum(GOODWILL) + 
sum(GROSS_SALES) + 
sum(MISCELLANEOUS_COST) + 
sum(PRODUCT_SAMPLES) + 
sum(PROMOTION) + 
sum(REFERRAL_FEE) + 
sum(REFUND_COMMISSION) + 
sum(REFUND_PROMOTION) + 
sum(REFUND_SHIPPING_CHARGEBACK) + 
sum(REFUNDED_REFERRAL_FEES) + 
sum(REIMBURSED_PRODUCT) + 
sum(REIMBURSED_SHIPPING) + 
sum(RESTOCKING_FEE) + 
sum(SHIPPING) + 
sum(SHIPPING_CHARGEBACK) + 
sum(SPONSORED_PRODUCTS_COST) + 
sum(TAX_OTHER) + 
sum(TAX_PRINCIPAL) + 
sum(TAX_REIMBURSED) + 
sum(TAX_SHIPPING) + 
sum(TURNER_COSTS) 
else sum(brandhut_commission)
end as brandhut_profit
from {{ref('product_pl_daily')}}
where date_trunc(year,date_day) = date_trunc(year,date_day-365-1)
group by 1,2,3,4,5





