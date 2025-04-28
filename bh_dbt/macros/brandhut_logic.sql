
{% macro get_brand_from_sku(sku_column) %}
    case  
        when {{ sku_column }} ilike '%shield%' then 'Onanoff 2'
        when {{ sku_column }} ilike any ('%roku%','%sunny%','%siri%') then '73&Sunny'
        when {{ sku_column }} ilike any ('%-TH-%', '%TTH%', '%tiny tree%','T-%') then 'Tiny Tree Houses'
        when {{ sku_column }} ilike '%alora%' then 'Alora'
        when {{ sku_column }} ilike '%ZED%' or {{ sku_column }} in (
            'ZEAPM03/00',
            'ZESC08W') 
            or {{ sku_column }} ilike 'ZE%' then 'ZENS'
        when {{sku_column}} ilike '%zens%' then 'ZENS'
        when {{ sku_column }} ilike any ('%storyph%','%ss-%') then 'Onanoff 2'
        when {{ sku_column }} ilike any ('%fokus%') then 'Fokus'
        when {{ sku_column }} ilike '%SPOT%' then 'SPOT'
        when {{ sku_column }} ilike '%QI%' then 'Qisten'
        when {{ sku_column }} ilike any ('CL%','%cellini%') then 'Cellini'
        when {{ sku_column }} ilike any ('%pop-fun%','%BP-%','BP-%','%-ON-%','%ON-%','%buddy%','%onanoff%','%onanoff%','%on-%','%bp-%','%play%','%fun%','%school%','%onanff%','%Onaonff%','%cosmo%','%explore%','%buddy%','%phones%') then 'ONANOFF'
        when {{ sku_column }} IN ('1005-30oz-SB') then 'Legacy'
    end
{% endmacro %}

--  cleanup logic
{% macro fuzzy_match_brand(sku_column) %}
    case  
        when {{ sku_column }} ilike '%shield%' then 'Onanoff 2'
        when {{ sku_column }} ilike any ('%roku%','%sunny%','%siri%') then '73&Sunny'
        when {{ sku_column }} ilike any ('%-TH-%', '%TTH%', '%tiny tree%','T-%') then 'Tiny Tree Houses'
        when {{ sku_column }} ilike any ('%alora%','FBA181QNPSRP.Missing.2') then 'Alora'
        when {{ sku_column }} ilike '%ZED%' or {{ sku_column }} in (
            'ZEAPM03/00',
            'ZESC08W') 
            or {{ sku_column }} ilike 'ZE%' then 'ZENS'
        when {{sku_column}} ilike '%zens%' then 'ZENS'
        when {{ sku_column }} ilike any ('%storyph%','%ss-%') then 'Onanoff 2'
        when {{ sku_column }} ilike any ('%fokus%') then 'Fokus'
        when {{ sku_column }} ilike '%SPOT%' then 'SPOT'
        when {{ sku_column }} ilike '%QI%' then 'Qisten'
        when {{ sku_column }} ilike any ('CL%','%cellini%') then 'Cellini'
        when {{ sku_column }} ilike any ('%pop-fun%','%BP-%','BP-%','%-ON-%','%ON-%','%buddy%','%onanoff%','%onanoff%','%on-%','%bp-%','%play%','%fun%','%school%','%onanff%','%Onaonff%','%cosmo%','%explore%','%buddy%','%phones%') then 'ONANOFF'
        when {{ sku_column }} IN ('1005-30oz-SB') then 'Legacy'
    end
{% endmacro %}

{% macro fuzzy_match_category(sku_column,brand_column,category_column) %}
    case 
        when {{category_column}} is null and {{brand_column}} = 'ZENS' then 'Zens Legacy'
        when {{brand_column}} = 'Onanoff 2' and {{sku_column}} ilike any ('%SS-%','%STSH-%','%shield%') then 'StoryShield'
        when {{brand_column}} = 'Onanoff 2' and {{sku_column}} ilike '%storyph%' then 'Storyphones'
        else {{category_column}} 
    end
{% endmacro %}

-- model logic: map to other amount distributable  
{% macro map_ledger_original_description(original_description, metric) %}
  case 
    when {{ original_description }} IN (
        'Other-AdjustmentEvent-FailedDisbursement',
        'Other-AdjustmentEvent-MiscAdjustment',
        'Other-AdjustmentEvent-ReserveCredit',
        'Other-AdjustmentEvent-ReserveDebit',
        'Other-ServiceFeeEvent-PaidServicesFee',
        'Other-ServiceFeeEvent-Subscription',
        'Other-ServiceFeeEvent-FBAInboundTransportationProgramFee',
        'Other-AdjustmentEvent-PostageBilling_PostageAdjustment',
        'Other-AdjustmentEvent-PostageRefund_PostageAdjustment',
        'Other-AdjustmentEvent-ReturnPostageBilling_FuelSurcharge',
        'Other-AdjustmentEvent-ReturnPostageBilling_Postage',
        'Other-ServiceFeeEvent-FBAPerUnitFulfillmentFee'
        ) then 'other_amount_distributable' 
    when {{ original_description }}  IN (
        'Other-ServiceFeeEvent-AmazonUpstreamProcessingFee',
        'Other-ServiceFeeEvent-AmazonUpstreamStorageTransportationFee') 
    then 'other_amount_spot_only'
    else  {{ metric }}
  end
{% endmacro %}


-- model logic: brandhut commission
{% macro brandhut_commission_logic(sku, brand,net_sales,gross_sales,units,monthly_brand_gs ) %}
case 
    when {{ brand }}  ilike '%cellini%'
        then -{{ net_sales }} *0.1 
    when {{ brand }}  ilike any ('%spot%','%zens%')
        then -{{ net_sales }} *0.15
    when {{ brand }}  ilike '%tiny tree%'
        then -{{ net_sales }} *0.2
    when {{ brand }}  ilike '%onanoff 2%'
        then 
        case 
            when {{ sku }}  ilike '%storyph%' 
                then 
                    case
                        when {{ monthly_brand_gs }}  <= 50000 then -{{ net_sales }} *.1
                        when {{ monthly_brand_gs }}  < 251000 then -{{ net_sales }}  *.09
                        else -{{ net_sales }}  * .06
                    end
            when {{ sku }}  ilike any ('%SS-%','%STSH-%','%shield%')
                then 
                    case
                        when {{ monthly_brand_gs }}  < 251000 then -{{ net_sales }}  *.12
                        else -{{ net_sales }}  * .08
                    end
            else -{{ net_sales }} *.1
        end 
    when {{ brand }}  = 'Pablo Artists'' Choice' then -{{ net_sales }}  *.15
    when {{ brand }}  ilike '%alora%' then -{{ net_sales }} *.15
    when {{ brand }}  = 'Fokus'
        then 
        case 
            when {{ gross_sales }} /nullif(greatest({{ units }} ,1),1) <= 79.99 then -{{ net_sales }} *.08
            when {{ gross_sales }} /nullif(greatest({{ units }} ,1),1) <= 89.99 then -{{ net_sales }} *.12
            when {{ gross_sales }} /nullif(greatest({{ units }} ,1),1) <= 100 then -{{ net_sales }} *.18
            when {{ gross_sales }} /nullif(greatest({{ units }} ,1),1) <= 109.99 then -{{ net_sales }} *.2
            when {{ gross_sales }} /nullif(greatest({{ units }} ,1),1) <= 119.99 then -{{ net_sales }} *.22
            when {{ gross_sales }} /nullif(greatest({{ units }} ,1),1) <= 129.99 then -{{ net_sales }} *.24
            when {{ gross_sales }} /nullif(greatest({{ units }} ,1),1) <= 149.99 then -{{ net_sales }} *.26
            when {{ gross_sales }} /nullif(greatest({{ units }} ,1),1) <= 159.99 then -{{ net_sales }} *.26
            when {{ gross_sales }} /nullif(greatest({{ units }} ,1),1) <= 139.99 then -{{ net_sales }} *.25
            when {{ gross_sales }} /nullif(greatest({{ units }} ,1),1) <= 169.99 then -{{ net_sales }} *.28
            when {{ gross_sales }} /nullif(greatest({{ units }} ,1),1) <= 179.99 then -{{ net_sales }} *.28
            when {{ gross_sales }} /nullif(greatest({{ units }} ,1),1) <= 189.99 then -{{ net_sales }} *.29
            else -{{ net_sales }} *.3
        end
    else 0 
end


{% endmacro %}

{% macro canada_tax_adjustment(marketplace_key, brand, gross_sales) %}
  case 
    when {{ marketplace_key }} = 'Amazon-CA' then
      case 
        when {{ brand }} ilike any ('%73%', '%onanoff%') then -0.06 * {{ gross_sales }}
        when {{ brand }} = 'SPOT' then -0.15 * {{ gross_sales }}
        when {{ brand }} ilike '%tiny tree%' then -0.15 * {{ gross_sales }}
        else 0
      end
    else 0
  end
{% endmacro %}


{% macro metric_group_1(metric_group_2) %}
    case   
        when {{metric_group_2}} in (
            'Ad Spend',
            'Brandhut Commission',
            'Other Expenses',
            'Other Marketing',
            'Referral Fees',
            'Shipping',
            'Taxes',
            'Warehousing'
        ) then 'Expenses'
        when {{metric_group_2}} in (
            'Gross Sales',
            'Returns'
        ) then 'Net Sales'
        else 'Helper'
        end
    {% endmacro %}

{% macro metric_group_2(metric_name) %}



    case 
        when {{ metric_name }} in (
        'DIST_SPONSORED_BRANDS_COST',
        'DIST_SPONSORED_DISPLAY_COST',
        'DIST_SPONOSORED_VIDEO_COST',
        'SPONSORED_PRODUCTS_COST'
        )
    then 'Ad Spend'
    when {{ metric_name }} in (
    'LEDGER_BRANDHUT_COMMISSION'
    )
    then 'Brandhut Commission'
    when {{ metric_name }} in (
    'LEDGER_GROSS_SALES')
    then 'Gross Sales'
    when {{ metric_name }} IN (
        'MANUAL_COGS' ,
        'MANUAL_MISCELLANEOUS_COST', 
        'ORDERCOUPONCOUPONREDEMPTIONFEE',
        'OTHERITEMFEESELLERDEALPAYMENT',
        'OTHERSERVICEFEEEVENTVINEFEE',
        'OTHERSERVICEFEEEVENTAMAZONUPSTREAMPROCESSINGFEE',
        'OTHERSERVICEFEEEVENTAMAZONUPSTREAMSTORAGETRANSPORTATIONFEE',
        'OTHERSERVICEFEEEVENTFBAINBOUNDTRANSPORTATIONFEE',
        'OTHERSERVICEFEEEVENTCUSTOMERRETURNHRRUNITFEE',
        'OTHERSERVICEFEEEVENTGLOBALINBOUNDTRANSPORTATIONDUTY',
        'OTHERSERVICEFEEEVENTGLOBALINBOUNDTRANSPORTATIONFREIGHT',
        'OTHERSERVICEFEEEVENTSTARSTORAGEBILLING',
        'OTHERSERVICEFEEEVENTFBAINBOUNDCONVENIENCEFEE',
        'MANUAL_UNALLOCATED_COSTS',
        'TRUE_UP_INVOICED',
    'DIST_LEDGER_OTHER_AMOUNT',
    'DIST_OTHER_AMOUNT_SPOT_ONLY',
    'LEDGER_OTHER_AMOUNT',
    'MANUAL_PRODUCT_SAMPLES',
    'MANUAL_TURNER_COSTS',
    'LEDGER_REMOVAL_COMPLETE',
    'LEDGER_DISPOSAL_COMPLETE',
    'CANADA_TAX_ON_GROSS_SALES',
    'BANK_CONVERSION_FEE'
    )
    then 'Other Expenses'
    when {{ metric_name }} in (
    'LEDGER_GIFT_WRAP',
    'LEDGER_GIFT_WRAP_CHARGEBACK',
    'LEDGER_GOODWILL',
    'LEDGER_PROMOTION',
    'LEDGER_REFUND_PROMOTION'
    )
    then 'Other Marketing'
    when {{ metric_name }} in (

    'LEDGER_REFERRAL_FEE',
    'LEDGER_REFUND_COMMISSION',
    'LEDGER_REFUNDED_REFERRAL_FEES'
    ) then 'Referral Fees'
    when {{ metric_name }} in (
        'LEDGER_REIMBURSED_PRODUCT',
    'LEDGER_REVERSAL_REIMBURSED'
    )
    then 'Returns'
    when {{ metric_name }} in (
        'LEDGER_FBA_PER_UNIT_FULFILMENT_FEE',
    'LEDGER_REFUND_SHIPPING_CHARGEBACK',
    'LEDGER_REIMBURSED_SHIPPING',
    'LEDGER_SHIPPING',
    'LEDGER_SHIPPING_CHARGEBACK',
    'MANUAL_FREIGHT'
    )
    then 'Shipping'
    when {{ metric_name }} in ('LEDGER_TAX_OTHER',
    'LEDGER_TAX_PRINCIPAL',
    'LEDGER_TAX_REIMBURSED',
    'LEDGER_TAX_SHIPPING')
    then 'Taxes'
    when {{ metric_name }} in (
    'LEDGER_FBA_INVENTORY_PLACEMENT_SERVICE',
    'LEDGER_FBA_STORAGE_FEE',
    'LEDGER_RESTOCKING_FEE',
    'LEDGER_WAREHOUSE_DAMAGE',
    'LEDGER_WAREHOUSE_LOST_MANUAL'
    )
    then 'Warehousing'
    end

    {% endmacro %}
