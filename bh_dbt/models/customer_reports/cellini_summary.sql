select 
date_trunc(month,date_day) as month,
sku, 
sum(units_sold) as unit_sales,
sum(reimbursed_product_unit),
sum(reimbursed_product_unit) /sum(units_sold) as return_rate,
sum(gross_sales) as gross_sales,
sum(reimbursed_product)+sum(reversal_reimbursed) as returned_sales,
sum(net_sales) as net_sales,
sum(total_referral_fees) as total_referral_fees,
sum(total_shipping_costs)+sum(total_warehousing_costs) as total_fba_shipping_warehousing,--includes storage
sum(total_other_marketing_costs) as total_other_marketing_costs,
sum(gross_profit)+sum(total_other_marketing_costs) as net_total_gross_profit,
sum(brandhut_commission) as brandhut_commission,
sum(total_advertising_costs) as total_advertising,
sum(0) as freight, --to be added
sum(brandhut_commission)+sum(total_advertising_costs)+freight as total_expenses,
sum(gross_profit)-total_expenses as total_invoice_amount
from {{ref('product_pl')}} pl
where 1=1
    and pl.brand ilike '%cellini%'
    and date_day < date_trunc(month,current_date())
group by 1,2