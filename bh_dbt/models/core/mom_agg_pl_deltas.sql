select
date_trunc(month,date_day) as month,
pl.brand, 
sum()
from {{ref('product_pl')}} pl