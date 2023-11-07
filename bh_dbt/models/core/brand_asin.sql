with product_report as (
    select 
    distinct 
    channel_product_id, 
    case 
        when p.brand ilike '%spot%' then 'SPOT'
        when p.brand ilike any ('%buddyph%','%onanoff%') then 'ONANOFF'
        when p.brand ilike '%cellini%' then 'Cellini'
        when p.brand ilike '%POP%' then 'POP'
        when p.brand ilike '%storyph%' then 'Storyphones'
        when p.brand ilike '%zens%' then 'ZENS'
        when p.brand ilike '%qisten%' then 'Qisten'
    else p.brand
    end as brand
    from {{var('readable')}}.report.report_product_latest_version p
)

with sku_level as (
    select 
    distinct 
    channel_product_id,
    case when 
    when pl.sku ilike '%ZED%' or pl.sku in (
        'ZEAPM03/00',
        'ZESC08W') then 'ZENS'
        when pl.sku ilike any ('%-BP-%','BP-%','%-ON-%','ON-%') then 'ONANOFF'
        when pl.sku ilike '%POP%' then 'POP'
        when pl.sku ilike '%SPOT%' then 'SPOT'
        when pl.sku ilike '%SPOT%' then 'SPOT'
        when pl.sku ilike '%QI%' then 'Qisten'
    end as brand
    from {{var('readable')}}.finance.finance_product_profit_loss pl
    where brand is not null
)

select distinct 
channel_product_id, brand
from (select * from product_report union all select * from sku_level)