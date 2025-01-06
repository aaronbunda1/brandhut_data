with product_report as (
    select 
    distinct 
    channel_product_id, 
    marketplace_key,
    case 
        when p.brand ilike '%spot%' then 'SPOT'
        when p.brand ilike '%storyph%' then 'Onanoff 2'
        when p.brand ilike any ('%tiny%','%tree%') then 'Tiny Tree Houses'
        when p.brand ilike '%zens%' then 'Zens'
        when p.channel_product_id in ('B0CHY732PN','B0CHY57B3X') then 'Fokus'
        when p.brand ilike any ('%buddyph%','%onanoff%') then 'ONANOFF'
        when p.brand ilike '%cellini%' then 'Cellini'
        when p.brand ilike '%POP%' then 'ONANOFF'
        when p.brand ilike '%zens%' then 'ZENS'
        when p.brand ilike '%qisten%' then 'Qisten'
        when p.brand ilike any ('%roku%','73%','%sunny%') then '73&Sunny'
    else p.brand
    end as brand
    from {{var('readable')['hawkspace']}}.reports.report_product_latest_version p
    where brand !='Qisten'
    qualify rank() over (partition by channel_product_id,marketplace_key order by observation_time desc) =1 
)

, sku_level as (
    select 
    distinct 
    channel_product_id,
    marketplace_key,
    {{ get_brand_from_sku('pl.sku') }} as brand
    from {{var('readable')['hawkspace']}}.finance.finance_product_profit_loss pl
)
, prededupe as (
select distinct 
channel_product_id, marketplace_key,channel_product_id||marketplace_key as key, brand
from 
(select coalesce(s.channel_product_id,p.channel_product_id) as channel_product_id,
coalesce(s.marketplace_key,p.marketplace_key) as marketplace_key,
coalesce(s.brand,p.brand) as brand
from sku_level s
full outer join product_report p using(channel_product_id,marketplace_key)
)
where brand is not null and channel_product_id != 'Unknown'
)

select * from prededupe
qualify count(*) over (partition by key) =1

union all
select *
from (select * from prededupe
qualify count(*) over (partition by key) >1
)
where brand = 'Tiny Tree Houses'
