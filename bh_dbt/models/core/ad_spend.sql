with non_sp_data as (
    select
    seller_name,
    account_key,
    c.marketplace_key,
    case 
        when campaign_name ilike '%Cellini%' then 'Cellini'
        when campaign_name ilike '%zens%' then 'ZENS'
        when campaign_name ilike any ('%quisten%','%qis%') then 'Qisten'
        when campaign_name ilike '%storyph%' then 'Storyphones'
        when campaign_name ilike '%pop%' then 'Onanoff 2'
        when campaign_name ilike '%fokus'% then 'Fokus'
        when campaign_name ilike '%SPOT%' then 'SPOT'
        when campaign_name ilike any ('%onanoff%','%onanonff%','%onaonff%','%buddyph%','%explore plus%','%explore+%','%playear%','%play+%','%school plus%','%school+%','%Play%','%Cosmos%','%wave%','%headphones%') then 'ONANOFF'
        when campaign_name ilike any ('%health direct%','%urban nomad%','%eukonic%','%frhome%','%thirty%','%ev gear%','%hsa%') then 'Other Brands'
    else 'Unknown'
    end as brand,
    sponsored_type,
    c.date_day,
    sum(c.costs) as ad_spend
    from datahawk_share_83514.advertising.advertising_campaign_metrics c
    where c.sponsored_type != 'SponoredProducts'
    group by 1,2,3,4,5,6
)

,sp as (
    select
    seller_name,
        account_key,
        sp.marketplace_key,
        b.brand,
        sponsored_type,
        date_day,
        sum(costs) as ad_spend
    from datahawk_share_83514.advertising.advertising_product_metrics sp
    left join datahawk_writable_83514.test.brand_asin b
        on b.channel_product_id = sp.channel_product_id
        and b.marketplace_key = sp.marketplace_key
    group by 1,2,3,4,5,6
)

select * from non_sp_data 
union all 
select * from sp