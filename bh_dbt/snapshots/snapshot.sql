{% snapshot product_pl_snapshot %}

{{
    config(
      target_database='datahawk_writable_83514',
      target_schema='snapshots',
      unique_key='key',

      strategy='timestamp',
      updated_at='updated_at'
    )
}}

select * from datahawk_writable_83514.brandhut.product_pl

{% endsnapshot %}

{% snapshot product_pl_new_snapshot %}
{{
    config(
      target_database='datahawk_writable_83514',
      target_schema='snapshots',
      unique_key='key',
      strategy='timestamp',
      updated_at='updated_at'
    )
}}

select * from datahawk_writable_83514.brandhut.product_pl_new

{% endsnapshot %}



{% snapshot ledger_snapshot %}
{{
    config(
      target_database='datahawk_writable_83514',
      target_schema='snapshots',
      unique_key='key',
      strategy='timestamp',
      updated_at='updated_at'
    )
}}

select concat(
coalesce(cast(posted_local_date as varchar(30)),''),
coalesce(account_key,''),
coalesce(amazon_region_id::string,''),
coalesce(marketplace_key,''),
coalesce(currency,''),
coalesce(asin,''),
coalesce(sku,''),
coalesce(metric,''),
coalesce(original_description,''),
coalesce(order_id,'')
)
as key, 
cast(posted_local_date as varchar(30)),
account_key,
amazon_region_id,
marketplace_key,
currency,
asin,
sku,
metric,
original_description,
order_id,
current_timestamp() as updated_at,
sum(amount) as amount, 
sum(amount_usd) as amount_usd,
sum(quantity) as quantity
from datahawk_share_83514.CUSTOM_83514.finance_profit_ledger
where posted_local_date >= '2023-01-01'
group by all

{% endsnapshot %}
