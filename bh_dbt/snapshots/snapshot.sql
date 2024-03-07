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

select * from datahawk_share_83514.CUSTOM_83514.finance_profit_ledger

{% endsnapshot %}
