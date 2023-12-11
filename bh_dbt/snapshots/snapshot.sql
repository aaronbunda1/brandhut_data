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

