{% snapshot snap_product %}

{{
    config(
        target_database='FULFILLMENT_DB',
        target_schema='SNAPSHOTS',
        unique_key='product_id',
        strategy='check',
        check_cols=['cost_price', 'selling_price'],
        invalidate_hard_deletes=True,
    )
}}

select * from {{ source('raw', 'dim_product') }}

{% endsnapshot %}