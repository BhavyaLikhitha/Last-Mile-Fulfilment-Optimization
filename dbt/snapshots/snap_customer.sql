{% snapshot snap_customer %}

{{
    config(
        target_database='FULFILLMENT_DB',
        target_schema='SNAPSHOTS',
        unique_key='customer_id',
        strategy='check',
        check_cols=['customer_segment', 'order_frequency_score'],
        invalidate_hard_deletes=True,
    )
}}

select * from {{ source('raw', 'dim_customer') }}

{% endsnapshot %}