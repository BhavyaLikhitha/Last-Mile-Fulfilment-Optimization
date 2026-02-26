{% snapshot snap_driver %}

{{
    config(
        target_database='FULFILLMENT_DB',
        target_schema='SNAPSHOTS',
        unique_key='driver_id',
        strategy='check',
        check_cols=['warehouse_id', 'availability_status'],
        invalidate_hard_deletes=True,
    )
}}

select * from {{ source('raw', 'dim_driver') }}

{% endsnapshot %}