{% snapshot snap_supplier %}

{{
    config(
        target_database='FULFILLMENT_DB',
        target_schema='SNAPSHOTS',
        unique_key='supplier_id',
        strategy='check',
        check_cols=['reliability_score', 'average_lead_time'],
        invalidate_hard_deletes=True,
    )
}}

select * from {{ source('raw', 'dim_supplier') }}

{% endsnapshot %}