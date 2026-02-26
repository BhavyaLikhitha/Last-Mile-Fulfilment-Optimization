/*
    stg_experiment_assignments — Silver layer
    Source: RAW fact_experiment_assignments
    Transformations:
      - Type casting
      - NULL handling
*/

with source as (
    select * from {{ source('raw', 'fact_experiment_assignments') }}
),

transformed as (
    select
        cast(assignment_id as varchar(30)) as assignment_id,
        cast(experiment_id as varchar(20)) as experiment_id,
        cast(order_id as varchar(30)) as order_id,
        cast(group_name as varchar(20)) as group_name,
        cast(assigned_at as timestamp) as assigned_at,
        cast(warehouse_id as varchar(20)) as warehouse_id,

        -- Audit
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        cast(batch_id as varchar(50)) as batch_id

    from source
    where experiment_id is not null
      and order_id is not null
)

select * from transformed