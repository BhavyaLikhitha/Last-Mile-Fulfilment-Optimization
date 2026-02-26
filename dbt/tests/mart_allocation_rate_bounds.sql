{{ config(severity='error', tags=['marts','allocation','bounds']) }}

select *
from {{ ref('mart_allocation_efficiency') }}
where nearest_assignment_rate > 100
   or nearest_assignment_rate < 0