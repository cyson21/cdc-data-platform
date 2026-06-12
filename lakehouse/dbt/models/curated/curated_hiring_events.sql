select
    event_id,
    event_date,
    source_table,
    source_primary_key,
    operation,
    stage,
    agent_task_status
from {{ source('cdc_lakehouse', 'curated_hiring_events') }}
