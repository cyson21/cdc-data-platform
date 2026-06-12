select
    event_date,
    applicant_id,
    job_posting_id,
    job_title,
    applicant_stage,
    evaluation_id,
    evaluation_score,
    evaluation_result,
    agent_task_id,
    agent_task_status,
    lineage_status
from {{ source('cdc_lakehouse', 'hiring_pipeline_lineage') }}
