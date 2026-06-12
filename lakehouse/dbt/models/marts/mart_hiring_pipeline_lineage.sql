with lineage as (
    select
        event_date,
        job_posting_id,
        job_title,
        applicant_id,
        lineage_status,
        evaluation_result,
        agent_task_status
    from {{ ref('curated_hiring_pipeline_lineage') }}
)
select
    event_date,
    job_posting_id,
    job_title,
    count(distinct applicant_id) as total_applicants,
    sum(case when lineage_status = 'COMPLETE' then 1 else 0 end) as complete_lineage_applicants,
    sum(case when lineage_status = 'APPLICANT_ONLY' then 1 else 0 end) as applicant_only_count,
    sum(case when evaluation_result = 'PASS' then 1 else 0 end) as passed_evaluations,
    sum(case when agent_task_status = 'COMPLETED' then 1 else 0 end) as completed_agent_tasks,
    case
        when count(distinct applicant_id) = 0 then 0
        else cast(sum(case when lineage_status = 'COMPLETE' then 1 else 0 end) as double) / count(distinct applicant_id)
    end as lineage_completion_rate
from lineage
group by 1, 2, 3
