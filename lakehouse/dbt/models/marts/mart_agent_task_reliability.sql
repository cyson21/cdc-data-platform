with agent_task_events as (
    select
        event_date,
        agent_task_status,
        count(*) as event_count
    from {{ ref('curated_hiring_events') }}
    where source_table = 'agent_tasks'
    group by 1, 2
)
select
    event_date,
    sum(case when agent_task_status = 'COMPLETED' then event_count else 0 end) as completed_task_events,
    sum(event_count) as total_task_events,
    case
        when sum(event_count) = 0 then 0
        else cast(sum(case when agent_task_status = 'COMPLETED' then event_count else 0 end) as double) / sum(event_count)
    end as completion_rate
from agent_task_events
group by 1
