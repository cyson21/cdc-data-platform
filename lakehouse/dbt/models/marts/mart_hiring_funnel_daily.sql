with applicant_events as (
    select
        event_date,
        stage,
        count(*) as event_count
    from {{ ref('curated_hiring_events') }}
    where source_table = 'applicants'
    group by 1, 2
)
select
    event_date,
    sum(case when stage = 'APPLIED' then event_count else 0 end) as applied_events,
    sum(case when stage = 'SCREENING' then event_count else 0 end) as screening_events,
    sum(event_count) as total_applicant_events
from applicant_events
group by 1
