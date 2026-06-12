create table if not exists pipeline_quality_sla_evaluations (
    id bigserial primary key,
    check_name varchar(120) not null,
    source_table varchar(120) not null,
    source_row_count bigint not null,
    raw_event_count bigint not null,
    canonical_event_count bigint not null,
    duplicate_event_count bigint not null default 0,
    missing_event_count bigint not null default 0,
    completeness_ratio double precision not null,
    duplicate_ratio double precision not null,
    freshness_lag_millis bigint not null,
    lag_millis bigint not null,
    min_completeness_ratio double precision not null,
    max_duplicate_ratio double precision not null,
    max_freshness_lag_millis bigint not null,
    max_lag_millis bigint not null,
    sla_status varchar(40) not null,
    violated_slo_ids jsonb not null default '[]'::jsonb,
    warning_slo_ids jsonb not null default '[]'::jsonb,
    details jsonb not null default '{}'::jsonb,
    evaluated_at timestamptz not null default now()
);

create index if not exists idx_pipeline_quality_sla_name_time
    on pipeline_quality_sla_evaluations(check_name, source_table, evaluated_at desc, id desc);
