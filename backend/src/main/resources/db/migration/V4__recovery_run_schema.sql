create table if not exists cdc_recovery_runs (
    id bigserial primary key,
    recovery_run_id varchar(120) not null unique,
    scenario varchar(80) not null,
    connector_name varchar(120) not null,
    source_table varchar(120) not null,
    source_primary_key varchar(300) not null,
    source_lsn_from numeric(20, 0) not null,
    source_lsn_to numeric(20, 0) not null,
    source_offset_from jsonb not null,
    source_offset_to jsonb not null,
    event_id varchar(160) not null,
    recovery_status varchar(40) not null,
    recovered_event_count bigint not null default 0,
    duplicate_event_count bigint not null default 0,
    gap_detected boolean not null default false,
    details jsonb not null,
    outage_started_at timestamptz,
    recovered_at timestamptz,
    created_at timestamptz not null default now()
);

create unique index if not exists uq_cdc_recovery_runs_source_boundary
    on cdc_recovery_runs(
        scenario,
        connector_name,
        source_table,
        source_primary_key,
        source_lsn_from,
        source_lsn_to,
        event_id
    );

create index if not exists idx_cdc_recovery_runs_scenario_time
    on cdc_recovery_runs(scenario, connector_name, recovered_at desc, created_at desc);
