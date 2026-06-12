create table if not exists cdc_event_ledger (
    id bigserial primary key,
    event_id varchar(160) not null,
    source_connector varchar(120) not null,
    source_schema varchar(120) not null,
    source_table varchar(120) not null,
    source_primary_key varchar(300) not null,
    source_lsn numeric(20, 0) not null,
    source_tx_id numeric(20, 0),
    source_offset jsonb not null,
    operation varchar(20) not null,
    processed_status varchar(40) not null,
    processed_at timestamptz,
    created_at timestamptz not null default now(),
    unique (event_id)
);

create table if not exists cdc_replay_requests (
    id bigserial primary key,
    request_id varchar(120) not null unique,
    requested_by varchar(120) not null,
    source_table varchar(120) not null,
    source_primary_key varchar(300),
    source_lsn_from numeric(20, 0),
    source_lsn_to numeric(20, 0),
    replay_status varchar(40) not null,
    result_summary jsonb,
    created_at timestamptz not null default now(),
    completed_at timestamptz
);

create table if not exists cdc_dlq_events (
    id bigserial primary key,
    event_id varchar(160) not null,
    source_connector varchar(120) not null,
    source_table varchar(120) not null,
    source_primary_key varchar(300) not null,
    source_lsn numeric(20, 0) not null,
    source_offset jsonb not null,
    raw_payload jsonb not null,
    failure_reason text not null,
    replay_status varchar(40) not null,
    created_at timestamptz not null default now(),
    replayed_at timestamptz
);

create table if not exists connector_health_snapshots (
    id bigserial primary key,
    connector_name varchar(120) not null,
    connector_status varchar(40) not null,
    task_status varchar(40),
    lag_millis bigint,
    offset_summary jsonb not null,
    captured_at timestamptz not null default now()
);

create table if not exists pipeline_quality_checks (
    id bigserial primary key,
    check_name varchar(120) not null,
    source_table varchar(120) not null,
    source_row_count bigint not null,
    raw_event_count bigint not null,
    canonical_event_count bigint not null,
    duplicate_event_count bigint not null default 0,
    missing_event_count bigint not null default 0,
    check_status varchar(40) not null,
    details jsonb,
    checked_at timestamptz not null default now()
);

create unique index if not exists uq_cdc_event_ledger_source_event
    on cdc_event_ledger(source_connector, source_table, source_primary_key, source_lsn);

create index if not exists idx_cdc_event_ledger_status on cdc_event_ledger(processed_status);
create index if not exists idx_cdc_dlq_events_replay_status on cdc_dlq_events(replay_status);
create index if not exists idx_connector_health_snapshots_name_time on connector_health_snapshots(connector_name, captured_at);
create index if not exists idx_pipeline_quality_checks_name_time on pipeline_quality_checks(check_name, checked_at);
