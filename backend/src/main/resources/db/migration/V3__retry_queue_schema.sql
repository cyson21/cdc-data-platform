create table if not exists cdc_retry_events (
    id bigserial primary key,
    event_id varchar(160) not null,
    source_connector varchar(120) not null,
    source_schema varchar(120) not null,
    source_table varchar(120) not null,
    source_primary_key varchar(300) not null,
    source_lsn numeric(20, 0) not null,
    source_offset jsonb not null,
    raw_payload jsonb not null,
    failure_reason text not null,
    retry_topic varchar(160) not null,
    attempt_count integer not null default 0,
    max_attempts integer not null,
    next_attempt_at timestamptz not null,
    retry_status varchar(40) not null,
    last_error text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    published_at timestamptz,
    unique (event_id)
);

create unique index if not exists uq_cdc_retry_events_source_event
    on cdc_retry_events(source_connector, source_table, source_primary_key, source_lsn);

create index if not exists idx_cdc_retry_events_ready
    on cdc_retry_events(retry_status, next_attempt_at, id);
