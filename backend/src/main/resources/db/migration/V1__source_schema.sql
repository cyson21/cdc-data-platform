create table if not exists job_postings (
    id uuid primary key,
    title varchar(200) not null,
    department varchar(120) not null,
    status varchar(40) not null,
    opened_at timestamptz,
    closed_at timestamptz,
    updated_at timestamptz not null default now()
);

create table if not exists applicants (
    id uuid primary key,
    job_posting_id uuid not null references job_postings(id),
    email_hash varchar(128) not null,
    stage varchar(60) not null,
    deleted_at timestamptz,
    updated_at timestamptz not null default now()
);

create table if not exists evaluations (
    id uuid primary key,
    applicant_id uuid not null references applicants(id),
    score numeric(6, 2) not null,
    grade varchar(20) not null,
    status varchar(40) not null,
    evaluated_at timestamptz,
    updated_at timestamptz not null default now()
);

create table if not exists agent_tasks (
    id uuid primary key,
    applicant_id uuid not null references applicants(id),
    task_type varchar(80) not null,
    status varchar(40) not null,
    attempt_count integer not null default 0,
    updated_at timestamptz not null default now()
);

alter table job_postings replica identity full;
alter table applicants replica identity full;
alter table evaluations replica identity full;
alter table agent_tasks replica identity full;

create index if not exists idx_applicants_job_posting_id on applicants(job_posting_id);
create index if not exists idx_evaluations_applicant_id on evaluations(applicant_id);
create index if not exists idx_agent_tasks_applicant_id on agent_tasks(applicant_id);
