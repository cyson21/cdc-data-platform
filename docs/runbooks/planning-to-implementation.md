# Planning To Implementation Runbook

## Purpose

Project 05 기획이 끝난 뒤 구현을 시작할 때 따라야 할 순서입니다. 이 문서는 구현 시작 전 경계와 첫 작업 단위를 고정합니다.

## Preflight

Run from `/Users/chanyang.son/Documents/side-projects/repos/cdc-data-platform`.

```bash
pwd
git status --short --branch
git rev-parse --show-toplevel
```

Expected before independent repo init:

```text
/Users/chanyang.son/Documents/side-projects/repos/cdc-data-platform
fatal: not a git repository
```

If `git rev-parse --show-toplevel` resolves to `/Users/chanyang.son/Documents/side-projects`, do not commit from the parent hub. Initialize a dedicated repo only after explicit approval.

## Read Order

1. `README.md`
7. `/Users/chanyang.son/Documents/side-projects/projects/05-cdc-data-platform/docs/2026-06-08-planning-completion-report.md`

## First Implementation Slice

Start with foundation plan Task 1 only.

1. Decide dedicated Git repo initialization.
3. Record the exact scope: no Project 01/02/03/04 changes, no AWS default dependency.
4. Then start backend skeleton.

## Do Not Start Yet

- Kafka outage runner before raw CDC capture works.
- Iceberg writer before canonical idempotency works.
- Real AWS S3/Athena/dbt-athena before local-first evidence exists.
- Phase 2 candidates before MVP M1-M6 evidence exists.

## Completion Rule

An implementation slice is complete only when these are all true:

- changed files are listed
- verification command and result are recorded
- source offset/LSN/event id evidence is present for CDC claims
- local-only vs real cloud evidence is labeled honestly
