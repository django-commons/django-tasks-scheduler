# Changelog

## v3.0.0 🌈

### Breaking Changes

- Renamed `REDIS_CLIENT_KWARGS` configuration to `CLIENT_KWARGS`.

### 🚀 Features

- Created a new `Task` model representing all kind of scheduled tasks.
    - In future versions, `CronTask`, `ScheduledTask` and `RepeatableTask` will be removed.
    - `Task` model has a `task_type` field to differentiate between the types of tasks.
    - Old tasks in the database will be migrated to the new `Task` model automatically.

### 🧰 Maintenance

- Update dependencies to latest versions.

## v2.1.1 🌈

### 🐛 Bug Fixes

- Support for valkey sentinel configuration @amirreza8002 (#191)
- Fix issue with task being scheduled despite being already scheduled #202

## v2.1.0 🌈

### 🚀 Features

- Support for custom job-class for every worker, using `--job-class` option in `rqworker` command. @gabriels1234 (#160)
- Support for integrating with sentry, using `--sentry-dsn`, `--sentry-debug`, and `--sentry-ca-certs` options in
  `rqworker` command.
- Support for using ValKey as broker instead of redis.

### 🧰 Maintenance

- Refactor settings module.

## v2.0.0 🌈

### Breaking Changes

- Remove support for django 3.* and 4.*. Only support django 5.0 and above.

## v1.3.4 🌈

### 🧰 Maintenance

- Update dependencies to latest versions

## v1.3.3 🌈

### 🐛 Bug Fixes

- Fix issue of django generating a new migration when settings.SCHEDULER_QUEUES is changed #119

## v1.3.2 🌈

- Fix issue with job_details template on python3.12 @cyber237 #87

## v1.3.1 🌈

### 🐛 Bug Fixes

- Fix workers' page when there are no queues #83

### 🧰 Maintenance

- Removes psycopg2 dependency from pyproject.toml @mbi (#78)

## v1.3.0 🌈

### 🚀 Features

- Add to CronTask and RepeatableTask counters for successful/failed runs.

### 🧰 Maintenance

- Support for django 5.0
- Update homepage url @dirkmueller (#65)

## v1.2.4 🌈

### 🐛 Bug Fixes

- Fix for non-existent task @gabriels1234 (#62)

### 🧰 Maintenance

- Use rq `fetch_many`

## v1.2.3 🌈

### 🐛 Bug Fixes

- Fix When a job fails it becomes unscheduled #45

## v1.2.1 🌈

### 🐛 Bug Fixes

- Fix infinite loop on callback calling is_scheduled() #37.

## v1.2.0 🌈

### 🚀 Features

- Rename `*Job` models to `*Task` to differentiate.

## v1.1.0 🌈

### 🚀 Features

- Enable using stats view using api token
- Reverted, active jobs are not marked as scheduled as there is currently no new job instance for them.

### 🐛 Bug Fixes

- #32 Running jobs should be marked as scheduled jobs. @rstalbow (#33)

## v1.0.2 🌈

### 🧰 Maintenance

- Update dependencies

### 🐛 Bug Fixes

- Add missing migration and check for missing migrations

## v1.0.1 🌈

* Update dependencies
* Remove redundant log calls

## v1.0.0 🌈

* Migrated from django-rq-scheduler

<!--
### 🚀 Features

### 🐛 Bug Fixes

### 🧰 Maintenance
-->