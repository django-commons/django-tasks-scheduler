# Changelog

## v4.0.7 🌈

### 🐛 Bug Fixes

- Fix: Scheduled jobs fail to execute: {'scheduled_time': ['Scheduled time must be in the future']} #297
- Fix Error with deserialize JobModel due to multi-processing #291

### 🧰 Maintenance

- Improve GitHub workflow performance using ruff action @DhavalGojiya #294

## v4.0.6 🌈

### 🐛 Bug Fixes

- Issue on task admin showing list of jobs where jobs have been deleted from broker #285
- Task admin view shows next run currently #288

### 🧰 Maintenance

- Added typing to many files

## v4.0.5 🌈

### 🐛 Bug Fixes

- fix:repeatable task without start date #276
- fix:admin list of tasks showing local datetime #280
- fix:wait for job child process using os.waitpid #281

### 🧰 Maintenance

- refactor some tests

## v4.0.4 🌈

### 🐛 Bug Fixes

- Issue when `SCHEDULER_CONFIG` is a `dict` #273
- Do not warn about _non_serializable_fields #274

### 🧰 Maintenance

- Fix gha zizmor findings
- Update dependencies to latest versions

## v4.0.3 🌈

### 🐛 Bug Fixes

- Updated `scheduler_worker` management command argument to `--without-scheduler` since the worker has a scheduler by
  default.

## v4.0.2 🌈

### 🐛 Bug Fixes

- Add type hint for `JOB_METHODS_LIST`
- Fix issue creating new `ONCE` task without a scheduled time #270

### 🧰 Maintenance

- Update dependencies to latest versions
- Migrate to use `uv` instead of `poetry` for package management

## v4.0.0 🌈

See breaking changes in 4.0.0 beta versions.

### 🐛 Bug Fixes

- Fix issue with non-primitive parameters for @job #249

## v4.0.0b3 🌈

Refactor the code to make it more organized and easier to maintain. This includes:

- All types are under `types` instead of separated to `broker_types` and `settings_types`.
- Added `__all__` to `models`, and other packages.

## v4.0.0b2 🌈

### 🐛 Bug Fixes

- Fix bug when `SCHEDULER_CONFIG` is `SchedulerConfiguration`

## v4.0.0b1 🌈

### Breaking Changes

This version is a full revamp of the package. The main changes are related to removing the RQ dependency.
Worker/Queue/Job are all implemented in the package itself. This change allows for more flexibility and control over
the tasks.

Management commands:

- `rqstats` => `scheduler_stats`
- `rqworker` => `scheduler_worker`

Settings:

- `SCHEDULER_CONFIG` is now a `SchedulerConfiguration` object to help IDE guide settings.
- `SCHEDULER_QUEUES` is now a list of `QueueConfiguration` objects to help IDE guide settings.
- Configuring queue to use `SSL`/`SSL_CERT_REQS`/`SOCKET_TIMEOUT` is now done using `CONNECTION_KWARGS` in
  `QueueConfiguration`
  ```python
  SCHEDULER_QUEUES: Dict[str, QueueConfiguration] = {
    'default': QueueConfiguration(
        HOST='localhost',
        PORT=6379,
        USERNAME='some-user',
        PASSWORD='some-password',
        CONNECTION_KWARGS={  # Eventual additional Broker connection arguments
            'ssl_cert_reqs': 'required',
            'ssl':True,
        },
    ),
   # ...
   }
   ```
- For how to configure in `settings.py`, please see the [settings documentation](./configuration.md).

## v3.0.2 🌈

### 🐛 Bug Fixes

- Fix issue updating wrong field #233

## v3.0.1 🌈

### 🐛 Bug Fixes

- Lookup error issue #228.

### 🧰 Maintenance

- Migrated to use ruff instead of flake8/black

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