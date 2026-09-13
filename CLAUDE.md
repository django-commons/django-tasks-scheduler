# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Testing

The tests live in `scheduler/tests/`, but they are run from `testproject/` — a minimal Django project
wired to the scheduler app, which supplies the settings module. Django's test runner, not pytest.

```bash
cd testproject/
uv sync --extra yaml

# Full test suite
uv run python manage.py test --exclude-tag multiprocess scheduler

# Single module / class / method
uv run python manage.py test scheduler.tests.test_settings
uv run python manage.py test scheduler.tests.test_settings.TestWorkerAdmin
uv run python manage.py test scheduler.tests.test_settings.TestWorkerAdmin.test_scheduler_config_as_dict

# Multiprocess tests (excluded above; they fork real workers and are slow)
uv run python manage.py test --tag multiprocess scheduler
```

`FAKEREDIS=True` runs the suite against an in-memory broker, with no Redis needed — the fastest way to
iterate. `BROKER_PORT` overrides the port when running against a real broker (6379 Redis, 6380 Valkey in CI).
There is no docker-compose file in the repo; for a real broker, run one yourself, e.g.
`redis-server --port 6379 --daemonize yes --save '' --appendonly no`.

CI also checks that the models and migrations agree:

```bash
cd testproject/ && uv run python manage.py makemigrations --check
```

### Linting & Formatting

```bash
ruff check --fix
ruff format
mypy scheduler/     # see caveat below
```

Pre-commit (`pre-commit install`) runs ruff check + ruff format, codespell, django-upgrade
(`--target-version 5.0`), pyproject-fmt, validate-pyproject, and the basic file hygiene hooks.
It does **not** run mypy, and neither does CI — `.github/workflows/test.yml` runs only the test matrix.
Ruff is therefore the only gate that actually blocks; keep `ruff check` and `ruff format --check` clean.

`mypy scheduler/` is configured `strict = true` but is **not** clean (~190 errors, concentrated in
`redis_models/`, `templatetags/`, `helpers/timeouts.py`, `admin/task_admin.py`), partly because
django-stubs is not a dev dependency, so Django base classes type as `Any`. Do not treat a non-empty
mypy run as a regression you caused; check whether your file was already failing before.

### Supported versions

`requires-python = ">=3.10"` and `django>=5`, but CI only exercises Python 3.11–3.14 (plus free-threaded
3.14t) against Django 5.2 and 6.0. Code must still import on 3.10 (`scheduler/types/settings_types.py`
falls back to `typing_extensions.Self`), but nothing tests it.

### Release

Version lives in `pyproject.toml` and is what release-drafter tags; `publish.yml` builds from pyproject on
a published release. `docs/changelog.md` is maintained by hand — add user-visible changes under `Unreleased`.

## Architecture

**django-tasks-scheduler** is a Django app providing an async task scheduler backed by Redis or Valkey.
Task *definitions* are persisted in Django's database; *execution* state lives entirely in the broker.

### Task types

One `Task` model (`scheduler/models/task.py`) with a `task_type` discriminator (`TaskType` text choices):

- `OnceTaskType` — one-time execution at `scheduled_time`
- `RepeatableTaskType` — repeats every `interval`/`interval_unit`, `repeat` times (blank = forever)
- `CronTaskType` — `cron_string` schedule, via croniter

`scheduler/types/broker_types.py` holds only the `TASK_TYPES` string list used for validation.

### Data flow

1. **Django DB** stores `Task` rows, with positional args (`TaskArg`) and keyword args (`TaskKwarg`)
   attached through generic relations (`scheduler/models/args.py`).
2. **WorkerScheduler** (`scheduler/worker/scheduler.py`) runs as a *thread* inside the worker process.
   Per loop it: takes/extends a per-queue `SchedulerLock`, gives every enabled task on its locked queues a
   pending job if it has none (`_reschedule_tasks`), then moves due jobs from the scheduled registry into
   the queued registry. Queues whose lock it loses are dropped from its set.
3. **Worker** (`scheduler/worker/worker.py`) blocks on the queued registries (`BZPOPMIN`), forks a child per
   job by default, and monitors it. `SimpleWorker`/`--fork-job-execution false` runs in-process.
4. **Redis models** (`scheduler/redis_models/`) mirror all execution state: `JobModel` (hash),
   `WorkerModel` (hash), the six per-queue registries (sorted sets), `Result` (stream), and the locks.

### Key abstractions

- **`@job` decorator** (`scheduler/decorators.py`): adds `.delay()` to a callable and records its dotted
  path in `JOB_METHODS_LIST`, which the admin's `callable` field offers as datalist suggestions.
- **`SchedulerConfiguration` / `QueueConfiguration`** (`scheduler/types/settings_types.py`): dataclasses
  loaded from `SCHEDULER_CONFIG` and `SCHEDULER_QUEUES`. `scheduler/settings.py` parses them at import
  time into module globals — note `conf_settings()` *rebinds* `SCHEDULER_CONFIG`, so tests that need to
  change it must mutate the live object rather than reassign the module attribute (see
  `scheduler/tests/conf.py`). `QueueConfiguration.__post_init__` validates connection params: `HOST`
  requires both `PORT` and `DB`, and exactly one of `URL`/`UNIX_SOCKET_PATH`/`HOST` may be set.
- **Queue** (`scheduler/helpers/queues/`): one interface over Redis, Valkey and FakeRedis.
  `create_and_enqueue_job()` is the central entry point; `getters.py` builds connections and implements
  the fail-fast probing used by admin views (`FAIL_FAST_QUEUE_PROBING`).
- **Worker commands** (`scheduler/worker/commands/`): JSON messages over a per-worker pub/sub channel,
  self-registering by `command_name` via `__init_subclass__` — a command class only exists once its module
  is imported, so new commands must be exported from `commands/__init__.py`.

### Invariants worth preserving

These encode fixes that are easy to undo by accident; the surrounding comments explain each in place.

- A recurring task's chain is owned by the job named in `Task.job_name`. Completion callbacks schedule a
  successor only when the finishing job *is* that job (`_complete_run`), otherwise a manual "Enqueue now"
  starts a second, self-sustaining chain.
- Writes to a `Task` that may be stale are restricted to `_SCHEDULING_FIELDS`, and `_RUN_STATE_FIELDS` are
  re-read before saving (`_refresh_run_state`). Run counters are incremented with `F()` expressions, never
  read-modify-write.
- The worker closes Django DB connections before every fork and after any maintenance pass that ran ORM
  code (`scheduler/helpers/db.py` explains what a shared TLS socket does to a forked child).
- Broker round trips are deliberately batched — registry counts, existence checks, per-task job indexes,
  admin list pages. Adding a per-row query or per-job round trip undoes deliberate work; prefer the
  pipelined `*_many` helpers.
- Job args/kwargs are pickled into the job hash. Anything enqueued must be picklable, and job payloads
  from the broker are trusted input.

### Admin & views

`scheduler/admin/` registers `TaskAdmin` plus two unmanaged placeholder models (`Queue`, `Worker` in
`scheduler/models/ephemeral_models.py`) whose changelists delegate to `scheduler/views/`. The views serve
queue stats, registry job lists, job detail/actions (enqueue, requeue, cancel, delete) and worker pages;
URLs are in `scheduler/urls.py`. All of them are `@staff_member_required`, except `stats_json`, which also
accepts a token checked by `SCHEDULER_CONFIG.TOKEN_VALIDATION_METHOD`.

### Test infrastructure

`SchedulerBaseCase` (`scheduler/tests/testtools.py`) creates a superuser and flushes the broker between
tests; `task_factory()` / `taskarg_factory()` build model instances. `scheduler/tests/conf.py` defines the
test queues — several point at deliberately unreachable brokers (refused ports, fake sentinels) to exercise
bad-configuration paths, with retries disabled so they fail fast. `scheduler/tests/jobs.py` holds the
callables tests enqueue.
