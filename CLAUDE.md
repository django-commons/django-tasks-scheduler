# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Testing

Tests live in `testproject/` and use Django's built-in test runner (not pytest).

```bash
cd testproject/
uv sync --extra yaml

# Full test suite
uv run python manage.py test --exclude-tag multiprocess scheduler

# Single module
uv run python manage.py test scheduler.tests.test_settings

# Single class
uv run python manage.py test scheduler.tests.test_settings.TestWorkerAdmin

# Single method
uv run python manage.py test scheduler.tests.test_settings.TestWorkerAdmin.test_scheduler_config_as_dict

# Multiprocess tests (excluded by default)
uv run python manage.py test --tag multiprocess scheduler
```

Set `FAKEREDIS=True` to avoid needing a real Redis instance. Set `BROKER_PORT` to override the default port (6379 for Redis, 6380 for Valkey).

### Linting & Formatting

```bash
ruff check --fix
ruff format
mypy scheduler/
```

Pre-commit hooks run ruff, codespell, mypy, and pyproject validation. Install with `pre-commit install`.

### Local Broker

```bash
docker compose up -d   # starts Redis (6379) and Valkey (6380)
```

## Architecture

**django-tasks-scheduler** is a Django app providing an async task scheduler backed by Redis or Valkey. Tasks are persisted in Django's database; job execution state lives in Redis.

### Task Types

Three task types are defined in `scheduler/types/`:
- `OnceTaskType` — one-time execution at a scheduled time
- `RepeatableTaskType` — repeats at a fixed interval
- `CronTaskType` — cron expression schedule

### Data Flow

1. **Django DB** stores `Task` model instances (in `scheduler/models/`). Each task has positional args (`TaskArg`) and keyword args (`TaskKwarg`) via Django generic relations.
2. **WorkerScheduler** (`scheduler/worker/`) periodically reads enabled tasks from the DB and enqueues them as jobs into Redis using the queue abstraction in `scheduler/helpers/queues/`.
3. **Worker** (`scheduler/worker/`) dequeues and executes jobs. Multiple workers coordinate via `SchedulerLock` (distributed lock in Redis) to avoid duplicate scheduling.
4. **Redis models** (`scheduler/redis_models/`) mirror job state: `JobModel`, `ScheduledJobRegistry`, `WorkerModel`.

### Key Abstractions

- **`@job` decorator** (`scheduler/decorators.py`): Adds a `.delay()` method to any callable, enqueuing it on the named queue.
- **`SchedulerConfiguration`** / **`QueueConfiguration`** (`scheduler/types/`): Dataclasses loaded from `SCHEDULER_CONFIG` and `SCHEDULER_QUEUES` in Django settings. Controls timeouts, TTLs, scheduler interval, callbacks, and broker connection details.
- **Queue** (`scheduler/helpers/queues/`): Single interface over Redis, Valkey, and FakeRedis. Central method: `create_and_enqueue_job()`.

### Admin & Views

`scheduler/admin/` provides Django admin classes for Task, Queue, and Worker monitoring. `scheduler/views/` adds queue statistics and job management endpoints (pause/resume/delete). URLs are registered in `scheduler/urls.py`.

### Test Infrastructure

Base test class `SchedulerBaseCase` (`scheduler/tests/testtools.py`) sets up queues and FakeRedis. Use `task_factory()` helpers to create Task instances in tests. The `testproject/` directory is a minimal Django project wired to the scheduler app.
