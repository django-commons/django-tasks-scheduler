# Testing automatic cron duplication

A normal scheduler sweep can overlap a successful cron execution. Before the
ownership fix, the sweep loaded whole Task rows and later saved that snapshot.
If completion advanced the task first, the sweep could restore the old job
reference and success counter, then create another successor. The callback also
committed a temporary `job_name=NULL` before creating its successor; a sweep
could read and later save that temporary state.

`TestCronSchedulerRace` exercises both orderings using PostgreSQL connections and
real broker registries. It invokes the background `_reschedule_tasks()` path and
executes the job through `Queue.run_sync()`, including its real success callback
and completion bookkeeping. Events pause the scheduler after its first SQL read;
the second test also pauses successor creation until that read completes. These
timing controls make the race deterministic instead of depending on machine load.
The tests do not inject stale Task objects or write a missing job reference.

The tests fail on the base revision `13195a3`:

| Sweep reads | Scheduled successors | Successful runs |
| --- | --- | --- |
| Before completion | 2 | 0 (rewound from 1) |
| During successor creation | 2 | 1 |
| Expected after either ordering | 1 | 1 |

The ownership fix selects fresh task rows under a transaction and row lock.
Completion uses the same lock, and reconciles the next owner before committing.
Both tests then pass. PostgreSQL CI runs these tests; SQLite skips them because it
cannot reproduce the transaction and row-lock behavior.

## Run locally

Use dedicated disposable brokers: the test fixtures call Redis `FLUSHALL`.
The ports below keep the tests separate from development services on 6379/5432.
From the repository root:

```sh
docker run --detach --rm --name scheduler-race-redis \
  -p 127.0.0.1:16381:6379 redis:8-alpine
docker run --detach --rm --name scheduler-race-postgres \
  -p 127.0.0.1:15434:5432 \
  -e POSTGRES_USER=scheduler -e POSTGRES_PASSWORD=scheduler-test \
  -e POSTGRES_DB=scheduler postgres:17-alpine
uv sync --extra yaml --locked
```

Wait for `docker exec scheduler-race-postgres pg_isready -U scheduler` to succeed.
Then run from `testproject/`:

```sh
BROKER_PORT=16381 PGPORT=15434 FAKEREDIS=False \
  uv run --with 'psycopg[binary]' python manage.py test \
  --settings=testproject.postgres_settings \
  scheduler.tests.test_task_types.test_cron_scheduler_race --verbosity=2
```

## Observe the failing baseline

Keep the current checkout and its test settings, but load the unfixed library
from a separate worktree. Copy the current test configuration too: the baseline
configuration ignores `BROKER_PORT` and would connect to port 6379. The regression
fixture checks the effective port before flushing Redis and fails if it does not
match `BROKER_PORT`. These copies change only the test harness, not the baseline
library implementation. From the repository root:

```sh
SCHEDULER_BASELINE="$(mktemp -d)"
git worktree add --detach "$SCHEDULER_BASELINE" 13195a3
cp scheduler/tests/test_task_types/test_cron_scheduler_race.py \
  "$SCHEDULER_BASELINE/scheduler/tests/test_task_types/"
cp scheduler/tests/conf.py "$SCHEDULER_BASELINE/scheduler/tests/conf.py"
cd testproject
PYTHONPATH="$SCHEDULER_BASELINE" BROKER_PORT=16381 PGPORT=15434 FAKEREDIS=False \
  uv run --with 'psycopg[binary]' python manage.py test \
  --settings=testproject.postgres_settings \
  scheduler.tests.test_task_types.test_cron_scheduler_race --verbosity=2
```

Expect two assertion failures showing the counts above. Run the same command
without `PYTHONPATH` to test the fix again. When finished, stop the disposable
containers with `docker stop scheduler-race-postgres scheduler-race-redis`.
