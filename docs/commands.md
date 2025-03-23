# Management commands

## `scheduler_worker` - Create a worker

Create a new worker with a scheduler for specific queues by order of priority.
If no queues are specified, will run on default queue only.

All queues must have the same redis settings on `SCHEDULER_QUEUES`.

```shell
usage: manage.py scheduler_worker [-h] [--pid PIDFILE] [--name NAME] [--worker-ttl WORKER_TTL] [--fork-job-execution FORK_JOB_EXECUTION] [--sentry-dsn SENTRY_DSN] [--sentry-debug] [--sentry-ca-certs SENTRY_CA_CERTS] [--burst]
                                  [--max-jobs MAX_JOBS] [--max-idle-time MAX_IDLE_TIME] [--with-scheduler] [--version] [-v {0,1,2,3}] [--settings SETTINGS] [--pythonpath PYTHONPATH] [--traceback] [--no-color] [--force-color]
                                  [--skip-checks]
                                  [queues ...]

positional arguments:
  queues                The queues to work on, separated by space, all queues should be using the same redis

options:
  -h, --help            show this help message and exit
  --pid PIDFILE         file to write the worker`s pid into
  --name NAME           Name of the worker
  --worker-ttl WORKER_TTL
                        Default worker timeout to be used
  --fork-job-execution FORK_JOB_EXECUTION
                        Fork job execution to another process
  --sentry-dsn SENTRY_DSN
                        Sentry DSN to use
  --sentry-debug        Enable Sentry debug mode
  --sentry-ca-certs SENTRY_CA_CERTS
                        Path to CA certs file
  --burst               Run worker in burst mode
  --max-jobs MAX_JOBS   Maximum number of jobs to execute before terminating worker
  --max-idle-time MAX_IDLE_TIME
                        Maximum number of seconds to wait for new job before terminating worker
  --with-scheduler      Run worker with scheduler, default to True
  --version             Show program's version number and exit.
  -v {0,1,2,3}, --verbosity {0,1,2,3}
                        Verbosity level; 0=minimal output, 1=normal output, 2=verbose output, 3=very verbose output
  --settings SETTINGS   The Python path to a settings module, e.g. "myproject.settings.main". If this isn't provided, the DJANGO_SETTINGS_MODULE environment variable will be used.
  --pythonpath PYTHONPATH
                        A directory to add to the Python path, e.g. "/home/djangoprojects/myproject".
  --traceback           Raise on CommandError exceptions.
  --no-color            Don't colorize the command output.
  --force-color         Force colorization of the command output.
  --skip-checks         Skip system checks.
```



## `export` - Export scheduled tasks

Export all scheduled tasks from django db to json/yaml format.

```shell
python manage.py export -o {yaml,json}
```

Result should be (for json):

```json
[
  {
    "model": "CronTaskType",
    "name": "Scheduled Task 1",
    "callable": "scheduler.tests.test_job",
    "callable_args": [
      {
        "arg_type": "datetime",
        "val": "2022-02-01"
      }
    ],
    "callable_kwargs": [],
    "enabled": true,
    "queue": "default",
    "at_front": false,
    "timeout": null,
    "result_ttl": null,
    "scheduled_time": "2023-02-21T14:06:06"
  },
  ...
]
```

## `import` - Import scheduled tasks

A json/yaml that was exported using the `export` command
can be imported to django.

- Specify the source file using `--filename` or take it from the standard input (default).
- Reset all scheduled tasks in the database before importing using `-r`/`--reset`.
- Update existing jobs for names that are found using `-u`/`--update`.

```shell
python manage.py import -f {yaml,json} --filename {SOURCE-FILE}
```

## `run_job` - Run a job immediately

Run a method in a queue immediately.

```shell
python manage.py run_job {callable} {callable args ...}
```

## `delete_failed_jobs` - delete failed jobs

Run this to empty failed jobs registry from a queue.

```shell
python manage.py delete_failed_jobs 
```

## `scheduler_stats` - Show scheduler stats

Prints scheduler stats as a table, json, or yaml, example:

```shell
$ python manage.py scheduler_stats

Django-Scheduler CLI Dashboard

--------------------------------------------------------------------------------
| Name             |    Queued |    Active |  Finished |  Canceled |   Workers |
--------------------------------------------------------------------------------
| default          |         0 |         0 |         0 |         0 |         0 |
| low              |         0 |         0 |         0 |         0 |         0 |
| high             |         0 |         0 |         0 |         0 |         0 |
| medium           |         0 |         0 |         0 |         0 |         0 |
| another          |         0 |         0 |         0 |         0 |         0 |
--------------------------------------------------------------------------------
```

```shell
usage: manage.py scheduler_stats [-h] [-j] [-y] [-i INTERVAL] [--version] [-v {0,1,2,3}] [--settings SETTINGS] [--pythonpath PYTHONPATH] [--traceback] [--no-color] [--force-color] [--skip-checks]

Print statistics

options:
  -h, --help            show this help message and exit
  -j, --json            Output statistics as JSON
  -y, --yaml            Output statistics as YAML
  -i INTERVAL, --interval INTERVAL
                        Poll statistics every N seconds
  --version             Show program's version number and exit.
  -v {0,1,2,3}, --verbosity {0,1,2,3}
                        Verbosity level; 0=minimal output, 1=normal output, 2=verbose output, 3=very verbose output
  --settings SETTINGS   The Python path to a settings module, e.g. "myproject.settings.main". If this isn't provided, the DJANGO_SETTINGS_MODULE environment variable will be used.
  --pythonpath PYTHONPATH
                        A directory to add to the Python path, e.g. "/home/djangoprojects/myproject".
  --traceback           Raise on CommandError exceptions.
  --no-color            Don't colorize the command output.
  --force-color         Force colorization of the command output.
  --skip-checks         Skip system checks.

```