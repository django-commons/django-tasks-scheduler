# Management commands

## rqworker

Create a new worker with a scheduler for specific queues by order of priority.
If no queues are specified, will run on default queue only.

All queues must have the same redis settings on `SCHEDULER_QUEUES`.

```shell
usage: manage.py rqworker [-h] [--pid PIDFILE] [--burst] [--name NAME] [--worker-ttl WORKER_TTL] [--max-jobs MAX_JOBS] [--fork-job-execution FORK_JOB_EXECUTION]
                          [--job-class JOB_CLASS] [--version] [-v {0,1,2,3}] [--settings SETTINGS] [--pythonpath PYTHONPATH] [--traceback] [--no-color] [--force-color]
                          [--skip-checks]
                          [queues ...]

positional arguments:
  queues                The queues to work on, separated by space, all queues should be using the same redis

options:
  --pid PIDFILE         file to write the worker`s pid into
  --burst               Run worker in burst mode
  --name NAME           Name of the worker
  --worker-ttl WORKER_TTL
                        Default worker timeout to be used
  --max-jobs MAX_JOBS   Maximum number of jobs to execute before terminating worker
  --fork-job-execution FORK_JOB_EXECUTION
                        Fork job execution to another process
  --job-class JOB_CLASS
                        Jobs class to use
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



## export

Export all scheduled tasks from django db to json/yaml format.

```shell
python manage.py export -o {yaml,json}
```

Result should be (for json):

```json
[
  {
    "model": "ScheduledJob",
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

## import

A json/yaml that was exported using the `export` command
can be imported to django.

- Specify the source file using `--filename` or take it from the standard input (default).
- Reset all scheduled tasks in the database before importing using `-r`/`--reset`.
- Update existing jobs for names that are found using `-u`/`--update`.

```shell
python manage.py import -f {yaml,json} --filename {SOURCE-FILE}
```

## run_job

Run a method in a queue immediately.

```shell
python manage.py run_job {callable} {callable args ...}
```

## delete failed jobs

Run this to empty failed jobs registry from a queue.

```shell
python manage.py delete_failed_jobs 
```
