# Management commands

## rqworker

Create a new worker with a scheduler for specific queues by order of priority.
If no queues are specified, will run on default queue only.

All queues must have the same redis settings on `SCHEDULER_QUEUES`.

```shell
python manage.py rqworker queue1 queue2 queue3

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
