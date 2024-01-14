# Django tasks Scheduler

[![Django CI](https://github.com/dsoftwareinc/django-tasks-scheduler/actions/workflows/test.yml/badge.svg)](https://github.com/dsoftwareinc/django-tasks-scheduler/actions/workflows/test.yml)
![badge](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/cunla/b756396efb895f0e34558c980f1ca0c7/raw/django-tasks-scheduler-4.json)
[![badge](https://img.shields.io/pypi/dm/django-tasks-scheduler)](https://pypi.org/project/django-tasks-scheduler/)
[![Open Source Helpers](https://www.codetriage.com/dsoftwareinc/django-tasks-scheduler/badges/users.svg)](https://www.codetriage.com/dsoftwareinc/django-tasks-scheduler)

---

A database backed async tasks scheduler for django.
This allows remembering scheduled tasks, their parameters, etc.

## Terminology

### Queue

A queue of messages between processes (main django-app process and worker usually).
This is implemented in `rq` package.

* A queue contains multiple registries for scheduled tasks, finished jobs, failed jobs, etc.

### Worker

A process listening to one or more queues **for jobs to be executed**, and executing jobs queued to be
executed.

### Scheduler

A process listening to one or more queues for **jobs to be scheduled for execution**, and schedule them
to be executed by a worker.

This is a subprocess of worker.

### Queued Job Execution

Once a worker listening to the queue becomes available, the job will be executed

### Scheduled Task Execution

A scheduler checking the queue periodically will check whether the time the job should be executed has come, and if so,
it will queue it.

* A job is considered scheduled if it is queued to be executed, or scheduled to be executed.
* If there is no scheduler, the job will not be queued to run.

### Scheduled Task

django models storing information about jobs. So it is possible to schedule using
django-admin and track their status.

There are three types of ScheduledTask.

* `Scheduled Task` - Run a job once, on a specific time (can be immediate).
* `Repeatable Task` - Run a job multiple times (limited number of times or infinite times) based on an interval
* `Cron Task` - Run a job multiple times (limited number of times or infinite times) based on a cron string

Scheduled jobs are scheduled when the django application starts, and after a scheduled task is executed.

## Scheduler sequence diagram

```mermaid
sequenceDiagram
    autonumber
    box Worker
        participant scheduler as Scheduler Process
    end
    box DB
        participant db as Database
        
    end
    box Redis queue
        participant queue as Queue
        participant schedule as Queue scheduled tasks
    end    
    loop Scheduler process - loop forever
        note over scheduler,schedule: Database interaction
        scheduler ->> db: Check for enabled tasks that should be scheduled
        critical There are tasks to be scheduled
            scheduler ->> schedule: Create a job for task that should be scheduled
        end
        note over scheduler,schedule: Redis queues interaction
        scheduler ->> schedule: check whether there are scheduled tasks that should be executed
        critical there are jobs that are scheduled to be executed
            scheduler ->> schedule: remove jobs to be scheduled
            scheduler ->> queue: enqueue jobs to be executed
        end
        scheduler ->> scheduler: sleep interval (See SCHEDULER_INTERVAL)
    end
```

## Worker sequence diagram

```mermaid
sequenceDiagram
    autonumber
    box Worker
        participant worker as Worker Process
    end
    box Redis queue
        participant queue as Queue
        participant finished as Queue finished jobs
        participant failed as Queue failed jobs
    end    
    loop Worker process - loop forever
        worker ->>+ queue: get the first job to be executed
        queue -->>- worker: A job to be executed or nothing 
        critical There is a job to be executed
            worker ->> queue: Remove job from queue
            worker ->> worker: Execute job
            critical Job ended successfully
                worker ->> finished: Write job result
            option Job ended unsuccessfully
                worker ->> failed: Write job result
            end
        option No job to be executed
            worker ->> worker: sleep 
        end       
    end
```

---

## Reporting issues or Features requests

Please report issues via [GitHub Issues](https://github.com/dsoftwareinc/django-tasks-scheduler/issues) .

---

## Acknowledgements

A lot of django-admin views and their tests were adopted from [django-rq](https://github.com/rq/django-rq).
