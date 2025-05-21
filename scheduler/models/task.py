import math
from datetime import timedelta, datetime
from typing import Dict, Any, Optional

import croniter
from django.conf import settings as django_settings
from django.contrib import admin
from django.contrib.contenttypes.fields import GenericRelation
from django.core.exceptions import ValidationError
from django.core.mail import mail_admins
from django.db import models
from django.templatetags.tz import utc
from django.urls import reverse
from django.utils import timezone
from django.utils.safestring import mark_safe
from django.utils.translation import gettext_lazy as _

from scheduler import settings
from scheduler.helpers.callback import Callback
from scheduler.helpers.queues import Queue
from scheduler.helpers.queues import get_queue
from scheduler.redis_models import JobModel
from scheduler.settings import logger, get_queue_names
from scheduler.types import ConnectionType, TASK_TYPES
from .args import TaskArg, TaskKwarg
from ..helpers import utils


def _get_task_for_job(job: JobModel) -> Optional["Task"]:
    if job.task_type is None or job.scheduled_task_id is None:
        return None
    task = Task.objects.filter(id=job.scheduled_task_id).first()
    return task


def failure_callback(job: JobModel, connection, result, *args, **kwargs):
    task = _get_task_for_job(job)
    if task is None:
        logger.warn(f"Could not find task for job {job.name}")
        return
    mail_admins(
        f"Task {task.id}/{task.name} has failed",
        "See django-admin for logs",
    )
    task.job_name = None
    task.failed_runs += 1
    task.last_failed_run = timezone.now()
    task.save(schedule_job=True)


def success_callback(job: JobModel, connection: ConnectionType, result: Any, *args, **kwargs):
    task = _get_task_for_job(job)
    if task is None:
        logger.warn(f"Could not find task for job {job.name}")
        return
    task.job_name = None
    task.successful_runs += 1
    task.last_successful_run = timezone.now()
    task.save(schedule_job=True)


def get_queue_choices():
    queue_names = get_queue_names()
    return [(queue, queue) for queue in queue_names]


class TaskType(models.TextChoices):
    CRON = "CronTaskType", _("Cron Task")
    REPEATABLE = "RepeatableTaskType", _("Repeatable Task")
    ONCE = "OnceTaskType", _("Run once")


class Task(models.Model):
    class TimeUnits(models.TextChoices):
        SECONDS = "seconds", _("seconds")
        MINUTES = "minutes", _("minutes")
        HOURS = "hours", _("hours")
        DAYS = "days", _("days")
        WEEKS = "weeks", _("weeks")

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    name = models.CharField(_("name"), max_length=128, unique=True, help_text=_("Name of the job"))
    task_type = models.CharField(_("Task type"), max_length=32, choices=TaskType.choices, default=TaskType.ONCE)
    callable = models.CharField(_("callable"), max_length=2048)
    callable_args = GenericRelation(TaskArg, related_query_name="args")
    callable_kwargs = GenericRelation(TaskKwarg, related_query_name="kwargs")
    enabled = models.BooleanField(
        _("enabled"),
        default=True,
        help_text=_(
            "Should job be scheduled? This field is useful to keep past jobs that should no longer be scheduled"
        ),
    )
    queue = models.CharField(_("queue"), max_length=255, choices=get_queue_choices, help_text=_("Queue name"))
    job_name = models.CharField(
        _("job name"), max_length=128, editable=False, blank=True, null=True, help_text=_("Current job_name on queue")
    )
    at_front = models.BooleanField(
        _("At front"),
        default=False,
        help_text=_("When queuing the job, add it in the front of the queue"),
    )
    timeout = models.IntegerField(
        _("timeout"),
        blank=True,
        null=True,
        help_text=_(
            "Timeout specifies the maximum runtime, in seconds, for the job "
            "before it'll be considered 'lost'. Blank uses the default "
            "timeout."
        ),
    )
    result_ttl = models.IntegerField(
        _("result ttl"),
        blank=True,
        null=True,
        help_text=mark_safe(
            """The TTL value (in seconds) of the job result.<br/>
               -1: Result never expires, you should delete jobs manually. <br/>
                0: Result gets deleted immediately. <br/>
                >0: Result expires after n seconds."""
        ),
    )
    failed_runs = models.PositiveIntegerField(
        _("failed runs"),
        default=0,
        help_text=_("Number of times the task has failed"),
    )
    successful_runs = models.PositiveIntegerField(
        _("successful runs"),
        default=0,
        help_text=_("Number of times the task has succeeded"),
    )
    last_successful_run = models.DateTimeField(
        _("last successful run"),
        blank=True,
        null=True,
        help_text=_("Last time the task has succeeded"),
    )
    last_failed_run = models.DateTimeField(
        _("last failed run"),
        blank=True,
        null=True,
        help_text=_("Last time the task has failed"),
    )
    interval = models.PositiveIntegerField(
        _("interval"),
        blank=True,
        null=True,
        help_text=_("Interval for repeatable task"),
    )
    interval_unit = models.CharField(
        _("interval unit"),
        max_length=12,
        choices=TimeUnits.choices,
        default=TimeUnits.HOURS,
        blank=True,
        null=True,
    )
    repeat = models.PositiveIntegerField(
        _("repeat"),
        blank=True,
        null=True,
        help_text=_("Number of times to run the job. Leaving this blank means it will run forever."),
    )
    scheduled_time = models.DateTimeField(_("scheduled time"), blank=True, null=True)
    cron_string = models.CharField(
        _("cron string"),
        max_length=64,
        blank=True,
        null=True,
        help_text=mark_safe(
            """Define the schedule in a crontab like syntax.
            Times are in UTC. Use <a href="https://crontab.guru/">crontab.guru</a> to create a cron string."""
        ),
    )

    def callable_func(self):
        """Translate callable string to callable"""
        return utils.callable_func(self.callable)

    @admin.display(boolean=True, description=_("is scheduled?"))
    def is_scheduled(self) -> bool:
        """Check whether a next job for this task is queued/scheduled to be executed"""
        if self.job_name is None:  # no job_id => is not scheduled
            return False
        # check whether job_id is in scheduled/queued/active jobs
        res = (
            (self.job_name in self.rqueue.scheduled_job_registry.all())
            or (self.job_name in self.rqueue.queued_job_registry.all())
            or (self.job_name in self.rqueue.active_job_registry.all())
        )
        # If the job_id is not scheduled/queued/started,
        # update the job_id to None. (The job_id belongs to a previous run which is completed)
        if not res:
            self.job_name = None
            super(Task, self).save()
        return res

    @admin.display(description="Callable")
    def function_string(self) -> str:
        args = self.parse_args()
        args_list = [repr(arg) for arg in args]
        kwargs = self.parse_kwargs()
        kwargs_list = [k + "=" + repr(v) for (k, v) in kwargs.items()]
        return self.callable + f"({', '.join(args_list + kwargs_list)})"

    def parse_args(self):
        """Parse args for running the job"""
        args = self.callable_args.all()
        return [arg.value() for arg in args]

    def parse_kwargs(self):
        """Parse kwargs for running the job"""
        kwargs = self.callable_kwargs.all()
        return dict([kwarg.value() for kwarg in kwargs])

    def _next_job_id(self):
        addition = timezone.now().strftime("%Y%m%d%H%M%S%f")
        return f"{self.queue}:{self.id}:{addition}"

    def _enqueue_args(self) -> Dict:
        """Args for Queue.enqueue_call.
        Set all arguments for Queue.enqueue. Particularly:
        - set job timeout and ttl
        - ensure a callback to reschedule the job next iteration.
        - Set job-id to proper format
        - set job meta
        """
        res = dict(
            meta=dict(),
            task_type=self.task_type,
            scheduled_task_id=self.id,
            on_success=Callback(success_callback),
            on_failure=Callback(failure_callback),
            name=self._next_job_id(),
        )
        if self.at_front:
            res["at_front"] = self.at_front
        if self.timeout:
            res["timeout"] = self.timeout
        if self.result_ttl is not None:
            res["result_ttl"] = self.result_ttl
        if self.task_type == TaskType.REPEATABLE:
            res["meta"]["interval"] = self.interval_seconds()
            res["meta"]["repeat"] = self.repeat
        return res

    @property
    def rqueue(self) -> Queue:
        """Returns django-queue for job"""
        return get_queue(self.queue)

    def enqueue_to_run(self) -> bool:
        """Enqueue task to run now as a different instance from the scheduled task."""
        kwargs = self._enqueue_args()
        self.rqueue.create_and_enqueue_job(run_task, args=(self.task_type, self.id), when=None, **kwargs)
        return True

    def unschedule(self) -> bool:
        """Remove a job from django-queue.

        If a job is queued to be executed or scheduled to be executed, it will remove it.
        """
        if self.job_name is not None:
            self.rqueue.delete_job(self.job_name)
            self.job_name = None
        self.save(schedule_job=False)
        return True

    def _schedule_time(self) -> datetime:
        if self.task_type == TaskType.CRON:
            self.scheduled_time = get_next_cron_time(self.cron_string)
        elif self.task_type == TaskType.REPEATABLE:
            _now = timezone.now()
            if self.scheduled_time >= _now:
                return utc(self.scheduled_time) if django_settings.USE_TZ else self.scheduled_time
            gap = math.ceil((_now.timestamp() - self.scheduled_time.timestamp()) / self.interval_seconds())
            if self.repeat is None or self.repeat >= gap:
                self.scheduled_time += timedelta(seconds=self.interval_seconds() * gap)
                self.repeat = (self.repeat - gap) if self.repeat is not None else None
        return utc(self.scheduled_time) if django_settings.USE_TZ else self.scheduled_time

    def to_dict(self) -> Dict:
        """Export model to dictionary, so it can be saved as external file backup"""
        interval_unit = str(self.interval_unit) if self.interval_unit else None
        res = dict(
            model=str(self.task_type),
            name=self.name,
            callable=self.callable,
            callable_args=[
                dict(
                    arg_type=arg.arg_type,
                    val=arg.val,
                )
                for arg in self.callable_args.all()
            ],
            callable_kwargs=[
                dict(
                    arg_type=arg.arg_type,
                    key=arg.key,
                    val=arg.val,
                )
                for arg in self.callable_kwargs.all()
            ],
            enabled=self.enabled,
            queue=self.queue,
            repeat=getattr(self, "repeat", None),
            at_front=self.at_front,
            timeout=self.timeout,
            result_ttl=self.result_ttl,
            cron_string=getattr(self, "cron_string", None),
            scheduled_time=self._schedule_time().isoformat(),
            interval=getattr(self, "interval", None),
            interval_unit=interval_unit,
            successful_runs=getattr(self, "successful_runs", None),
            failed_runs=getattr(self, "failed_runs", None),
            last_successful_run=getattr(self, "last_successful_run", None),
            last_failed_run=getattr(self, "last_failed_run", None),
        )
        return res

    def get_absolute_url(self):
        model = self._meta.model.__name__.lower()
        return reverse(
            f"admin:scheduler_{model}_change",
            args=[
                self.id,
            ],
        )

    def __str__(self):
        func = self.function_string()
        return f"{self.task_type}[{self.name}={func}]"

    def _schedule(self) -> bool:
        """Schedule the next execution for the task to run.
        :returns: True if a job was scheduled, False otherwise.
        """
        self.refresh_from_db()
        if self.is_scheduled():
            logger.debug(f"Task {self.name} already scheduled")
            return False
        if not self.enabled:
            logger.debug(f"Task {str(self)} disabled, enable task before scheduling")
            return False
        schedule_time = self._schedule_time()
        if self.task_type in {TaskType.REPEATABLE, TaskType.ONCE} and schedule_time < timezone.now():
            logger.debug(f"Task {str(self)} scheduled time is in the past, not scheduling")
            return False
        kwargs = self._enqueue_args()
        job = self.rqueue.create_and_enqueue_job(
            run_task,
            args=(self.task_type, self.id),
            when=schedule_time,
            **kwargs,
        )
        self.job_name = job.name
        return True

    def save(self, **kwargs):
        schedule_job = kwargs.pop("schedule_job", True)
        update_fields = kwargs.get("update_fields", None)
        if update_fields is not None:
            kwargs["update_fields"] = set(update_fields).union({"updated_at"})
        super(Task, self).save(**kwargs)
        if schedule_job:
            self._schedule()
            super(Task, self).save()

    def delete(self, **kwargs):
        self.unschedule()
        super(Task, self).delete(**kwargs)

    def interval_seconds(self):
        kwargs = {
            self.interval_unit: self.interval,
        }
        return timedelta(**kwargs).total_seconds()

    def clean_callable(self):
        try:
            utils.callable_func(self.callable)
        except Exception:
            raise ValidationError(
                {"callable": ValidationError(_("Invalid callable, must be importable"), code="invalid")}
            )

    def clean_queue(self):
        queue_names = get_queue_names()
        if self.queue not in queue_names:
            raise ValidationError(
                {
                    "queue": ValidationError(
                        "Invalid queue, must be one of: {}".format(", ".join(queue_names)), code="invalid"
                    )
                }
            )

    def clean_interval_unit(self):
        config = settings.SCHEDULER_CONFIG
        if config.SCHEDULER_INTERVAL > self.interval_seconds():
            raise ValidationError(
                _("Job interval is set lower than %(queue)r queue's interval. minimum interval is %(interval)"),
                code="invalid",
                params={"queue": self.queue, "interval": config.SCHEDULER_INTERVAL},
            )
        if self.interval_seconds() % config.SCHEDULER_INTERVAL:
            raise ValidationError(
                _("Job interval is not a multiple of rq_scheduler's interval frequency: %(interval)ss"),
                code="invalid",
                params={"interval": config.SCHEDULER_INTERVAL},
            )

    def clean_result_ttl(self) -> None:
        """Throws an error if there are repeats left to run and the result_ttl won't last until the next scheduled time.
        :return: None
        """
        if self.result_ttl and self.result_ttl != -1 and self.result_ttl < self.interval_seconds() and self.repeat:
            raise ValidationError(
                _(
                    "Job result_ttl must be either indefinite (-1) or "
                    "longer than the interval, %(interval)s seconds, to ensure rescheduling."
                ),
                code="invalid",
                params={"interval": self.interval_seconds()},
            )

    def clean_cron_string(self):
        try:
            croniter.croniter(self.cron_string)
        except ValueError as e:
            raise ValidationError({"cron_string": ValidationError(_(str(e)), code="invalid")})

    def clean(self):
        self.clean_queue()
        self.clean_callable()
        if self.task_type == TaskType.CRON:
            self.clean_cron_string()
        if self.task_type == TaskType.REPEATABLE:
            self.clean_interval_unit()
            self.clean_result_ttl()
        if self.task_type == TaskType.ONCE and self.scheduled_time is None:
            raise ValidationError({"scheduled_time": ValidationError(_("Scheduled time is required"), code="invalid")})
        if self.task_type == TaskType.ONCE and self.scheduled_time < timezone.now():
            raise ValidationError(
                {"scheduled_time": ValidationError(_("Scheduled time must be in the future"), code="invalid")}
            )


def get_next_cron_time(cron_string: Optional[str]) -> Optional[timezone.datetime]:
    """Calculate the next scheduled time by creating a crontab object with a cron string"""
    if cron_string is None:
        return None
    now = timezone.now()
    itr = croniter.croniter(cron_string, now)
    next_itr = itr.get_next(timezone.datetime)
    return next_itr


def get_scheduled_task(task_type_str: str, task_id: int) -> Task:
    # Try with new model names
    if task_type_str in TASK_TYPES:
        try:
            task_type = TaskType(task_type_str)
            task = Task.objects.filter(task_type=task_type, id=task_id).first()
            if task is None:
                raise ValueError(f"Job {task_type}:{task_id} does not exit")
            return task
        except ValueError:
            raise ValueError(f"Invalid task type {task_type_str}")
    raise ValueError(f"Job Model {task_type_str} does not exist, choices are {TASK_TYPES}")


def run_task(task_model: str, task_id: int) -> Any:
    """Run a scheduled job"""
    if isinstance(task_id, str):
        task_id = int(task_id)
    scheduled_task = get_scheduled_task(task_model, task_id)
    logger.debug(f"Running task {str(scheduled_task)}")
    args = scheduled_task.parse_args()
    kwargs = scheduled_task.parse_kwargs()
    res = scheduled_task.callable_func()(*args, **kwargs)
    return res
