import math
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any, Optional

import croniter
from django.conf import settings as django_settings
from django.contrib import admin
from django.contrib.contenttypes.fields import GenericRelation
from django.core.exceptions import ValidationError
from django.core.mail import mail_admins
from django.db import models, router, transaction
from django.db.models import F
from django.templatetags.tz import utc
from django.urls import reverse
from django.utils import timezone
from django.utils.safestring import mark_safe
from django.utils.translation import gettext_lazy as _

from scheduler import settings
from scheduler.helpers.callback import Callback
from scheduler.helpers.queues import Queue, get_queue
from scheduler.helpers.queues.queue_logic import get_current_job
from scheduler.redis_models import JobModel
from scheduler.settings import get_queue_names, logger
from scheduler.types import TASK_TYPES, ConnectionType
from scheduler.types.broker_types import BrokerErrorTypes

from ..helpers import utils
from . import cron
from .args import TaskArg, TaskKwarg

# Fields that `_schedule()` may change. Everything that writes back a task it read before a run finished - the
# completion callbacks and the scheduler loop - restricts itself to these, so a stale instance cannot restore an old
# job name or roll back the outcome counters.
_SCHEDULING_FIELDS = ("job_name", "scheduled_time", "repeat")

# Fields the run machinery owns. The completion callbacks maintain them and the admin marks them read-only, so they
# are never edited through a form and a save may re-read them instead of writing back what the instance holds -
# which, for an instance read before a run finished, is a stale job name and stale counters.
_RUN_STATE_FIELDS = ("job_name", "successful_runs", "last_successful_run", "failed_runs", "last_failed_run")


def _complete_task(job: JobModel, *, failed: bool) -> None:
    """Record the outcome of a finished job and, when that job owned the task's chain, schedule its successor."""
    if job.scheduled_task_id is None or job.meta.get(cron._SKIPPED):
        return
    using = job.meta.get("scheduler_task_database", "default")
    with transaction.atomic(using=using):
        task = Task.objects.using(using).select_for_update().filter(pk=job.scheduled_task_id).first()
        if task is None or not cron.same_generation(task, job):
            return
        now = timezone.now()
        if failed:
            counters: dict[str, Any] = {"failed_runs": F("failed_runs") + 1, "last_failed_run": now}
        else:
            counters = {"successful_runs": F("successful_runs") + 1, "last_successful_run": now}
        # Count in the database rather than through this instance. The row lock serializes completions
        # where the database has one; on SQLite it does not, and another run of the same task finishing
        # at the same time would otherwise drop this result.
        Task.objects.using(using).filter(pk=task.pk).update(updated_at=now, **counters)
        if task.job_name != job.name:
            # Not the task's pending execution - a manual "Enqueue now" run, or a leftover duplicate. The
            # scheduled job is still waiting, so scheduling a successor here would start a second chain.
            logger.debug(f"Job {job.name} is not the scheduled run of task {task.name}, not scheduling a successor")
        elif task.task_type == TaskType.CRON:
            task.job_name = None
            cron.reconcile(task, exclude=job.name)
        elif job.task_type != str(TaskType.CRON):
            # The finishing job is still in the active registry, so discount it rather than clearing the
            # pointer: a failed reschedule must not leave the task looking unscheduled to a racing sweep.
            if not task._schedule(exclude_job_name=job.name) and task.schedule_updated and task.job_name == job.name:
                task.job_name = None
            models.Model.save(task, using=using, update_fields=(*_SCHEDULING_FIELDS, "updated_at"))
    if failed:
        try:
            mail_admins(f"Task {task.pk}/{task.name} has failed", "See django-admin for logs")
        except Exception:
            # Reporting failure must not retry already committed completion bookkeeping.
            logger.exception("Could not report failed task %s", task.name)


def failure_callback(job: JobModel, connection: ConnectionType, result: Any, *args: Any, **kwargs: Any) -> None:
    _complete_task(job, failed=True)


def success_callback(job: JobModel, connection: ConnectionType, result: Any, *args: Any, **kwargs: Any) -> None:
    _complete_task(job, failed=False)


def get_queue_choices() -> list[tuple[str, str]]:
    queue_names = get_queue_names()
    return [(queue, queue) for queue in queue_names]


class TaskType(models.TextChoices):
    CRON = "CronTaskType", _("Cron Task")
    REPEATABLE = "RepeatableTaskType", _("Repeatable Task")
    ONCE = "OnceTaskType", _("Run once")


class Task(models.Model):
    #: False when the last save reached the database but could not reach the broker to update
    #: the schedule. The row is correct, the schedule is stale until the next scheduler sweep
    #: repairs it. Not a database field - it only describes the save that just ran.
    schedule_updated: bool = True

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

    def callable_func(self) -> Callable:
        """Translate callable string to callable"""
        return utils.callable_func(self.callable)

    @admin.display(boolean=True, description=_("is scheduled?"))  # type: ignore[misc]
    def is_scheduled(self) -> bool | None:
        """Check whether the job this task owns is queued/scheduled to be executed.

        This is a membership check on ``job_name`` for every task type: one pipelined read,
        cheap enough to run once per row in the admin changelist. Reconciliation needs the
        full registry sweep in ``cron.read_schedule`` instead, which is far more expensive.

        Returns None when the broker cannot be read, which the admin shows as an unknown
        state rather than a checkmark. Callers deciding whether to enqueue must treat None
        as "possibly already scheduled", never as False.
        """
        if self.job_name is None:
            return False
        try:
            with self.rqueue.connection.pipeline() as pipeline:
                self.rqueue.scheduled_job_registry.exists(pipeline, self.job_name)
                self.rqueue.queued_job_registry.exists(pipeline, self.job_name)
                self.rqueue.active_job_registry.exists(pipeline, self.job_name)
                return any(item is not None for item in pipeline.execute())
        except BrokerErrorTypes:
            logger.exception("Could not inspect task %s", self.name)
            return None

    @admin.display(description="Callable")  # type: ignore[misc]
    def function_string(self) -> str:
        args = self.parse_args()
        args_list = [repr(arg) for arg in args]
        kwargs = self.parse_kwargs()
        kwargs_list = [k + "=" + repr(v) for (k, v) in kwargs.items()]
        return self.callable + f"({', '.join(args_list + kwargs_list)})"

    def parse_args(self) -> list[Any]:
        """Parse args for running the job"""
        args = self.callable_args.all()
        return [arg.value() for arg in args]

    def parse_kwargs(self) -> dict[str, Any]:
        """Parse kwargs for running the job"""
        kwargs = self.callable_kwargs.all()
        return dict([kwarg.value() for kwarg in kwargs])

    def _next_job_id(self) -> str:
        addition = timezone.now().strftime("%Y%m%d%H%M%S%f")
        if self._state.db and self._state.db != "default":
            addition = f"{self._state.db}:{addition}"
        return f"{self.queue}:{self.id}:{addition}"

    def _enqueue_args(self) -> dict[str, Any]:
        """Args for Queue.enqueue_call.
        Set all arguments for Queue.enqueue. Particularly:
        - set job timeout and ttl
        - ensure a callback to reschedule the job next iteration.
        - Set job-id to proper format
        - set job meta
        """
        res = {
            "meta": {"scheduler_task_database": self._state.db or "default"},
            "task_type": self.task_type,
            "scheduled_task_id": self.id,
            "on_success": Callback(success_callback),
            "on_failure": Callback(failure_callback),
            "name": self._next_job_id(),
        }
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
        using = self._state.db or router.db_for_write(Task, instance=self)
        with transaction.atomic(using=using):
            current = Task.objects.using(using).select_for_update().get(pk=self.pk)
            kwargs = current._enqueue_args()
            if current.task_type == TaskType.CRON:
                kwargs["meta"][cron._MANUAL] = "1"
            current.rqueue.create_and_enqueue_job(run_task, args=(current.task_type, current.pk), when=None, **kwargs)
        return True

    def unschedule(self, *, using: str | None = None, enabled: bool | None = None) -> bool:
        """Remove waiting executions without deleting a running job or manual cron run.

        Only the schedule is written. Pass ``enabled`` to change that flag on the locked row
        as well; leaving it unset keeps the stored value, so a caller holding a stale
        instance cannot rewrite the flag as a side effect of dequeuing.
        """
        using = using or self._state.db or router.db_for_write(Task, instance=self)
        with transaction.atomic(using=using):
            current = Task.objects.using(using).select_for_update().get(pk=self.pk)
            if current.task_type == TaskType.CRON:
                cron.retire_schedule(current)
            elif current.job_name is not None:
                current.rqueue.delete_job(current.job_name)
            current.job_name = None
            update_fields = ["job_name", "updated_at"]
            if enabled is not None:
                current.enabled = enabled
                update_fields.append("enabled")
            models.Model.save(current, using=using, update_fields=update_fields)
            cron._copy_runtime(current, self)
            if enabled is not None:
                self.enabled = enabled
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

    def to_dict(self) -> dict[str, Any]:
        """Export model to dictionary, so it can be saved as external file backup"""
        interval_unit = str(self.interval_unit) if self.interval_unit else None
        res = {
            "model": str(self.task_type),
            "name": self.name,
            "callable": self.callable,
            "callable_args": [{"arg_type": arg.arg_type, "val": arg.val} for arg in self.callable_args.all()],
            "callable_kwargs": [
                {"arg_type": arg.arg_type, "key": arg.key, "val": arg.val} for arg in self.callable_kwargs.all()
            ],
            "enabled": self.enabled,
            "queue": self.queue,
            "repeat": getattr(self, "repeat", None),
            "at_front": self.at_front,
            "timeout": self.timeout,
            "result_ttl": self.result_ttl,
            "cron_string": getattr(self, "cron_string", None),
            "scheduled_time": self._schedule_time().isoformat(),
            "interval": getattr(self, "interval", None),
            "interval_unit": interval_unit,
            "successful_runs": getattr(self, "successful_runs", None),
            "failed_runs": getattr(self, "failed_runs", None),
            "last_successful_run": getattr(self, "last_successful_run", None),
            "last_failed_run": getattr(self, "last_failed_run", None),
        }
        return res

    def get_absolute_url(self) -> str:
        model = self._meta.model.__name__.lower()
        return reverse(f"admin:scheduler_{model}_change", args=[self.id])

    def __str__(self) -> str:
        func = self.function_string()
        return f"{self.task_type}[{self.name}={func}]"

    def _schedule(self, exclude_job_name: str | None = None) -> bool:
        """Schedule the next execution for the task to run.

        :param exclude_job_name: Name of a job that should not count as this task's pending execution. The completion
            callbacks pass the job that is finishing: it is still in the active registry while they run, so without
            this the task would look like it is already scheduled and never get a successor.
        :returns: True if a job was scheduled, False otherwise.
        """
        self.schedule_updated = True
        if self.job_name == exclude_job_name:
            scheduled = False
        else:
            scheduled = self.is_scheduled()
        if scheduled is None:
            self.schedule_updated = False
            logger.warning(f"Could not read the schedule for task {self.name}; not enqueuing another job")
            return False
        if scheduled:
            logger.debug(f"Task {self.name} already scheduled")
            return False
        if not self.enabled:
            logger.debug(f"Task {self!s} disabled, enable task before scheduling")
            return False
        schedule_time = self._schedule_time()
        if self.task_type in {TaskType.REPEATABLE, TaskType.ONCE} and schedule_time < timezone.now():
            logger.debug(f"Task {self!s} scheduled time is in the past, not scheduling")
            return False
        kwargs = self._enqueue_args()
        try:
            job = self.rqueue.create_and_enqueue_job(
                run_task, args=(self.task_type, self.id), when=schedule_time, **kwargs
            )
        except BrokerErrorTypes:
            # A successor's broker failure must not roll back the completed run's counters.
            self.schedule_updated = False
            logger.exception("Could not schedule task %s; leaving its job reference unchanged", self.name)
            return False
        self.job_name = job.name
        return True

    def _refresh_run_state(self, current: Optional["Task"]) -> None:
        """Take the fields the run machinery owns from the locked row, so saving a stale instance
        cannot undo a run that finished while the caller held it.

        Cron tasks get the wider ``cron._copy_runtime`` instead, which also carries the schedule.
        """
        if current is None:  # a new row, or one deleted underneath us
            return
        for field_name in _RUN_STATE_FIELDS:
            setattr(self, field_name, getattr(current, field_name))

    def save(self, **kwargs: Any) -> None:
        using = kwargs.get("using") or router.db_for_write(Task, instance=self)
        with transaction.atomic(using=using):
            current = Task.objects.using(using).select_for_update().filter(pk=self.pk).first() if self.pk else None
            self._save_locked(current, **kwargs)

    def _save_locked(self, current: Optional["Task"], **kwargs: Any) -> None:
        should_clean = kwargs.pop("clean", True)
        schedule_job = kwargs.pop("schedule_job", True)
        if kwargs.get("update_fields") is not None:
            fields = set(kwargs["update_fields"])
            if not fields:
                return
            kwargs["update_fields"] = fields | {"updated_at"}
            if current is not None:
                for field in self._meta.concrete_fields:
                    if field.name not in fields and field.attname not in fields:
                        setattr(self, field.attname, getattr(current, field.attname))
        involves_cron = self.task_type == TaskType.CRON or (current is not None and current.task_type == TaskType.CRON)
        identity_changed = current is not None and (current.queue, current.task_type) != (self.queue, self.task_type)
        if involves_cron:
            if current is not None:
                requested_time = self.scheduled_time
                cron._copy_runtime(current, self)
                if identity_changed:
                    self.scheduled_time = requested_time
            elif not self._state.adding:
                raise Task.DoesNotExist("Cannot save a task that was deleted")
        if should_clean:
            self.clean()
        if schedule_job and not involves_cron:
            self._refresh_run_state(current)
        if involves_cron and identity_changed and current is not None:
            cron.retire_schedule(current)
            self.job_name = None
            if kwargs.get("update_fields"):
                kwargs["update_fields"] |= {"job_name"}
        super().save(**kwargs)
        if schedule_job:
            if self.task_type == TaskType.CRON:
                self.schedule_updated = cron.reconcile(self)
            else:
                self._schedule()
                super().save(using=self._state.db, update_fields=(*_SCHEDULING_FIELDS, "updated_at"))

    def reschedule_if_needed(self) -> bool:
        """Give the task a pending job if it has none, writing back only the scheduling fields.

        Used by the scheduler loop, which works from instances read before the loop started: a full-row
        save there would restore an old job name a completion callback has since replaced, adding a
        second recurring chain. Cron tasks reconcile instead, which also clears any duplicate.

        :returns: True if the call gave the task a job it did not have.
        """
        if self.task_type == TaskType.CRON:
            previous = self.job_name
            self.schedule_updated = cron.reconcile(self)
            return self.job_name is not None and self.job_name != previous
        scheduled = self._schedule()
        self.save(schedule_job=False, clean=False, update_fields=_SCHEDULING_FIELDS)
        return scheduled

    def delete(self, **kwargs: Any) -> None:
        using = kwargs.get("using") or router.db_for_write(Task, instance=self)
        with transaction.atomic(using=using):
            self.unschedule(using=using)
            super().delete(**kwargs)

    def interval_seconds(self) -> float:
        kwargs = {
            self.interval_unit: self.interval,
        }
        return timedelta(**kwargs).total_seconds()

    def clean_callable(self) -> None:
        try:
            utils.callable_func(self.callable)
        except Exception:
            raise ValidationError(
                {"callable": ValidationError(_("Invalid callable, must be importable"), code="invalid")}
            )

    def clean_queue(self) -> None:
        queue_names = get_queue_names()
        if self.queue not in queue_names:
            raise ValidationError(
                {"queue": ValidationError(f"Invalid queue, must be one of: {', '.join(queue_names)}", code="invalid")}
            )

    def clean_interval_unit(self) -> None:
        config = settings.SCHEDULER_CONFIG
        if config.SCHEDULER_INTERVAL > self.interval_seconds():
            raise ValidationError(
                _("Job interval is set lower than %(queue)r queue's interval. minimum interval is %(interval)"),
                code="invalid",
                params={"queue": self.queue, "interval": config.SCHEDULER_INTERVAL},
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

    def clean_cron_string(self) -> None:
        try:
            croniter.croniter(self.cron_string)
        except ValueError as e:
            raise ValidationError({"cron_string": ValidationError(_(str(e)), code="invalid")})

    def clean(self) -> None:
        if self.task_type not in TaskType.values:
            raise ValidationError(
                {"task_type": ValidationError(_("Invalid task type"), code="invalid")},
            )
        self.clean_queue()
        self.clean_callable()
        if self.task_type == TaskType.CRON:
            self.clean_cron_string()
        if self.task_type == TaskType.REPEATABLE:
            self.clean_interval_unit()
            self.clean_result_ttl()
        if self.task_type == TaskType.REPEATABLE and self.scheduled_time is None:
            self.scheduled_time = timezone.now() + timedelta(seconds=2)
        if self.task_type == TaskType.ONCE and self.scheduled_time is None:
            raise ValidationError({"scheduled_time": ValidationError(_("Scheduled time is required"), code="invalid")})
        if self.task_type == TaskType.ONCE and self.scheduled_time < timezone.now():
            raise ValidationError(
                {"scheduled_time": ValidationError(_("Scheduled time must be in the future"), code="invalid")}
            )


def get_next_cron_time(cron_string: str | None) -> datetime | None:
    """Calculate the next scheduled time by creating a crontab object with a cron string"""
    if cron_string is None:
        return None
    now = timezone.now()
    itr = croniter.croniter(cron_string, now)
    next_itr = itr.get_next(datetime)
    return next_itr


def get_scheduled_task(task_type_str: str, task_id: int) -> Task:
    if task_type_str not in TASK_TYPES:
        raise ValueError(f"Job Model {task_type_str} does not exist, choices are {TASK_TYPES}")
    try:
        task_type = TaskType(task_type_str)
    except ValueError:
        raise ValueError(f"Invalid task type {task_type_str}")
    job = get_current_job()
    using = job.meta.get("scheduler_task_database", "default") if job else "default"
    task = Task.objects.using(using).filter(task_type=task_type, id=task_id).first()
    if task is None:
        raise ValueError(f"Job {task_type}:{task_id} does not exist")
    return task  # type: ignore[no-any-return]


def run_task(task_model: str, task_id: int) -> Any:
    """Run a scheduled job"""
    if isinstance(task_id, str):
        task_id = int(task_id)
    job = get_current_job()
    if job is not None and job.task_type == str(TaskType.CRON):
        using = job.meta.get("scheduler_task_database", "default")
        with transaction.atomic(using=using):
            task = Task.objects.using(using).select_for_update().filter(pk=job.scheduled_task_id).first()
            allowed = False
            if task is not None and cron.same_generation(task, job):
                if job.meta.get(cron._MANUAL):
                    allowed = task.task_type == job.task_type
                elif task.task_type == TaskType.CRON and task.queue == job.queue_name:
                    allowed = cron.reconcile(task, create=False) and task.enabled and task.job_name == job.name
            if not allowed:
                job.meta[cron._SKIPPED] = "1"
                return {"skipped": "obsolete recurring job"}
    scheduled_task = get_scheduled_task(task_model, task_id)
    logger.debug(f"Running task {scheduled_task!s}")
    args = scheduled_task.parse_args()
    kwargs = scheduled_task.parse_kwargs()
    res = scheduled_task.callable_func()(*args, **kwargs)  # type: ignore[no-untyped-call]
    return res
