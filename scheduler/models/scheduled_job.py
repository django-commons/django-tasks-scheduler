import math
import uuid
from datetime import timedelta
from typing import Dict

import croniter
from django.apps import apps
from django.contrib import admin
from django.contrib.contenttypes.fields import GenericRelation
from django.core.exceptions import ValidationError
from django.db import models
from django.templatetags.tz import utc
from django.urls import reverse
from django.utils import timezone
from django.utils.safestring import mark_safe
from django.utils.translation import gettext_lazy as _

from scheduler import settings
from scheduler import tools
from scheduler.models.args import JobArg, JobKwarg
from scheduler.queues import get_queue, logger
from scheduler.rq_classes import DjangoQueue

SCHEDULER_INTERVAL = settings.SCHEDULER_CONFIG['SCHEDULER_INTERVAL']


def callback_save_job(job, connection, result, *args, **kwargs):
    model_name = job.meta.get('job_type', None)
    if model_name is None:
        return
    model = apps.get_model(app_label='scheduler', model_name=model_name)
    scheduled_job = model.objects.filter(job_id=job.id).first()
    if scheduled_job is not None:
        scheduled_job.unschedule()
        scheduled_job.schedule()


class BaseJob(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)
    QUEUES = [(key, key) for key in settings.QUEUES.keys()]
    JOB_TYPE = 'BaseJob'
    name = models.CharField(
        _('name'), max_length=128, unique=True,
        help_text='Name of the job.', )
    callable = models.CharField(_('callable'), max_length=2048)
    callable_args = GenericRelation(JobArg, related_query_name='args')
    callable_kwargs = GenericRelation(JobKwarg, related_query_name='kwargs')
    enabled = models.BooleanField(
        _('enabled'), default=True,
        help_text=_('Should job be scheduled? This field is useful to keep '
                    'past jobs that should no longer be scheduled'),
    )
    queue = models.CharField(
        _('queue'), max_length=255, choices=QUEUES,
        help_text=_('Queue name'), )
    job_id = models.CharField(
        _('job id'), max_length=128, editable=False, blank=True, null=True,
        help_text=_('Current job_id on queue'))
    repeat = models.PositiveIntegerField(
        _('repeat'), blank=True, null=True,
        help_text=_('Number of times to run the job. Leaving this blank means it will run forever.'), )
    at_front = models.BooleanField(
        _('At front'), default=False, blank=True, null=True,
        help_text=_('When queuing the job, add it in the front of the queue'), )
    timeout = models.IntegerField(
        _('timeout'), blank=True, null=True,
        help_text=_("Timeout specifies the maximum runtime, in seconds, for the job "
                    "before it'll be considered 'lost'. Blank uses the default "
                    "timeout."), )
    result_ttl = models.IntegerField(
        _('result ttl'), blank=True, null=True,
        help_text=mark_safe(
            """The TTL value (in seconds) of the job result.<br/>
               -1: Result never expires, you should delete jobs manually. <br/>
                0: Result gets deleted immediately. <br/>
                >0: Result expires after n seconds."""), )

    def callable_func(self):
        """Translate callable string to callable"""
        return tools.callable_func(self.callable)

    @admin.display(boolean=True, description=_('is scheduled?'))
    def is_scheduled(self) -> bool:
        """Check whether job is queued/scheduled to be executed"""
        if not self.job_id:
            return False
        scheduled_jobs = self.rqueue.scheduled_job_registry.get_job_ids()
        enqueued_jobs = self.rqueue.get_job_ids()
        res = (self.job_id in scheduled_jobs) or (self.job_id in enqueued_jobs)
        if not res:  # self.job_id is not None
            self.job_id = None
            super(BaseJob, self).save()
        return res

    @admin.display(description='Callable')
    def function_string(self) -> str:
        args = self.parse_args()
        args_list = [repr(arg) for arg in args]
        kwargs = self.parse_kwargs()
        kwargs_list = [k + '=' + repr(v) for (k, v) in kwargs.items()]
        return self.callable + f"({', '.join(args_list + kwargs_list)})"

    def parse_args(self):
        """Parse args for running job"""
        args = self.callable_args.all()
        return [arg.value() for arg in args]

    def parse_kwargs(self):
        """Parse kwargs for running job"""
        kwargs = self.callable_kwargs.all()
        return dict([kwarg.value() for kwarg in kwargs])

    def _next_job_id(self):
        addition = uuid.uuid4().hex[-10:]
        name = self.name.replace('/', '.')
        return f'{self.queue}:{name}:{addition}'

    def _enqueue_args(self) -> Dict:
        """args for DjangoQueue.enqueue.
        Set all arguments for DjangoQueue.enqueue/enqueue_at.
        Particularly:
        - set job timeout and ttl
        - ensure a callback to reschedule job next iteration.
        - set job-id to proper format
        - set job meta
        """
        res = dict(
            meta=dict(
                repeat=self.repeat,
                job_type=self.JOB_TYPE,
                scheduled_job_id=self.id,
            ),
            on_success=callback_save_job,
            on_failure=callback_save_job,
            job_id=self._next_job_id(),
        )
        if self.at_front:
            res['at_front'] = self.at_front
        if self.timeout:
            res['job_timeout'] = self.timeout
        if self.result_ttl is not None:
            res['result_ttl'] = self.result_ttl
        return res

    @property
    def rqueue(self) -> DjangoQueue:
        """Returns django-queue for job
        """
        return get_queue(self.queue)

    def ready_for_schedule(self) -> bool:
        """Is job ready to be scheduled?

        If job is already scheduled or disabled, then it is not
        ready to be scheduled.

        :returns: True if job is ready to be scheduled.
        """
        if self.is_scheduled():
            logger.debug(f'Job {self.name} already scheduled')
            return False
        if not self.enabled:
            logger.debug(f'Job {str(self)} disabled, enable job before scheduling')
            return False
        return True

    def schedule(self) -> bool:
        """Schedule job to run.
        :returns: True if job was scheduled, False otherwise.
        """
        if not self.ready_for_schedule():
            return False
        schedule_time = self._schedule_time()
        kwargs = self._enqueue_args()
        job = self.rqueue.enqueue_at(
            schedule_time,
            tools.run_job,
            args=(self.JOB_TYPE, self.id),
            **kwargs, )
        self.job_id = job.id
        super(BaseJob, self).save()
        return True

    def enqueue_to_run(self) -> bool:
        """Enqueue job to run now.
        """
        kwargs = self._enqueue_args()
        job = self.rqueue.enqueue(
            tools.run_job,
            args=(self.JOB_TYPE, self.id),
            **kwargs,
        )
        self.job_id = job.id
        super(BaseJob, self).save()
        return True

    def unschedule(self) -> bool:
        """Remove job from django-queue.

        If job is queued to be executed or scheduled to be executed, it will remove it.
        """
        queue = self.rqueue
        if self.is_scheduled():
            queue.remove(self.job_id)
            queue.scheduled_job_registry.remove(self.job_id)
        self.job_id = None
        super(BaseJob, self).save()
        return True

    def _schedule_time(self):
        return utc(self.scheduled_time)

    def to_dict(self) -> Dict:
        """Export model to dictionary, so it can be saved as external file backup
        """
        res = dict(
            model=self.JOB_TYPE,
            name=self.name,
            callable=self.callable,
            callable_args=[
                dict(arg_type=arg.arg_type, val=arg.val, )
                for arg in self.callable_args.all()],
            callable_kwargs=[
                dict(arg_type=arg.arg_type, key=arg.key, val=arg.val, )
                for arg in self.callable_kwargs.all()],
            enabled=self.enabled,
            queue=self.queue,
            repeat=self.repeat,
            at_front=self.at_front,
            timeout=self.timeout,
            result_ttl=self.result_ttl,
            cron_string=getattr(self, 'cron_string', None),
            scheduled_time=self._schedule_time().isoformat(),
            interval=getattr(self, 'interval', None),
            interval_unit=getattr(self, 'interval_unit', None),
        )
        return res

    def get_absolute_url(self):
        model = self._meta.model.__name__.lower()
        return reverse(f'admin:scheduler_{model}_change', args=[self.id, ])

    def __str__(self):
        func = self.function_string()
        return f'{self.JOB_TYPE}[{self.name}={func}]'

    def save(self, **kwargs):
        schedule_job = kwargs.pop('schedule_job', True)
        update_fields = kwargs.get('update_fields', None)
        if update_fields:
            kwargs['update_fields'] = set(update_fields).union({'modified'})
        super(BaseJob, self).save(**kwargs)
        if schedule_job:
            self.schedule()
            super(BaseJob, self).save()

    def delete(self, **kwargs):
        self.unschedule()
        super(BaseJob, self).delete(**kwargs)

    def clean_callable(self):
        try:
            tools.callable_func(self.callable)
        except Exception:
            raise ValidationError({
                'callable': ValidationError(
                    _('Invalid callable, must be importable'), code='invalid')
            })

    def clean_queue(self):
        queue_keys = settings.QUEUES.keys()
        if self.queue not in queue_keys:
            raise ValidationError({
                'queue': ValidationError(
                    _('Invalid queue, must be one of: {}'.format(
                        ', '.join(queue_keys))), code='invalid')
            })

    def clean(self):
        self.clean_queue()
        self.clean_callable()

    class Meta:
        abstract = True


class ScheduledTimeMixin(models.Model):
    scheduled_time = models.DateTimeField(_('scheduled time'))

    class Meta:
        abstract = True


class ScheduledJob(ScheduledTimeMixin, BaseJob):
    repeat = None
    JOB_TYPE = 'ScheduledJob'

    def ready_for_schedule(self) -> bool:
        return (super(ScheduledJob, self).ready_for_schedule()
                and (self.scheduled_time is None
                     or self.scheduled_time >= timezone.now()))

    class Meta:
        verbose_name = _('Scheduled Job')
        verbose_name_plural = _('Scheduled Jobs')
        ordering = ('name',)


class RepeatableJob(ScheduledTimeMixin, BaseJob):
    class TimeUnits(models.TextChoices):
        SECONDS = 'seconds', _('seconds')
        MINUTES = 'minutes', _('minutes')
        HOURS = 'hours', _('hours')
        DAYS = 'days', _('days')
        WEEKS = 'weeks', _('weeks')

    interval = models.PositiveIntegerField(_('interval'))
    interval_unit = models.CharField(
        _('interval unit'), max_length=12, choices=TimeUnits.choices, default=TimeUnits.HOURS
    )
    JOB_TYPE = 'RepeatableJob'

    def clean(self):
        super(RepeatableJob, self).clean()
        self.clean_interval_unit()
        self.clean_result_ttl()

    def clean_interval_unit(self):
        if SCHEDULER_INTERVAL > self.interval_seconds():
            raise ValidationError(
                _("Job interval is set lower than %(queue)r queue's interval. "
                  "minimum interval is %(interval)"),
                code='invalid',
                params={'queue': self.queue, 'interval': SCHEDULER_INTERVAL})
        if self.interval_seconds() % SCHEDULER_INTERVAL:
            raise ValidationError(
                _("Job interval is not a multiple of rq_scheduler's interval frequency: %(interval)ss"),
                code='invalid',
                params={'interval': SCHEDULER_INTERVAL})

    def clean_result_ttl(self) -> None:
        """
        Throws an error if there are repeats left to run and the result_ttl won't last until the next scheduled time.
        :return: None
        """
        if self.result_ttl and self.result_ttl != -1 and self.result_ttl < self.interval_seconds() and self.repeat:
            raise ValidationError(
                _("Job result_ttl must be either indefinite (-1) or "
                  "longer than the interval, %(interval)s seconds, to ensure rescheduling."),
                code='invalid',
                params={'interval': self.interval_seconds()}, )

    def interval_display(self):
        return '{} {}'.format(self.interval, self.get_interval_unit_display())

    def interval_seconds(self):
        kwargs = {self.interval_unit: self.interval, }
        return timedelta(**kwargs).total_seconds()

    def _enqueue_args(self):
        res = super(RepeatableJob, self)._enqueue_args()
        res['meta']['interval'] = self.interval_seconds()
        return res

    def ready_for_schedule(self):
        if super(RepeatableJob, self).ready_for_schedule() is False:
            return False
        if self.scheduled_time < timezone.now():
            gap = math.ceil((timezone.now().timestamp() - self.scheduled_time.timestamp()) / self.interval_seconds())
            if self.repeat is None or self.repeat >= gap:
                self.scheduled_time += timedelta(seconds=self.interval_seconds() * gap)
                self.repeat = (self.repeat - gap) if self.repeat is not None else None

        if self.scheduled_time < timezone.now():
            return False
        return True

    class Meta:
        verbose_name = _('Repeatable Job')
        verbose_name_plural = _('Repeatable Jobs')
        ordering = ('name',)


class CronJob(BaseJob):
    JOB_TYPE = 'CronJob'

    cron_string = models.CharField(
        _('cron string'), max_length=64,
        help_text=mark_safe(
            '''Define the schedule in a crontab like syntax.
            Times are in UTC. Use <a href="https://crontab.guru/">crontab.guru</a> to create a cron string.''')
    )

    def clean(self):
        super(CronJob, self).clean()
        self.clean_cron_string()

    def clean_cron_string(self):
        try:
            croniter.croniter(self.cron_string)
        except ValueError as e:
            raise ValidationError({'cron_string': ValidationError(_(str(e)), code='invalid')})

    def _schedule_time(self):
        return tools.get_next_cron_time(self.cron_string)

    class Meta:
        verbose_name = _('Cron Job')
        verbose_name_plural = _('Cron Jobs')
        ordering = ('name',)
