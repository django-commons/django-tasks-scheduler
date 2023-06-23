import zoneinfo
from datetime import datetime, timedelta

from django.contrib.messages import get_messages
from django.core.exceptions import ValidationError
from django.test import override_settings
from django.urls import reverse
from django.utils import timezone

from scheduler import settings
from scheduler.models import BaseJob, CronJob, JobArg, JobKwarg, RepeatableJob, ScheduledJob
from scheduler.tools import run_job, create_worker
from . import jobs
from .testtools import (
    job_factory, jobarg_factory, _get_job_from_scheduled_registry,
    SchedulerBaseCase, _get_executions)
from ..queues import get_queue


def assert_response_has_msg(response, message):
    messages = [m.message for m in get_messages(response.wsgi_request)]
    assert message in messages, f'expected "{message}" in {messages}'


def has_execution_with_status(django_job, status) -> bool:
    job_list = _get_executions(django_job)
    job_list = [(j.id, j.get_status()) for j in job_list]
    for job in job_list:
        if job[1] == status:
            return True
    print(f'{job_list} does not contain a job with status {status}')
    return False


class BaseTestCases:
    class TestBaseJob(SchedulerBaseCase):
        JobModelClass = BaseJob

        def test_callable_func(self):
            job = job_factory(self.JobModelClass)
            job.callable = 'scheduler.tests.jobs.test_job'
            func = job.callable_func()
            self.assertEqual(jobs.test_job, func)

        def test_callable_func_not_callable(self):
            job = job_factory(self.JobModelClass)
            job.callable = 'scheduler.tests.jobs.test_non_callable'
            with self.assertRaises(TypeError):
                job.callable_func()

        def test_clean_callable(self):
            job = job_factory(self.JobModelClass)
            job.callable = 'scheduler.tests.jobs.test_job'
            self.assertIsNone(job.clean_callable())

        def test_clean_callable_invalid(self):
            job = job_factory(self.JobModelClass)
            job.callable = 'scheduler.tests.jobs.test_non_callable'
            with self.assertRaises(ValidationError):
                job.clean_callable()

        def test_clean_queue(self):
            for queue in settings.QUEUES.keys():
                job = job_factory(self.JobModelClass)
                job.queue = queue
                self.assertIsNone(job.clean_queue())

        def test_clean_queue_invalid(self):
            job = job_factory(self.JobModelClass)
            job.queue = 'xxxxxx'
            job.callable = 'scheduler.tests.jobs.test_job'
            with self.assertRaises(ValidationError):
                job.clean()

        # next 2 check the above are included in job.clean() function
        def test_clean_base(self):
            job = job_factory(self.JobModelClass)
            job.queue = list(settings.QUEUES)[0]
            job.callable = 'scheduler.tests.jobs.test_job'
            self.assertIsNone(job.clean())

        def test_clean_invalid_callable(self):
            job = job_factory(self.JobModelClass)
            job.queue = list(settings.QUEUES)[0]
            job.callable = 'scheduler.tests.jobs.test_non_callable'
            with self.assertRaises(ValidationError):
                job.clean()

        def test_clean_invalid_queue(self):
            job = job_factory(self.JobModelClass)
            job.queue = 'xxxxxx'
            job.callable = 'scheduler.tests.jobs.test_job'
            with self.assertRaises(ValidationError):
                job.clean()

        def test_is_schedulable_already_scheduled(self):
            job = job_factory(self.JobModelClass, )
            job.schedule()
            self.assertTrue(job.is_scheduled())

        def test_is_schedulable_disabled(self):
            job = job_factory(self.JobModelClass)
            job.enabled = False
            self.assertFalse(job.enabled)

        def test_schedule(self):
            job = job_factory(self.JobModelClass, )
            self.assertTrue(job.is_scheduled())
            self.assertIsNotNone(job.job_id)

        def test_unschedulable(self):
            job = job_factory(self.JobModelClass, enabled=False)
            self.assertFalse(job.is_scheduled())
            self.assertIsNone(job.job_id)

        def test_unschedule(self):
            job = job_factory(self.JobModelClass, )
            self.assertTrue(job.unschedule())
            self.assertIsNone(job.job_id)

        def test_unschedule_not_scheduled(self):
            job = job_factory(self.JobModelClass, enabled=False)
            self.assertTrue(job.unschedule())
            self.assertIsNone(job.job_id)

        def test_save_enabled(self):
            job = job_factory(self.JobModelClass, )
            self.assertIsNotNone(job.job_id)

        def test_save_disabled(self):
            job = job_factory(self.JobModelClass, enabled=False)
            job.save()
            self.assertIsNone(job.job_id)

        def test_save_and_schedule(self):
            job = job_factory(self.JobModelClass, )
            self.assertIsNotNone(job.job_id)
            self.assertTrue(job.is_scheduled())

        def test_schedule2(self):
            job = job_factory(self.JobModelClass)
            job.queue = list(settings.QUEUES)[0]
            job.enabled = False
            job.scheduled_time = timezone.now() + timedelta(minutes=1)
            self.assertFalse(job.schedule())

        def test_delete_and_unschedule(self):
            job = job_factory(self.JobModelClass, )
            self.assertIsNotNone(job.job_id)
            self.assertTrue(job.is_scheduled())
            job.delete()
            self.assertFalse(job.is_scheduled())

        def test_job_create(self):
            prev_count = self.JobModelClass.objects.count()
            job_factory(self.JobModelClass)
            self.assertEqual(self.JobModelClass.objects.count(), prev_count + 1)

        def test_str(self):
            name = "test"
            job = job_factory(self.JobModelClass, name=name)
            self.assertEqual(f'{self.JobModelClass.__name__}[{name}={job.callable}()]', str(job))

        def test_callable_passthrough(self):
            job = job_factory(self.JobModelClass)
            entry = _get_job_from_scheduled_registry(job)
            self.assertEqual(entry.func, run_job)
            job_model, job_id = entry.args
            self.assertEqual(job_model, self.JobModelClass.__name__)
            self.assertEqual(job_id, job.id)

        def test_timeout_passthrough(self):
            job = job_factory(self.JobModelClass, timeout=500)
            entry = _get_job_from_scheduled_registry(job)
            self.assertEqual(entry.timeout, 500)

        def test_at_front_passthrough(self):
            job = job_factory(self.JobModelClass, at_front=True)
            queue = job.rqueue
            jobs_to_schedule = queue.scheduled_job_registry.get_job_ids()
            self.assertIn(job.job_id, jobs_to_schedule)

        def test_callable_result(self):
            job = job_factory(self.JobModelClass, )
            entry = _get_job_from_scheduled_registry(job)
            self.assertEqual(entry.perform(), 2)

        def test_callable_empty_args_and_kwargs(self):
            job = job_factory(self.JobModelClass, callable='scheduler.tests.jobs.test_args_kwargs')
            entry = _get_job_from_scheduled_registry(job)
            self.assertEqual(entry.perform(), 'test_args_kwargs()')

        def test_delete_args(self):
            job = job_factory(self.JobModelClass, )
            arg = jobarg_factory(JobArg, val='one', content_object=job)
            self.assertEqual(1, job.callable_args.count())
            arg.delete()
            self.assertEqual(0, job.callable_args.count())

        def test_delete_kwargs(self):
            job = job_factory(self.JobModelClass, )
            kwarg = jobarg_factory(JobKwarg, key='key1', arg_type='str', val='one', content_object=job)
            self.assertEqual(1, job.callable_kwargs.count())
            kwarg.delete()
            self.assertEqual(0, job.callable_kwargs.count())

        def test_parse_args(self):
            job = job_factory(self.JobModelClass, )
            date = timezone.now()
            jobarg_factory(JobArg, val='one', content_object=job)
            jobarg_factory(JobArg, arg_type='int', val=2, content_object=job)
            jobarg_factory(JobArg, arg_type='bool', val=True, content_object=job)
            jobarg_factory(JobArg, arg_type='bool', val=False, content_object=job)
            jobarg_factory(JobArg, arg_type='datetime', val=date, content_object=job)
            self.assertEqual(job.parse_args(), ['one', 2, True, False, date])

        def test_parse_kwargs(self):
            job = job_factory(self.JobModelClass, )
            date = timezone.now()
            jobarg_factory(JobKwarg, key='key1', arg_type='str', val='one', content_object=job)
            jobarg_factory(JobKwarg, key='key2', arg_type='int', val=2, content_object=job)
            jobarg_factory(JobKwarg, key='key3', arg_type='bool', val=True, content_object=job)
            jobarg_factory(JobKwarg, key='key4', arg_type='datetime', val=date, content_object=job)
            kwargs = job.parse_kwargs()
            self.assertEqual(kwargs, dict(key1='one', key2=2, key3=True, key4=date))

        def test_callable_args_and_kwargs(self):
            job = job_factory(self.JobModelClass, callable='scheduler.tests.jobs.test_args_kwargs')
            date = timezone.now()
            jobarg_factory(JobArg, arg_type='str', val='one', content_object=job)
            jobarg_factory(JobKwarg, key='key1', arg_type='int', val=2, content_object=job)
            jobarg_factory(JobKwarg, key='key2', arg_type='datetime', val=date, content_object=job)
            jobarg_factory(JobKwarg, key='key3', arg_type='bool', val=False, content_object=job)
            job.save()
            entry = _get_job_from_scheduled_registry(job)
            self.assertEqual(entry.perform(),
                             "test_args_kwargs('one', key1=2, key2={}, key3=False)".format(date))

        def test_function_string(self):
            job = job_factory(self.JobModelClass, )
            date = timezone.now()
            jobarg_factory(JobArg, arg_type='str', val='one', content_object=job)
            jobarg_factory(JobArg, arg_type='int', val='1', content_object=job)
            jobarg_factory(JobArg, arg_type='datetime', val=date, content_object=job)
            jobarg_factory(JobArg, arg_type='bool', val=True, content_object=job)
            jobarg_factory(JobKwarg, key='key1', arg_type='str', val='one', content_object=job)
            jobarg_factory(JobKwarg, key='key2', arg_type='int', val=2, content_object=job)
            jobarg_factory(JobKwarg, key='key3', arg_type='datetime', val=date, content_object=job)
            jobarg_factory(JobKwarg, key='key4', arg_type='bool', val=False, content_object=job)
            self.assertEqual(job.function_string(),
                             f"scheduler.tests.jobs.test_job('one', 1, {repr(date)}, True, "
                             f"key1='one', key2=2, key3={repr(date)}, key4=False)")

        def test_admin_list_view(self):
            # arrange
            self.client.login(username='admin', password='admin')
            job = job_factory(self.JobModelClass, )
            model = job._meta.model.__name__.lower()
            url = reverse(f'admin:scheduler_{model}_changelist')
            # act
            res = self.client.get(url)
            # assert
            self.assertEqual(200, res.status_code)

        def test_admin_list_view_delete_model(self):
            # arrange
            self.client.login(username='admin', password='admin')
            job = job_factory(self.JobModelClass, )
            model = job._meta.model.__name__.lower()
            url = reverse(f'admin:scheduler_{model}_changelist')
            # act
            res = self.client.post(url, data={
                'action': 'delete_model',
                '_selected_action': [job.pk, ],
            })
            # assert
            self.assertEqual(302, res.status_code)

        def test_admin_run_job_now_enqueues_job_at(self):
            # arrange
            self.client.login(username='admin', password='admin')
            job = job_factory(self.JobModelClass, )
            model = job._meta.model.__name__.lower()
            url = reverse(f'admin:scheduler_{model}_changelist')
            # act
            res = self.client.post(url, data={
                'action': 'enqueue_job_now',
                '_selected_action': [job.pk, ],
            })
            # assert
            self.assertEqual(302, res.status_code)
            job.refresh_from_db()
            queue = get_queue(job.queue)
            self.assertIn(job.job_id, queue.get_job_ids())

        def test_admin_change_view(self):
            # arrange
            self.client.login(username='admin', password='admin')
            job = job_factory(self.JobModelClass, )
            model = job._meta.model.__name__.lower()
            url = reverse(f'admin:scheduler_{model}_change', args=[job.pk, ])
            # act
            res = self.client.get(url)
            # assert
            self.assertEqual(200, res.status_code)

        def test_admin_change_view__bad_redis_connection(self):
            # arrange
            self.client.login(username='admin', password='admin')
            job = job_factory(self.JobModelClass, queue='test2', instance_only=True)
            job.save(schedule_job=False)
            model = job._meta.model.__name__.lower()
            url = reverse(f'admin:scheduler_{model}_change', args=[job.pk, ])
            # act
            res = self.client.get(url)
            # assert
            self.assertEqual(200, res.status_code)

        def test_admin_enqueue_job_now(self):
            # arrange
            self.client.login(username='admin', password='admin')
            job = job_factory(self.JobModelClass, )
            self.assertIsNotNone(job.job_id)
            self.assertTrue(job.is_scheduled())
            data = {
                'action': 'enqueue_job_now',
                '_selected_action': [job.id, ],
            }
            model = job._meta.model.__name__.lower()
            url = reverse(f'admin:scheduler_{model}_changelist')
            # act
            res = self.client.post(url, data=data, follow=True)
            # assert part 1
            self.assertEqual(200, res.status_code)
            entry = _get_job_from_scheduled_registry(job)
            job_model, scheduled_job_id = entry.args
            self.assertEqual(job_model, job.JOB_TYPE)
            self.assertEqual(scheduled_job_id, job.id)
            self.assertEqual('scheduled', entry.get_status())
            self.assertTrue(has_execution_with_status(job, 'queued'))

            # act 2
            worker = create_worker('default', fork_job_execution=False, )
            worker.work(burst=True)

            # assert 2
            entry = _get_job_from_scheduled_registry(job)
            self.assertEqual(job_model, job.JOB_TYPE)
            self.assertEqual(scheduled_job_id, job.id)
            self.assertTrue(has_execution_with_status(job, 'finished'))

        def test_admin_enable_job(self):
            # arrange
            self.client.login(username='admin', password='admin')
            job = job_factory(self.JobModelClass, enabled=False)
            self.assertIsNone(job.job_id)
            self.assertFalse(job.is_scheduled())
            data = {
                'action': 'enable_selected',
                '_selected_action': [job.id, ],
            }
            model = job._meta.model.__name__.lower()
            url = reverse(f'admin:scheduler_{model}_changelist')
            # act
            res = self.client.post(url, data=data, follow=True)
            # assert
            self.assertEqual(200, res.status_code)
            job.refresh_from_db()
            self.assertTrue(job.enabled)
            self.assertTrue(job.is_scheduled())
            assert_response_has_msg(res, '1 job was successfully enabled and scheduled.')

        def test_admin_disable_job(self):
            # arrange
            self.client.login(username='admin', password='admin')
            job = job_factory(self.JobModelClass, enabled=True)
            job.save()
            data = {
                'action': 'disable_selected',
                '_selected_action': [job.id, ],
            }
            model = job._meta.model.__name__.lower()
            url = reverse(f'admin:scheduler_{model}_changelist')
            self.assertTrue(job.is_scheduled())
            # act
            res = self.client.post(url, data=data, follow=True)
            # assert
            self.assertEqual(200, res.status_code)
            job.refresh_from_db()
            self.assertFalse(job.is_scheduled())
            self.assertFalse(job.enabled)
            assert_response_has_msg(res, '1 job was successfully disabled and unscheduled.')

        def test_admin_single_delete(self):
            # arrange
            self.client.login(username='admin', password='admin')
            prev_count = self.JobModelClass.objects.count()
            job = job_factory(self.JobModelClass, )
            self.assertIsNotNone(job.job_id)
            self.assertTrue(job.is_scheduled())
            prev = len(_get_executions(job))
            model = job._meta.model.__name__.lower()
            url = reverse(f'admin:scheduler_{model}_delete', args=[job.pk, ])
            data = {
                'post': 'yes',
            }
            # act
            res = self.client.post(url, data=data, follow=True)
            # assert
            self.assertEqual(200, res.status_code)
            self.assertEqual(prev_count, self.JobModelClass.objects.count())
            self.assertEquals(prev - 1, len(_get_executions(job)))

        def test_admin_delete_selected(self):
            # arrange
            self.client.login(username='admin', password='admin')
            job = job_factory(self.JobModelClass, enabled=True)
            job.save()
            queue = get_queue(job.queue)
            scheduled_jobs = queue.scheduled_job_registry.get_job_ids()
            job_id = job.job_id
            self.assertIn(job_id, scheduled_jobs)
            data = {
                'action': 'delete_selected',
                '_selected_action': [job.id, ],
                'post': 'yes',
            }
            model = job._meta.model.__name__.lower()
            url = reverse(f'admin:scheduler_{model}_changelist')
            # act
            res = self.client.post(url, data=data, follow=True)
            # assert
            self.assertEqual(200, res.status_code)
            assert_response_has_msg(res, f'Successfully deleted 1 {self.JobModelClass._meta.verbose_name}.')
            self.assertIsNone(self.JobModelClass.objects.filter(id=job.id).first())
            scheduled_jobs = queue.scheduled_job_registry.get_job_ids()
            self.assertNotIn(job_id, scheduled_jobs)

    class TestSchedulableJob(TestBaseJob):
        # Currently ScheduledJob and RepeatableJob
        JobModelClass = ScheduledJob

        def test_schedule_time_utc(self):
            job = job_factory(self.JobModelClass)
            est = zoneinfo.ZoneInfo('US/Eastern')
            scheduled_time = datetime(2016, 12, 25, 8, 0, 0, tzinfo=est)
            job.scheduled_time = scheduled_time
            utc = zoneinfo.ZoneInfo('UTC')
            expected = scheduled_time.astimezone(utc).isoformat()
            self.assertEqual(expected, job._schedule_time().isoformat())

        def test_result_ttl_passthrough(self):
            job = job_factory(self.JobModelClass, result_ttl=500)
            entry = _get_job_from_scheduled_registry(job)
            self.assertEqual(entry.result_ttl, 500)


class TestScheduledJob(BaseTestCases.TestSchedulableJob):
    JobModelClass = ScheduledJob

    def test_clean(self):
        job = job_factory(self.JobModelClass)
        job.queue = list(settings.QUEUES)[0]
        job.callable = 'scheduler.tests.jobs.test_job'
        self.assertIsNone(job.clean())

    def test_unschedulable_old_job(self):
        job = job_factory(self.JobModelClass, scheduled_time=timezone.now() - timedelta(hours=1))
        self.assertFalse(job.is_scheduled())


class TestRepeatableJob(BaseTestCases.TestSchedulableJob):
    JobModelClass = RepeatableJob

    def test_unschedulable_old_job(self):
        job = job_factory(self.JobModelClass, scheduled_time=timezone.now() - timedelta(hours=1), repeat=0)
        self.assertFalse(job.is_scheduled())

    def test_schedulable_old_job_repeat_none(self):
        # If repeat is None, the job should be scheduled
        job = job_factory(self.JobModelClass, scheduled_time=timezone.now() - timedelta(hours=1), repeat=None)
        self.assertTrue(job.is_scheduled())

    def test_clean(self):
        job = job_factory(self.JobModelClass)
        job.queue = list(settings.QUEUES)[0]
        job.callable = 'scheduler.tests.jobs.test_job'
        job.interval = 1
        job.result_ttl = -1
        self.assertIsNone(job.clean())

    def test_clean_seconds(self):
        job = job_factory(self.JobModelClass)
        job.queue = list(settings.QUEUES)[0]
        job.callable = 'scheduler.tests.jobs.test_job'
        job.interval = 60
        job.result_ttl = -1
        job.interval_unit = 'seconds'
        self.assertIsNone(job.clean())

    @override_settings(SCHEDULER_CONFIG={
        'SCHEDULER_INTERVAL': 10,
    })
    def test_clean_too_frequent(self):
        job = job_factory(self.JobModelClass)
        job.queue = list(settings.QUEUES)[0]
        job.callable = 'scheduler.tests.jobs.test_job'
        job.interval = 2  # Smaller than 10
        job.result_ttl = -1
        job.interval_unit = 'seconds'
        with self.assertRaises(ValidationError):
            job.clean_interval_unit()

    def test_clean_not_multiple(self):
        job = job_factory(self.JobModelClass)
        job.queue = list(settings.QUEUES)[0]
        job.callable = 'scheduler.tests.jobs.test_job'
        job.interval = 121
        job.interval_unit = 'seconds'
        with self.assertRaises(ValidationError):
            job.clean_interval_unit()

    def test_clean_short_result_ttl(self):
        job = job_factory(self.JobModelClass)
        job.queue = list(settings.QUEUES)[0]
        job.callable = 'scheduler.tests.jobs.test_job'
        job.interval = 1
        job.repeat = 1
        job.result_ttl = 3599
        job.interval_unit = 'hours'
        job.repeat = 42
        with self.assertRaises(ValidationError):
            job.clean_result_ttl()

    def test_clean_indefinite_result_ttl(self):
        job = job_factory(self.JobModelClass)
        job.queue = list(settings.QUEUES)[0]
        job.callable = 'scheduler.tests.jobs.test_job'
        job.interval = 1
        job.result_ttl = -1
        job.interval_unit = 'hours'
        job.clean_result_ttl()

    def test_clean_undefined_result_ttl(self):
        job = job_factory(self.JobModelClass)
        job.queue = list(settings.QUEUES)[0]
        job.callable = 'scheduler.tests.jobs.test_job'
        job.interval = 1
        job.interval_unit = 'hours'
        job.clean_result_ttl()

    def test_interval_seconds_weeks(self):
        job = job_factory(self.JobModelClass, interval=2, interval_unit='weeks')
        self.assertEqual(1209600.0, job.interval_seconds())

    def test_interval_seconds_days(self):
        job = job_factory(self.JobModelClass, interval=2, interval_unit='days')
        self.assertEqual(172800.0, job.interval_seconds())

    def test_interval_seconds_hours(self):
        job = job_factory(self.JobModelClass, interval=2, interval_unit='hours')
        self.assertEqual(7200.0, job.interval_seconds())

    def test_interval_seconds_minutes(self):
        job = job_factory(self.JobModelClass, interval=15, interval_unit='minutes')
        self.assertEqual(900.0, job.interval_seconds())

    def test_interval_seconds_seconds(self):
        job = RepeatableJob(interval=15, interval_unit='seconds')
        self.assertEqual(15.0, job.interval_seconds())

    def test_interval_display(self):
        job = job_factory(self.JobModelClass, interval=15, interval_unit='minutes')
        self.assertEqual(job.interval_display(), '15 minutes')

    def test_result_interval(self):
        job = job_factory(self.JobModelClass, )
        entry = _get_job_from_scheduled_registry(job)
        self.assertEqual(entry.meta['interval'], 3600)

    def test_repeat(self):
        job = job_factory(self.JobModelClass, repeat=10)
        entry = _get_job_from_scheduled_registry(job)
        self.assertEqual(entry.meta['repeat'], 10)

    def test_repeat_old_job_exhausted(self):
        base_time = timezone.now()
        job = job_factory(self.JobModelClass, scheduled_time=base_time - timedelta(hours=10), repeat=10)
        self.assertEqual(job.is_scheduled(), False)

    def test_repeat_old_job_last_iter(self):
        base_time = timezone.now()
        job = job_factory(self.JobModelClass, scheduled_time=base_time - timedelta(hours=9, minutes=30), repeat=10)
        self.assertEqual(job.repeat, 0)
        self.assertEqual(job.is_scheduled(), True)

    def test_repeat_old_job_remaining(self):
        base_time = timezone.now()
        job = job_factory(self.JobModelClass, scheduled_time=base_time - timedelta(minutes=30), repeat=5)
        self.assertEqual(job.repeat, 4)
        self.assertEqual(job.scheduled_time, base_time + timedelta(minutes=30))
        self.assertEqual(job.is_scheduled(), True)

    def test_repeat_none_interval_2_min(self):
        base_time = timezone.now()
        job = job_factory(self.JobModelClass, scheduled_time=base_time - timedelta(minutes=29), repeat=None)
        job.interval = 120
        job.interval_unit = 'seconds'
        job.schedule()
        self.assertTrue(job.scheduled_time > base_time)
        self.assertTrue(job.is_scheduled())

    def test_check_rescheduled_after_execution(self):
        job = job_factory(self.JobModelClass, scheduled_time=timezone.now() + timedelta(seconds=1))
        queue = job.rqueue
        first_run_id = job.job_id
        entry = queue.fetch_job(first_run_id)
        queue.run_sync(entry)
        job.refresh_from_db()
        self.assertTrue(job.is_scheduled())
        self.assertNotEquals(job.job_id, first_run_id)


class TestCronJob(BaseTestCases.TestBaseJob):
    JobModelClass = CronJob

    def test_clean(self):
        job = job_factory(self.JobModelClass)
        job.cron_string = '* * * * *'
        job.queue = list(settings.QUEUES)[0]
        job.callable = 'scheduler.tests.jobs.test_job'
        self.assertIsNone(job.clean())

    def test_clean_cron_string_invalid(self):
        job = job_factory(self.JobModelClass)
        job.cron_string = 'not-a-cron-string'
        job.queue = list(settings.QUEUES)[0]
        job.callable = 'scheduler.tests.jobs.test_job'
        with self.assertRaises(ValidationError):
            job.clean_cron_string()

    def test_repeat(self):
        job = job_factory(self.JobModelClass, repeat=10)
        entry = _get_job_from_scheduled_registry(job)
        self.assertEqual(entry.meta['repeat'], 10)

    def test_check_rescheduled_after_execution(self):
        job = job_factory(self.JobModelClass, )
        queue = job.rqueue
        first_run_id = job.job_id
        entry = queue.fetch_job(first_run_id)
        queue.run_sync(entry)
        job.refresh_from_db()
        self.assertTrue(job.is_scheduled())
        self.assertNotEquals(job.job_id, first_run_id)
