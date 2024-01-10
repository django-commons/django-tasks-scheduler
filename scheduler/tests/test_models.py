import zoneinfo
from datetime import datetime, timedelta

from django.contrib.messages import get_messages
from django.core.exceptions import ValidationError
from django.test import override_settings
from django.urls import reverse
from django.utils import timezone
from freezegun import freeze_time

from scheduler import settings
from scheduler.models import BaseTask, TaskArg, TaskKwarg, ScheduledTask
from scheduler.tools import run_task, create_worker
from . import jobs
from .testtools import (
    task_factory, taskarg_factory, _get_job_from_scheduled_registry,
    SchedulerBaseCase, _get_executions)
from ..queues import get_queue


def assert_response_has_msg(response, message):
    messages = [m.message for m in get_messages(response.wsgi_request)]
    assert message in messages, f'expected "{message}" in {messages}'


def assert_has_execution_with_status(task, status):
    job_list = _get_executions(task)
    job_list = [(j.id, j.get_status()) for j in job_list]
    for job in job_list:
        if job[1] == status:
            return
    raise AssertionError(f'{task} does not have an execution with status {status}: {job_list}')


class BaseTestCases:
    class TestBaseTask(SchedulerBaseCase):
        TaskModelClass = BaseTask

        def test_callable_func(self):
            task = task_factory(self.TaskModelClass)
            task.callable = 'scheduler.tests.jobs.test_job'
            func = task.callable_func()
            self.assertEqual(jobs.test_job, func)

        def test_callable_func_not_callable(self):
            task = task_factory(self.TaskModelClass)
            task.callable = 'scheduler.tests.jobs.test_non_callable'
            with self.assertRaises(TypeError):
                task.callable_func()

        def test_clean_callable(self):
            task = task_factory(self.TaskModelClass)
            task.callable = 'scheduler.tests.jobs.test_job'
            self.assertIsNone(task.clean_callable())

        def test_clean_callable_invalid(self):
            task = task_factory(self.TaskModelClass)
            task.callable = 'scheduler.tests.jobs.test_non_callable'
            with self.assertRaises(ValidationError):
                task.clean_callable()

        def test_clean_queue(self):
            for queue in settings.QUEUES.keys():
                task = task_factory(self.TaskModelClass)
                task.queue = queue
                self.assertIsNone(task.clean_queue())

        def test_clean_queue_invalid(self):
            task = task_factory(self.TaskModelClass)
            task.queue = 'xxxxxx'
            task.callable = 'scheduler.tests.jobs.test_job'
            with self.assertRaises(ValidationError):
                task.clean()

        # next 2 check the above are included in job.clean() function
        def test_clean_base(self):
            task = task_factory(self.TaskModelClass)
            task.queue = list(settings.QUEUES)[0]
            task.callable = 'scheduler.tests.jobs.test_job'
            self.assertIsNone(task.clean())

        def test_clean_invalid_callable(self):
            task = task_factory(self.TaskModelClass)
            task.queue = list(settings.QUEUES)[0]
            task.callable = 'scheduler.tests.jobs.test_non_callable'
            with self.assertRaises(ValidationError):
                task.clean()

        def test_clean_invalid_queue(self):
            task = task_factory(self.TaskModelClass)
            task.queue = 'xxxxxx'
            task.callable = 'scheduler.tests.jobs.test_job'
            with self.assertRaises(ValidationError):
                task.clean()

        def test_is_schedulable_already_scheduled(self):
            task = task_factory(self.TaskModelClass, )
            task.schedule()
            self.assertTrue(task.is_scheduled())

        def test_is_schedulable_disabled(self):
            task = task_factory(self.TaskModelClass)
            task.enabled = False
            self.assertFalse(task.enabled)

        def test_schedule(self):
            task = task_factory(self.TaskModelClass, )
            self.assertTrue(task.is_scheduled())
            self.assertIsNotNone(task.job_id)

        def test_unschedulable(self):
            task = task_factory(self.TaskModelClass, enabled=False)
            self.assertFalse(task.is_scheduled())
            self.assertIsNone(task.job_id)

        def test_unschedule(self):
            task = task_factory(self.TaskModelClass, )
            self.assertTrue(task.unschedule())
            self.assertIsNone(task.job_id)

        def test_unschedule_not_scheduled(self):
            task = task_factory(self.TaskModelClass, enabled=False)
            self.assertTrue(task.unschedule())
            self.assertIsNone(task.job_id)

        def test_save_enabled(self):
            task = task_factory(self.TaskModelClass, )
            self.assertIsNotNone(task.job_id)

        def test_save_disabled(self):
            task = task_factory(self.TaskModelClass, enabled=False)
            task.save()
            self.assertIsNone(task.job_id)

        def test_save_and_schedule(self):
            task = task_factory(self.TaskModelClass, )
            self.assertIsNotNone(task.job_id)
            self.assertTrue(task.is_scheduled())

        def test_schedule2(self):
            task = task_factory(self.TaskModelClass)
            task.queue = list(settings.QUEUES)[0]
            task.enabled = False
            task.scheduled_time = timezone.now() + timedelta(minutes=1)
            self.assertFalse(task.schedule())

        def test_delete_and_unschedule(self):
            task = task_factory(self.TaskModelClass, )
            self.assertIsNotNone(task.job_id)
            self.assertTrue(task.is_scheduled())
            task.delete()
            self.assertFalse(task.is_scheduled())

        def test_job_create(self):
            prev_count = self.TaskModelClass.objects.count()
            task_factory(self.TaskModelClass)
            self.assertEqual(self.TaskModelClass.objects.count(), prev_count + 1)

        def test_str(self):
            name = "test"
            task = task_factory(self.TaskModelClass, name=name)
            self.assertEqual(f'{self.TaskModelClass.__name__}[{name}={task.callable}()]', str(task))

        def test_callable_passthrough(self):
            task = task_factory(self.TaskModelClass)
            entry = _get_job_from_scheduled_registry(task)
            self.assertEqual(entry.func, run_task)
            job_model, job_id = entry.args
            self.assertEqual(job_model, self.TaskModelClass.__name__)
            self.assertEqual(job_id, task.id)

        def test_timeout_passthrough(self):
            task = task_factory(self.TaskModelClass, timeout=500)
            entry = _get_job_from_scheduled_registry(task)
            self.assertEqual(entry.timeout, 500)

        def test_at_front_passthrough(self):
            task = task_factory(self.TaskModelClass, at_front=True)
            queue = task.rqueue
            jobs_to_schedule = queue.scheduled_job_registry.get_job_ids()
            self.assertIn(task.job_id, jobs_to_schedule)

        def test_callable_result(self):
            task = task_factory(self.TaskModelClass, )
            entry = _get_job_from_scheduled_registry(task)
            self.assertEqual(entry.perform(), 2)

        def test_callable_empty_args_and_kwargs(self):
            task = task_factory(self.TaskModelClass, callable='scheduler.tests.jobs.test_args_kwargs')
            entry = _get_job_from_scheduled_registry(task)
            self.assertEqual(entry.perform(), 'test_args_kwargs()')

        def test_delete_args(self):
            task = task_factory(self.TaskModelClass, )
            arg = taskarg_factory(TaskArg, val='one', content_object=task)
            self.assertEqual(1, task.callable_args.count())
            arg.delete()
            self.assertEqual(0, task.callable_args.count())

        def test_delete_kwargs(self):
            task = task_factory(self.TaskModelClass, )
            kwarg = taskarg_factory(TaskKwarg, key='key1', arg_type='str', val='one', content_object=task)
            self.assertEqual(1, task.callable_kwargs.count())
            kwarg.delete()
            self.assertEqual(0, task.callable_kwargs.count())

        def test_parse_args(self):
            task = task_factory(self.TaskModelClass, )
            date = timezone.now()
            taskarg_factory(TaskArg, val='one', content_object=task)
            taskarg_factory(TaskArg, arg_type='int', val=2, content_object=task)
            taskarg_factory(TaskArg, arg_type='bool', val=True, content_object=task)
            taskarg_factory(TaskArg, arg_type='bool', val=False, content_object=task)
            taskarg_factory(TaskArg, arg_type='datetime', val=date, content_object=task)
            self.assertEqual(task.parse_args(), ['one', 2, True, False, date])

        def test_parse_kwargs(self):
            job = task_factory(self.TaskModelClass, )
            date = timezone.now()
            taskarg_factory(TaskKwarg, key='key1', arg_type='str', val='one', content_object=job)
            taskarg_factory(TaskKwarg, key='key2', arg_type='int', val=2, content_object=job)
            taskarg_factory(TaskKwarg, key='key3', arg_type='bool', val=True, content_object=job)
            taskarg_factory(TaskKwarg, key='key4', arg_type='datetime', val=date, content_object=job)
            kwargs = job.parse_kwargs()
            self.assertEqual(kwargs, dict(key1='one', key2=2, key3=True, key4=date))

        def test_callable_args_and_kwargs(self):
            task = task_factory(self.TaskModelClass, callable='scheduler.tests.jobs.test_args_kwargs')
            date = timezone.now()
            taskarg_factory(TaskArg, arg_type='str', val='one', content_object=task)
            taskarg_factory(TaskKwarg, key='key1', arg_type='int', val=2, content_object=task)
            taskarg_factory(TaskKwarg, key='key2', arg_type='datetime', val=date, content_object=task)
            taskarg_factory(TaskKwarg, key='key3', arg_type='bool', val=False, content_object=task)
            task.save()
            entry = _get_job_from_scheduled_registry(task)
            self.assertEqual(entry.perform(),
                             "test_args_kwargs('one', key1=2, key2={}, key3=False)".format(date))

        def test_function_string(self):
            task = task_factory(self.TaskModelClass, )
            date = timezone.now()
            taskarg_factory(TaskArg, arg_type='str', val='one', content_object=task)
            taskarg_factory(TaskArg, arg_type='int', val='1', content_object=task)
            taskarg_factory(TaskArg, arg_type='datetime', val=date, content_object=task)
            taskarg_factory(TaskArg, arg_type='bool', val=True, content_object=task)
            taskarg_factory(TaskKwarg, key='key1', arg_type='str', val='one', content_object=task)
            taskarg_factory(TaskKwarg, key='key2', arg_type='int', val=2, content_object=task)
            taskarg_factory(TaskKwarg, key='key3', arg_type='datetime', val=date, content_object=task)
            taskarg_factory(TaskKwarg, key='key4', arg_type='bool', val=False, content_object=task)
            self.assertEqual(task.function_string(),
                             f"scheduler.tests.jobs.test_job('one', 1, {repr(date)}, True, "
                             f"key1='one', key2=2, key3={repr(date)}, key4=False)")

        def test_admin_list_view(self):
            # arrange
            self.client.login(username='admin', password='admin')
            job = task_factory(self.TaskModelClass, )
            model = job._meta.model.__name__.lower()
            url = reverse(f'admin:scheduler_{model}_changelist')
            # act
            res = self.client.get(url)
            # assert
            self.assertEqual(200, res.status_code)

        def test_admin_list_view_delete_model(self):
            # arrange
            self.client.login(username='admin', password='admin')
            task = task_factory(self.TaskModelClass, )
            model = task._meta.model.__name__.lower()
            url = reverse(f'admin:scheduler_{model}_changelist')
            # act
            res = self.client.post(url, data={
                'action': 'delete_model',
                '_selected_action': [task.pk, ],
            })
            # assert
            self.assertEqual(302, res.status_code)

        def test_admin_run_job_now_enqueues_job_at(self):
            # arrange
            self.client.login(username='admin', password='admin')
            task = task_factory(self.TaskModelClass, )
            model = task._meta.model.__name__.lower()
            url = reverse(f'admin:scheduler_{model}_changelist')
            # act
            res = self.client.post(url, data={
                'action': 'enqueue_job_now',
                '_selected_action': [task.pk, ],
            })
            # assert
            self.assertEqual(302, res.status_code)
            task.refresh_from_db()
            queue = get_queue(task.queue)
            self.assertIn(task.job_id, queue.get_job_ids())

        def test_admin_change_view(self):
            # arrange
            self.client.login(username='admin', password='admin')
            task = task_factory(self.TaskModelClass, )
            model = task._meta.model.__name__.lower()
            url = reverse(f'admin:scheduler_{model}_change', args=[task.pk, ])
            # act
            res = self.client.get(url)
            # assert
            self.assertEqual(200, res.status_code)

        def test_admin_change_view__bad_redis_connection(self):
            # arrange
            self.client.login(username='admin', password='admin')
            task = task_factory(self.TaskModelClass, queue='test2', instance_only=True)
            task.save(schedule_job=False)
            model = task._meta.model.__name__.lower()
            url = reverse(f'admin:scheduler_{model}_change', args=[task.pk, ])
            # act
            res = self.client.get(url)
            # assert
            self.assertEqual(200, res.status_code)

        def test_admin_enqueue_job_now(self):
            # arrange
            self.client.login(username='admin', password='admin')
            task = task_factory(self.TaskModelClass, )
            self.assertIsNotNone(task.job_id)
            self.assertTrue(task.is_scheduled())
            data = {
                'action': 'enqueue_job_now',
                '_selected_action': [task.id, ],
            }
            model = task._meta.model.__name__.lower()
            url = reverse(f'admin:scheduler_{model}_changelist')
            # act
            res = self.client.post(url, data=data, follow=True)

            # assert part 1
            self.assertEqual(200, res.status_code)
            entry = _get_job_from_scheduled_registry(task)
            task_model, scheduled_task_id = entry.args
            self.assertEqual(task_model, task.TASK_TYPE)
            self.assertEqual(scheduled_task_id, task.id)
            self.assertEqual('scheduled', entry.get_status())
            assert_has_execution_with_status(task, 'queued')

            # act 2
            worker = create_worker('default', fork_job_execution=False, )
            worker.work(burst=True)

            # assert 2
            entry = _get_job_from_scheduled_registry(task)
            self.assertEqual(task_model, task.TASK_TYPE)
            self.assertEqual(scheduled_task_id, task.id)
            assert_has_execution_with_status(task, 'finished')

        def test_admin_enable_job(self):
            # arrange
            self.client.login(username='admin', password='admin')
            task = task_factory(self.TaskModelClass, enabled=False)
            self.assertIsNone(task.job_id)
            self.assertFalse(task.is_scheduled())
            data = {
                'action': 'enable_selected',
                '_selected_action': [task.id, ],
            }
            model = task._meta.model.__name__.lower()
            url = reverse(f'admin:scheduler_{model}_changelist')
            # act
            res = self.client.post(url, data=data, follow=True)
            # assert
            self.assertEqual(200, res.status_code)
            task.refresh_from_db()
            self.assertTrue(task.enabled)
            self.assertTrue(task.is_scheduled())
            assert_response_has_msg(res, '1 job was successfully enabled and scheduled.')

        def test_admin_disable_job(self):
            # arrange
            self.client.login(username='admin', password='admin')
            task = task_factory(self.TaskModelClass, enabled=True)
            task.save()
            data = {
                'action': 'disable_selected',
                '_selected_action': [task.id, ],
            }
            model = task._meta.model.__name__.lower()
            url = reverse(f'admin:scheduler_{model}_changelist')
            self.assertTrue(task.is_scheduled())
            # act
            res = self.client.post(url, data=data, follow=True)
            # assert
            self.assertEqual(200, res.status_code)
            task.refresh_from_db()
            self.assertFalse(task.is_scheduled())
            self.assertFalse(task.enabled)
            assert_response_has_msg(res, '1 job was successfully disabled and unscheduled.')

        def test_admin_single_delete(self):
            # arrange
            self.client.login(username='admin', password='admin')
            prev_count = self.TaskModelClass.objects.count()
            task = task_factory(self.TaskModelClass, )
            self.assertIsNotNone(task.job_id)
            self.assertTrue(task.is_scheduled())
            prev = len(_get_executions(task))
            model = task._meta.model.__name__.lower()
            url = reverse(f'admin:scheduler_{model}_delete', args=[task.pk, ])
            data = {
                'post': 'yes',
            }
            # act
            res = self.client.post(url, data=data, follow=True)
            # assert
            self.assertEqual(200, res.status_code)
            self.assertEqual(prev_count, self.TaskModelClass.objects.count())
            self.assertEqual(prev - 1, len(_get_executions(task)))

        def test_admin_delete_selected(self):
            # arrange
            self.client.login(username='admin', password='admin')
            task = task_factory(self.TaskModelClass, enabled=True)
            task.save()
            queue = get_queue(task.queue)
            scheduled_jobs = queue.scheduled_job_registry.get_job_ids()
            job_id = task.job_id
            self.assertIn(job_id, scheduled_jobs)
            data = {
                'action': 'delete_selected',
                '_selected_action': [task.id, ],
                'post': 'yes',
            }
            model = task._meta.model.__name__.lower()
            url = reverse(f'admin:scheduler_{model}_changelist')
            # act
            res = self.client.post(url, data=data, follow=True)
            # assert
            self.assertEqual(200, res.status_code)
            assert_response_has_msg(res, f'Successfully deleted 1 {self.TaskModelClass._meta.verbose_name}.')
            self.assertIsNone(self.TaskModelClass.objects.filter(id=task.id).first())
            scheduled_jobs = queue.scheduled_job_registry.get_job_ids()
            self.assertNotIn(job_id, scheduled_jobs)

    class TestSchedulableJob(TestBaseTask):
        # Currently ScheduledJob and RepeatableJob
        TaskModelClass = ScheduledTask

        @freeze_time("2016-12-25")
        @override_settings(USE_TZ=False)
        def test_schedule_time_no_tz(self):
            task = task_factory(self.TaskModelClass)
            task.scheduled_time = datetime(2016, 12, 25, 8, 0, 0, tzinfo=None)
            self.assertEqual("2016-12-25T08:00:00", task._schedule_time().isoformat())

        @freeze_time("2016-12-25")
        @override_settings(USE_TZ=True)
        def test_schedule_time_with_tz(self):
            task = task_factory(self.TaskModelClass)
            est = zoneinfo.ZoneInfo('US/Eastern')
            task.scheduled_time = datetime(2016, 12, 25, 8, 0, 0, tzinfo=est)
            self.assertEqual("2016-12-25T13:00:00+00:00", task._schedule_time().isoformat())

        def test_result_ttl_passthrough(self):
            job = task_factory(self.TaskModelClass, result_ttl=500)
            entry = _get_job_from_scheduled_registry(job)
            self.assertEqual(entry.result_ttl, 500)


class TestScheduledJob(BaseTestCases.TestSchedulableJob):
    TaskModelClass = ScheduledTask

    def test_clean(self):
        job = task_factory(self.TaskModelClass)
        job.queue = list(settings.QUEUES)[0]
        job.callable = 'scheduler.tests.jobs.test_job'
        self.assertIsNone(job.clean())

    def test_unschedulable_old_job(self):
        job = task_factory(self.TaskModelClass, scheduled_time=timezone.now() - timedelta(hours=1))
        self.assertFalse(job.is_scheduled())
