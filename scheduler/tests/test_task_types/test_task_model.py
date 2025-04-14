import zoneinfo
from datetime import datetime, timedelta

import time_machine
from django.contrib.messages import get_messages
from django.core.exceptions import ValidationError
from django.test import override_settings
from django.urls import reverse
from django.utils import timezone

from scheduler import settings
from scheduler.helpers.queues import get_queue
from scheduler.helpers.queues import perform_job
from scheduler.models import TaskType, Task, TaskArg, TaskKwarg, run_task
from scheduler.redis_models import JobStatus, JobModel
from scheduler.tests import jobs, test_settings  # noqa
from scheduler.tests.testtools import (
    task_factory,
    taskarg_factory,
    _get_task_scheduled_job_from_registry,
    SchedulerBaseCase,
    _get_executions,
)
from scheduler.worker import create_worker


def assert_response_has_msg(response, message):
    messages = [m.message for m in get_messages(response.wsgi_request)]
    assert message in messages, f'expected "{message}" in {messages}'


def assert_has_execution_with_status(task, status):
    job_list = _get_executions(task)
    job_list = [(j.name, j.get_status(connection=task.rqueue.connection)) for j in job_list]
    for job in job_list:
        if job[1] == status:
            return
    raise AssertionError(f"{task} does not have an execution with status {status}: {job_list}")


class BaseTestCases:
    class TestBaseTask(SchedulerBaseCase):
        task_type = None
        queue_name = settings.get_queue_names()[0]

        def test_callable_func(self):
            task = task_factory(self.task_type)
            task.callable = "scheduler.tests.jobs.test_job"
            func = task.callable_func()
            self.assertEqual(jobs.test_job, func)

        def test_callable_func_not_callable(self):
            task = task_factory(self.task_type)
            task.callable = "scheduler.tests.jobs.test_non_callable"
            with self.assertRaises(TypeError):
                task.callable_func()

        def test_clean_callable(self):
            task = task_factory(self.task_type)
            task.callable = "scheduler.tests.jobs.test_job"
            self.assertIsNone(task.clean_callable())

        def test_clean_callable_invalid(self):
            task = task_factory(self.task_type)
            task.callable = "scheduler.tests.jobs.test_non_callable"
            with self.assertRaises(ValidationError):
                task.clean_callable()

        def test_clean_queue(self):
            for queue in settings.get_queue_names():
                task = task_factory(self.task_type)
                task.queue = queue
                self.assertIsNone(task.clean_queue())

        def test_clean_queue_invalid(self):
            task = task_factory(self.task_type)
            task.queue = "xxxxxx"
            task.callable = "scheduler.tests.jobs.test_job"
            with self.assertRaises(ValidationError):
                task.clean()

        # next 2 check the above are included in job.clean() function
        def test_clean_base(self):
            task = task_factory(self.task_type)
            task.queue = self.queue_name
            task.callable = "scheduler.tests.jobs.test_job"
            self.assertIsNone(task.clean())

        def test_clean_invalid_callable(self):
            task = task_factory(self.task_type)
            task.queue = self.queue_name
            task.callable = "scheduler.tests.jobs.test_non_callable"
            with self.assertRaises(ValidationError):
                task.clean()

        def test_clean_invalid_queue(self):
            task = task_factory(self.task_type)
            task.queue = "xxxxxx"
            task.callable = "scheduler.tests.jobs.test_job"
            with self.assertRaises(ValidationError):
                task.clean()

        def test_is_schedulable_already_scheduled(self):
            task = task_factory(self.task_type)
            task.save()
            self.assertTrue(task.is_scheduled())

        def test_is_schedulable_disabled(self):
            task = task_factory(self.task_type)
            task.enabled = False
            self.assertFalse(task.enabled)

        def test_schedule(self):
            task = task_factory(
                self.task_type,
            )
            self.assertTrue(task.is_scheduled())
            self.assertIsNotNone(task.job_name)

        def test_unschedulable(self):
            task = task_factory(self.task_type, enabled=False)
            self.assertFalse(task.is_scheduled())
            self.assertIsNone(task.job_name)

        def test_unschedule(self):
            task = task_factory(self.task_type)
            self.assertTrue(task.unschedule())
            self.assertIsNone(task.job_name)

        def test_unschedule_not_scheduled(self):
            task = task_factory(self.task_type, enabled=False)
            self.assertTrue(task.unschedule())
            self.assertIsNone(task.job_name)

        def test_save_enabled(self):
            task = task_factory(self.task_type)
            self.assertIsNotNone(task.job_name)

        def test_save_disabled(self):
            task = task_factory(self.task_type, enabled=False)
            task.save()
            self.assertIsNone(task.job_name)

        def test_save_and_schedule(self):
            task = task_factory(self.task_type)
            self.assertIsNotNone(task.job_name)
            self.assertTrue(task.is_scheduled())

        def test_schedule2(self):
            task = task_factory(self.task_type)
            task.queue = self.queue_name
            task.enabled = False
            task.scheduled_time = timezone.now() + timedelta(minutes=1)
            self.assertFalse(task._schedule())

        def test_delete_and_unschedule(self):
            task = task_factory(self.task_type)
            self.assertIsNotNone(task.job_name)
            self.assertTrue(task.is_scheduled())
            task.delete()
            self.assertFalse(task.is_scheduled())

        def test_job_create(self):
            prev_count = Task.objects.filter(task_type=self.task_type).count()
            task_factory(self.task_type)
            self.assertEqual(Task.objects.filter(task_type=self.task_type).count(), prev_count + 1)

        def test_str(self):
            name = "test"
            task = task_factory(self.task_type, name=name)
            self.assertEqual(f"{self.task_type.value}[{name}={task.callable}()]", str(task))

        def test_callable_passthrough(self):
            task = task_factory(self.task_type)
            entry = _get_task_scheduled_job_from_registry(task)
            self.assertEqual(entry.func, run_task)
            job_model, task_id = entry.args
            self.assertEqual(job_model, self.task_type.value)
            self.assertEqual(task_id, task.id)

        def test_timeout_passthrough(self):
            task = task_factory(self.task_type, timeout=500)
            entry = _get_task_scheduled_job_from_registry(task)
            self.assertEqual(entry.timeout, 500)

        def test_at_front_passthrough(self):
            task = task_factory(self.task_type, at_front=True)
            queue = task.rqueue
            jobs_to_schedule = queue.scheduled_job_registry.all()
            self.assertIn(task.job_name, jobs_to_schedule)

        def test_callable_result(self):
            task = task_factory(self.task_type)
            entry = _get_task_scheduled_job_from_registry(task)
            queue = get_queue("default")
            self.assertEqual(perform_job(entry, connection=queue.connection), 2)

        def test_callable_empty_args_and_kwargs(self):
            task = task_factory(self.task_type, callable="scheduler.tests.jobs.test_args_kwargs")
            entry = _get_task_scheduled_job_from_registry(task)
            queue = get_queue("default")
            self.assertEqual(perform_job(entry, connection=queue.connection), "test_args_kwargs()")

        def test_delete_args(self):
            task = task_factory(self.task_type)
            arg = taskarg_factory(TaskArg, val="one", content_object=task)
            self.assertEqual(1, task.callable_args.count())
            arg.delete()
            self.assertEqual(0, task.callable_args.count())

        def test_delete_kwargs(self):
            task = task_factory(self.task_type)
            kwarg = taskarg_factory(TaskKwarg, key="key1", arg_type="str", val="one", content_object=task)
            self.assertEqual(1, task.callable_kwargs.count())
            kwarg.delete()
            self.assertEqual(0, task.callable_kwargs.count())

        def test_parse_args(self):
            task = task_factory(self.task_type)
            date = timezone.now()
            taskarg_factory(TaskArg, val="one", content_object=task)
            taskarg_factory(TaskArg, arg_type="int", val=2, content_object=task)
            taskarg_factory(TaskArg, arg_type="bool", val=True, content_object=task)
            taskarg_factory(TaskArg, arg_type="bool", val=False, content_object=task)
            taskarg_factory(TaskArg, arg_type="datetime", val=date, content_object=task)
            self.assertEqual(task.parse_args(), ["one", 2, True, False, date])

        def test_parse_kwargs(self):
            job = task_factory(self.task_type)
            date = timezone.now()
            taskarg_factory(TaskKwarg, key="key1", arg_type="str", val="one", content_object=job)
            taskarg_factory(TaskKwarg, key="key2", arg_type="int", val=2, content_object=job)
            taskarg_factory(TaskKwarg, key="key3", arg_type="bool", val=True, content_object=job)
            taskarg_factory(TaskKwarg, key="key4", arg_type="datetime", val=date, content_object=job)
            kwargs = job.parse_kwargs()
            self.assertEqual(kwargs, dict(key1="one", key2=2, key3=True, key4=date))

        def test_callable_args_and_kwargs(self):
            task = task_factory(self.task_type, callable="scheduler.tests.jobs.test_args_kwargs")
            date = timezone.now()
            taskarg_factory(TaskArg, arg_type="str", val="one", content_object=task)
            taskarg_factory(TaskKwarg, key="key1", arg_type="int", val=2, content_object=task)
            taskarg_factory(TaskKwarg, key="key2", arg_type="datetime", val=date, content_object=task)
            taskarg_factory(TaskKwarg, key="key3", arg_type="bool", val=False, content_object=task)
            task.save()
            entry = _get_task_scheduled_job_from_registry(task)
            queue = get_queue("default")
            self.assertEqual(
                perform_job(entry, connection=queue.connection),
                "test_args_kwargs('one', key1=2, key2={}, key3=False)".format(date),
            )

        def test_function_string(self):
            task = task_factory(self.task_type)
            date = timezone.now()
            taskarg_factory(TaskArg, arg_type="str", val="one", content_object=task)
            taskarg_factory(TaskArg, arg_type="int", val="1", content_object=task)
            taskarg_factory(TaskArg, arg_type="datetime", val=date, content_object=task)
            taskarg_factory(TaskArg, arg_type="bool", val=True, content_object=task)
            taskarg_factory(TaskKwarg, key="key1", arg_type="str", val="one", content_object=task)
            taskarg_factory(TaskKwarg, key="key2", arg_type="int", val=2, content_object=task)
            taskarg_factory(TaskKwarg, key="key3", arg_type="datetime", val=date, content_object=task)
            taskarg_factory(TaskKwarg, key="key4", arg_type="bool", val=False, content_object=task)
            self.assertEqual(
                task.function_string(),
                f"scheduler.tests.jobs.test_job('one', 1, {repr(date)}, True, "
                f"key1='one', key2=2, key3={repr(date)}, key4=False)",
            )

        def test_admin_list_view(self):
            # arrange
            self.client.login(username="admin", password="admin")
            task_factory(self.task_type)
            url = reverse("admin:scheduler_task_changelist")
            # act
            res = self.client.get(url)
            # assert
            self.assertEqual(200, res.status_code)

        def test_admin_list_view_delete_model(self):
            # arrange
            self.client.login(username="admin", password="admin")
            task = task_factory(
                self.task_type,
            )
            url = reverse("admin:scheduler_task_changelist")
            # act
            res = self.client.post(
                url,
                data={
                    "action": "delete_model",
                    "_selected_action": [
                        task.pk,
                    ],
                },
            )
            # assert
            self.assertEqual(302, res.status_code)

        def test_admin_run_job_now_enqueues_job_at(self):
            # arrange
            self.client.login(username="admin", password="admin")
            task = task_factory(self.task_type)
            url = reverse("admin:scheduler_task_changelist")
            # act
            res = self.client.post(
                url,
                data={
                    "action": "enqueue_job_now",
                    "_selected_action": [
                        task.pk,
                    ],
                },
            )
            # assert
            self.assertEqual(302, res.status_code)
            task.refresh_from_db()
            queue = get_queue(task.queue)
            assert_has_execution_with_status(task, JobStatus.QUEUED)
            self.assertIn(task.job_name, queue.scheduled_job_registry.all())

        def test_admin_change_view(self):
            # arrange
            self.client.login(username="admin", password="admin")
            task = task_factory(
                self.task_type,
            )
            url = reverse(
                "admin:scheduler_task_change",
                args=[
                    task.pk,
                ],
            )
            # act
            res = self.client.get(url)
            # assert
            self.assertEqual(200, res.status_code)

        def test_admin_change_view__bad_redis_connection(self):
            # arrange
            self.client.login(username="admin", password="admin")
            task = task_factory(self.task_type, queue="test2", instance_only=True)
            task.save(schedule_job=False)
            url = reverse(
                "admin:scheduler_task_change",
                args=[
                    task.pk,
                ],
            )
            # act
            res = self.client.get(url)
            # assert
            self.assertEqual(200, res.status_code)

        def test_admin_enqueue_job_now(self):
            # arrange
            self.client.login(username="admin", password="admin")
            task = task_factory(self.task_type)
            self.assertIsNotNone(task.job_name)
            job = JobModel.get(task.job_name, connection=task.rqueue.connection)
            self.assertEqual(job.status, JobStatus.SCHEDULED)
            self.assertTrue(task.is_scheduled())
            data = {
                "action": "enqueue_job_now",
                "_selected_action": [
                    task.id,
                ],
            }
            url = reverse("admin:scheduler_task_changelist")
            # act
            res = self.client.post(url, data=data, follow=True)

            # assert part 1
            self.assertEqual(200, res.status_code)
            assert_has_execution_with_status(task, JobStatus.QUEUED)
            entry = _get_task_scheduled_job_from_registry(task)
            task_type, scheduled_task_id = entry.args
            self.assertEqual(task_type, task.task_type)
            self.assertEqual(scheduled_task_id, task.id)
            self.assertEqual(JobStatus.SCHEDULED, entry.get_status(connection=task.rqueue.connection))

            # act 2
            worker = create_worker("default", fork_job_execution=False, burst=True)
            worker.work()

            # assert 2
            entry = _get_task_scheduled_job_from_registry(task)
            self.assertEqual(task_type, task.task_type)
            self.assertEqual(scheduled_task_id, task.id)
            assert_has_execution_with_status(task, "finished")

        def test_admin_enable_job(self):
            # arrange
            self.client.login(username="admin", password="admin")
            task = task_factory(self.task_type, enabled=False)
            self.assertIsNone(task.job_name)
            self.assertFalse(task.is_scheduled())
            data = {
                "action": "enable_selected",
                "_selected_action": [
                    task.id,
                ],
            }
            url = reverse("admin:scheduler_task_changelist")
            # act
            res = self.client.post(url, data=data, follow=True)
            # assert
            self.assertEqual(200, res.status_code)
            task.refresh_from_db()
            self.assertTrue(task.enabled)
            self.assertTrue(task.is_scheduled())
            assert_response_has_msg(res, "1 task was successfully enabled and scheduled.")

        def test_admin_disable_job(self):
            # arrange
            self.client.login(username="admin", password="admin")
            task = task_factory(self.task_type, enabled=True)
            task.save()
            data = {
                "action": "disable_selected",
                "_selected_action": [
                    task.id,
                ],
            }
            url = reverse("admin:scheduler_task_changelist")
            self.assertTrue(task.is_scheduled())
            # act
            res = self.client.post(url, data=data, follow=True)
            # assert
            self.assertEqual(200, res.status_code)
            task.refresh_from_db()
            self.assertFalse(task.is_scheduled())
            self.assertFalse(task.enabled)
            assert_response_has_msg(res, "1 task was successfully disabled and unscheduled.")

        def test_admin_single_delete(self):
            # arrange
            self.client.login(username="admin", password="admin")
            prev_count = Task.objects.filter(task_type=self.task_type).count()
            task = task_factory(self.task_type)
            self.assertIsNotNone(task.job_name)
            self.assertTrue(task.is_scheduled())
            prev_executions_count = len(_get_executions(task))
            url = reverse("admin:scheduler_task_delete", args=[task.pk])
            data = dict(post="yes")
            # act
            res = self.client.post(url, data=data, follow=True)
            # assert
            self.assertEqual(200, res.status_code)
            self.assertEqual(prev_count, Task.objects.filter(task_type=self.task_type).count())
            self.assertEqual(prev_executions_count - 1, len(_get_executions(task)))

        def test_admin_delete_selected(self):
            # arrange
            self.client.login(username="admin", password="admin")
            task = task_factory(self.task_type, enabled=True)
            task.save()
            queue = get_queue(task.queue)
            scheduled_jobs = queue.scheduled_job_registry.all()
            job_name = task.job_name
            self.assertIn(job_name, scheduled_jobs)
            data = {
                "action": "delete_selected",
                "_selected_action": [
                    task.id,
                ],
                "post": "yes",
            }
            url = reverse("admin:scheduler_task_changelist")
            # act
            res = self.client.post(url, data=data, follow=True)
            # assert
            self.assertEqual(200, res.status_code)
            assert_response_has_msg(res, "Successfully deleted 1 task.")
            self.assertIsNone(Task.objects.filter(task_type=self.task_type).filter(id=task.id).first())
            scheduled_jobs = queue.scheduled_job_registry.all()
            self.assertNotIn(job_name, scheduled_jobs)

    class TestSchedulableTask(TestBaseTask):
        # Currently ScheduledJob and RepeatableJob
        task_type = TaskType.ONCE

        @time_machine.travel(datetime(2016, 12, 25))
        @override_settings(USE_TZ=False)
        def test_schedule_time_no_tz(self):
            task = task_factory(self.task_type)
            task.scheduled_time = datetime(2016, 12, 25, 8, 0, 0, tzinfo=None)
            self.assertEqual("2016-12-25T08:00:00", task._schedule_time().isoformat())

        @time_machine.travel(datetime(2016, 12, 25))
        @override_settings(USE_TZ=True)
        def test_schedule_time_with_tz(self):
            task = task_factory(self.task_type)
            est = zoneinfo.ZoneInfo("US/Eastern")
            task.scheduled_time = datetime(2016, 12, 25, 8, 0, 0, tzinfo=est)
            self.assertEqual("2016-12-25T13:00:00+00:00", task._schedule_time().isoformat())

        def test_result_ttl_passthrough(self):
            job = task_factory(self.task_type, result_ttl=500)
            entry = _get_task_scheduled_job_from_registry(job)
            self.assertEqual(entry.success_ttl, 500)
