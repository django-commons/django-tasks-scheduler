import json
import os
import tempfile
from unittest import mock

import yaml
from django.core.management import call_command
from django.test import TestCase

from scheduler.models import ScheduledTask, RepeatableTask, Task
from scheduler.queues import get_queue
from scheduler.tests.jobs import failing_job, test_job
from scheduler.tests.testtools import task_factory
from . import test_settings  # noqa
from .test_views import BaseTestCase
from ..models.task import TaskType
from ..tools import create_worker


class RqworkerTestCase(TestCase):

    def test_rqworker__no_queues_params(self):
        queue = get_queue("default")

        # enqueue some jobs that will fail
        jobs = []
        job_ids = []
        for _ in range(0, 3):
            job = queue.enqueue(failing_job)
            jobs.append(job)
            job_ids.append(job.id)

        # Create a worker to execute these jobs
        call_command("rqworker", fork_job_execution=False, burst=True)

        # check if all jobs are really failed
        for job in jobs:
            self.assertTrue(job.is_failed)

    def test_rqworker__job_class_param__green(self):
        queue = get_queue("default")

        # enqueue some jobs that will fail
        jobs = []
        job_ids = []
        for _ in range(0, 3):
            job = queue.enqueue(failing_job)
            jobs.append(job)
            job_ids.append(job.id)

        # Create a worker to execute these jobs
        call_command(
            "rqworker", "--job-class", "scheduler.rq_classes.JobExecution", fork_job_execution=False, burst=True
        )

        # check if all jobs are really failed
        for job in jobs:
            self.assertTrue(job.is_failed)

    def test_rqworker__bad_job_class__fail(self):
        queue = get_queue("default")

        # enqueue some jobs that will fail
        jobs = []
        job_ids = []
        for _ in range(0, 3):
            job = queue.enqueue(failing_job)
            jobs.append(job)
            job_ids.append(job.id)

        # Create a worker to execute these jobs
        with self.assertRaises(ImportError):
            call_command("rqworker", "--job-class", "rq.badclass", fork_job_execution=False, burst=True)

    def test_rqworker__run_jobs(self):
        queue = get_queue("default")

        # enqueue some jobs that will fail
        jobs = []
        job_ids = []
        for _ in range(0, 3):
            job = queue.enqueue(failing_job)
            jobs.append(job)
            job_ids.append(job.id)

        # Create a worker to execute these jobs
        call_command("rqworker", "default", fork_job_execution=False, burst=True)

        # check if all jobs are really failed
        for job in jobs:
            self.assertTrue(job.is_failed)

    def test_rqworker__worker_with_two_queues(self):
        queue = get_queue("default")
        queue2 = get_queue("django_tasks_scheduler_test")

        # enqueue some jobs that will fail
        jobs = []
        job_ids = []
        for _ in range(0, 3):
            job = queue.enqueue(failing_job)
            jobs.append(job)
            job_ids.append(job.id)
        job = queue2.enqueue(failing_job)
        jobs.append(job)
        job_ids.append(job.id)

        # Create a worker to execute these jobs
        call_command("rqworker", "default", "django_tasks_scheduler_test", fork_job_execution=False, burst=True)

        # check if all jobs are really failed
        for job in jobs:
            self.assertTrue(job.is_failed)

    def test_rqworker__worker_with_one_queue__does_not_perform_other_queue_job(self):
        queue = get_queue("default")
        queue2 = get_queue("django_tasks_scheduler_test")

        job = queue.enqueue(failing_job)
        other_job = queue2.enqueue(failing_job)

        # Create a worker to execute these jobs
        call_command("rqworker", "default", fork_job_execution=False, burst=True)
        # assert
        self.assertTrue(job.is_failed)
        self.assertTrue(other_job.is_queued)


class RqstatsTest(TestCase):
    def test_rqstats__does_not_fail(self):
        call_command("rqstats", "-j")
        call_command("rqstats", "-y")
        call_command("rqstats")


class DeleteFailedExecutionsTest(BaseTestCase):
    def test_delete_failed_executions__delete_jobs(self):
        queue = get_queue("default")
        call_command("delete_failed_executions", queue="default")
        queue.enqueue(failing_job)
        worker = create_worker("default")
        worker.work(burst=True)
        self.assertEqual(1, len(queue.failed_job_registry))
        call_command("delete_failed_executions", queue="default")
        self.assertEqual(0, len(queue.failed_job_registry))


class RunJobTest(TestCase):
    def test_run_job__should_schedule_job(self):
        queue = get_queue("default")
        queue.empty()
        func_name = f"{test_job.__module__}.{test_job.__name__}"
        # act
        call_command("run_job", func_name, queue="default")
        # assert
        job_list = queue.get_jobs()
        self.assertEqual(1, len(job_list))
        self.assertEqual(func_name + "()", job_list[0].get_call_string())


class ExportTest(TestCase):
    def setUp(self) -> None:
        self.tmpfile = tempfile.NamedTemporaryFile()

    def tearDown(self) -> None:
        os.remove(self.tmpfile.name)

    def test_export__should_export_job(self):
        jobs = list()
        jobs.append(task_factory(ScheduledTask, enabled=True))
        jobs.append(task_factory(RepeatableTask, enabled=True))

        # act
        call_command("export", filename=self.tmpfile.name)
        # assert
        result = json.load(self.tmpfile)
        self.assertEqual(len(jobs), len(result))
        self.assertEqual(result[0], jobs[0].to_dict())
        self.assertEqual(result[1], jobs[1].to_dict())

    def test_export__should_export_enabled_jobs_only(self):
        jobs = list()
        jobs.append(task_factory(ScheduledTask, enabled=True))
        jobs.append(task_factory(RepeatableTask, enabled=False))

        # act
        call_command("export", filename=self.tmpfile.name, enabled=True)
        # assert
        result = json.load(self.tmpfile)
        self.assertEqual(len(jobs) - 1, len(result))
        self.assertEqual(result[0], jobs[0].to_dict())

    def test_export__should_export_job_yaml_without_yaml_lib(self):
        jobs = list()
        jobs.append(task_factory(ScheduledTask, enabled=True))
        jobs.append(task_factory(RepeatableTask, enabled=True))

        # act
        with mock.patch.dict("sys.modules", {"yaml": None}):
            with self.assertRaises(SystemExit) as cm:
                call_command("export", filename=self.tmpfile.name, format="yaml")
            self.assertEqual(cm.exception.code, 1)

    def test_export__should_export_job_yaml_green(self):
        jobs = list()
        jobs.append(task_factory(ScheduledTask, enabled=True))
        jobs.append(task_factory(RepeatableTask, enabled=True))

        # act
        call_command("export", filename=self.tmpfile.name, format="yaml")
        # assert
        result = yaml.load(self.tmpfile, yaml.SafeLoader)
        self.assertEqual(len(jobs), len(result))
        self.assertEqual(result[0], jobs[0].to_dict())
        self.assertEqual(result[1], jobs[1].to_dict())


class ImportTest(TestCase):
    def setUp(self) -> None:
        self.tmpfile = tempfile.NamedTemporaryFile(mode="w")

    def tearDown(self) -> None:
        os.remove(self.tmpfile.name)

    def test_import__should_schedule_job(self):
        jobs = list()
        jobs.append(task_factory(ScheduledTask, enabled=True, instance_only=True))
        jobs.append(task_factory(RepeatableTask, enabled=True, instance_only=True))
        res = json.dumps([j.to_dict() for j in jobs])
        self.tmpfile.write(res)
        self.tmpfile.flush()
        # act
        call_command("import", filename=self.tmpfile.name)
        # assert
        self.assertEqual(1, Task.objects.filter(task_type=TaskType.ONCE).count())
        self.assertEqual(1, Task.objects.filter(task_type=TaskType.REPEATABLE).count())
        db_job = Task.objects.filter(task_type=TaskType.ONCE).first()
        attrs = ["name", "queue", "callable", "enabled", "timeout"]
        for attr in attrs:
            self.assertEqual(getattr(jobs[0], attr), getattr(db_job, attr))

    def test_import__should_schedule_job_yaml(self):
        tasks = list()
        tasks.append(task_factory(ScheduledTask, enabled=True, instance_only=True))
        tasks.append(task_factory(RepeatableTask, enabled=True, instance_only=True))
        res = yaml.dump([j.to_dict() for j in tasks], default_flow_style=False)
        self.tmpfile.write(res)
        self.tmpfile.flush()
        # act
        call_command("import", filename=self.tmpfile.name, format="yaml")
        # assert
        self.assertEqual(1, Task.objects.filter(task_type=TaskType.ONCE).count())
        self.assertEqual(1, Task.objects.filter(task_type=TaskType.REPEATABLE).count())
        db_job = Task.objects.filter(task_type=TaskType.ONCE).first()
        attrs = ["name", "queue", "callable", "enabled", "timeout"]
        for attr in attrs:
            self.assertEqual(getattr(tasks[0], attr), getattr(db_job, attr))

    def test_import__should_schedule_job_yaml_without_yaml_lib(self):
        jobs = list()
        jobs.append(task_factory(ScheduledTask, enabled=True, instance_only=True))
        jobs.append(task_factory(RepeatableTask, enabled=True, instance_only=True))
        res = yaml.dump([j.to_dict() for j in jobs], default_flow_style=False)
        self.tmpfile.write(res)
        self.tmpfile.flush()
        # act
        with mock.patch.dict("sys.modules", {"yaml": None}):
            with self.assertRaises(SystemExit) as cm:
                call_command("import", filename=self.tmpfile.name, format="yaml")
            self.assertEqual(cm.exception.code, 1)

    def test_import__should_schedule_job_reset(self):
        jobs = list()
        task_factory(ScheduledTask, enabled=True)
        task_factory(ScheduledTask, enabled=True)
        jobs.append(task_factory(ScheduledTask, enabled=True))
        jobs.append(task_factory(RepeatableTask, enabled=True, instance_only=True))
        res = json.dumps([j.to_dict() for j in jobs])
        self.tmpfile.write(res)
        self.tmpfile.flush()
        # act
        call_command(
            "import",
            filename=self.tmpfile.name,
            reset=True,
        )
        # assert
        self.assertEqual(1, Task.objects.filter(task_type=TaskType.ONCE).count())
        db_job = Task.objects.filter(task_type=TaskType.ONCE).first()
        attrs = ["name", "queue", "callable", "enabled", "timeout"]
        for attr in attrs:
            self.assertEqual(getattr(jobs[0], attr), getattr(db_job, attr))
        self.assertEqual(1, Task.objects.filter(task_type=TaskType.REPEATABLE).count())
        db_job = Task.objects.filter(task_type=TaskType.REPEATABLE).first()
        attrs = ["name", "queue", "callable", "enabled", "timeout"]
        for attr in attrs:
            self.assertEqual(getattr(jobs[1], attr), getattr(db_job, attr))

    def test_import__should_schedule_job_update_existing(self):
        tasks = list()
        tasks.append(task_factory(ScheduledTask, enabled=True))
        tasks.append(task_factory(ScheduledTask, enabled=True))
        res = json.dumps([j.to_dict() for j in tasks])
        self.tmpfile.write(res)
        self.tmpfile.flush()
        # act
        call_command(
            "import",
            filename=self.tmpfile.name,
            update=True,
        )
        # assert
        self.assertEqual(2, Task.objects.filter(task_type=TaskType.ONCE).count())
        db_job = Task.objects.filter(task_type=TaskType.ONCE).get(name=tasks[0].name)
        attrs = ["name", "queue", "callable", "enabled", "timeout"]
        for attr in attrs:
            self.assertEqual(getattr(tasks[0], attr), getattr(db_job, attr))

    def test_import__should_schedule_job_without_update_existing(self):
        tasks = list()
        tasks.append(task_factory(ScheduledTask, enabled=True))
        tasks.append(task_factory(ScheduledTask, enabled=True))
        res = json.dumps([j.to_dict() for j in tasks])
        self.tmpfile.write(res)
        self.tmpfile.flush()
        # act
        call_command(
            "import",
            filename=self.tmpfile.name,
        )
        # assert
        self.assertEqual(2, Task.objects.filter(task_type=TaskType.ONCE).count())
        db_job = Task.objects.get(name=tasks[0].name)
        attrs = ["id", "name", "queue", "callable", "enabled", "timeout"]
        for attr in attrs:
            self.assertEqual(getattr(tasks[0], attr), getattr(db_job, attr))
