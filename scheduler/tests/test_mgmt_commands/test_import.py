import json
import os
import tempfile
from unittest import mock

import yaml
from django.core.management import call_command
from django.test import TestCase

from scheduler.models import Task
from scheduler.tests import test_settings  # noqa
from scheduler.tests.testtools import task_factory
from scheduler.models import TaskType


class ImportTest(TestCase):
    def setUp(self) -> None:
        self.tmpfile = tempfile.NamedTemporaryFile(mode="w")

    def tearDown(self) -> None:
        os.remove(self.tmpfile.name)

    def test_import__should_schedule_job(self):
        jobs = list()
        jobs.append(task_factory(TaskType.ONCE, enabled=True, instance_only=True))
        jobs.append(task_factory(TaskType.REPEATABLE, enabled=True, instance_only=True))
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
        tasks.append(task_factory(TaskType.ONCE, enabled=True, instance_only=True))
        tasks.append(task_factory(TaskType.REPEATABLE, enabled=True, instance_only=True))
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
        jobs.append(task_factory(TaskType.ONCE, enabled=True, instance_only=True))
        jobs.append(task_factory(TaskType.REPEATABLE, enabled=True, instance_only=True))
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
        task_factory(TaskType.ONCE, enabled=True)
        task_factory(TaskType.ONCE, enabled=True)
        jobs.append(task_factory(TaskType.ONCE, enabled=True))
        jobs.append(task_factory(TaskType.REPEATABLE, enabled=True, instance_only=True))
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
        tasks.append(task_factory(TaskType.ONCE, enabled=True))
        tasks.append(task_factory(TaskType.ONCE, enabled=True))
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
        tasks.append(task_factory(TaskType.ONCE, enabled=True))
        tasks.append(task_factory(TaskType.ONCE, enabled=True))
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
