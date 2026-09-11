import json
import os
import tempfile
from unittest import mock

import yaml
from django.core.management import call_command
from django.test import TestCase

from scheduler.models import Task, TaskArg, TaskKwarg, TaskType
from scheduler.tests import conf  # noqa
from scheduler.tests.testtools import task_factory, taskarg_factory


class ImportTest(TestCase):
    def setUp(self) -> None:
        # tearDown removes the file itself. With delete=True (The default behaviour), __del__ unlinks it a
        # second time and python<3.12 dumps that FileNotFoundError to stderr.
        # https://github.com/python/cpython/blob/3.11/Lib/tempfile.py#L463
        self.tmpfile = tempfile.NamedTemporaryFile(mode="w", delete=False)

    def tearDown(self) -> None:
        os.remove(self.tmpfile.name)

    def test_import__should_schedule_job(self):
        tasks = []
        tasks.append(task_factory(TaskType.ONCE, enabled=True, instance_only=True))
        tasks.append(task_factory(TaskType.REPEATABLE, enabled=True, instance_only=True))
        res = json.dumps([j.to_dict() for j in tasks])
        self.tmpfile.write(res)
        self.tmpfile.flush()
        # act
        call_command("import", filename=self.tmpfile.name)
        # assert
        self.assertEqual(1, Task.objects.filter(task_type=TaskType.ONCE).count())
        self.assertEqual(1, Task.objects.filter(task_type=TaskType.REPEATABLE).count())
        db_task = Task.objects.filter(task_type=TaskType.ONCE).first()
        attrs = ["name", "queue", "callable", "enabled", "timeout"]
        for attr in attrs:
            self.assertEqual(getattr(tasks[0], attr), getattr(db_task, attr))

    def test_import__should_schedule_job_yaml(self):
        tasks = []
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
        task = Task.objects.filter(task_type=TaskType.ONCE).first()
        attrs = ["name", "queue", "callable", "enabled", "timeout"]
        for attr in attrs:
            self.assertEqual(getattr(tasks[0], attr), getattr(task, attr))

    def test_import__should_schedule_job_yaml_without_yaml_lib(self):
        tasks = []
        tasks.append(task_factory(TaskType.ONCE, enabled=True, instance_only=True))
        tasks.append(task_factory(TaskType.REPEATABLE, enabled=True, instance_only=True))
        res = yaml.dump([j.to_dict() for j in tasks], default_flow_style=False)
        self.tmpfile.write(res)
        self.tmpfile.flush()
        # act
        with mock.patch.dict("sys.modules", {"yaml": None}):
            with self.assertRaises(SystemExit) as cm:
                call_command("import", filename=self.tmpfile.name, format="yaml")
            self.assertEqual(cm.exception.code, 1)

    def test_import__should_schedule_job_reset(self):
        tasks = []
        task_factory(TaskType.ONCE, enabled=True)
        task_factory(TaskType.ONCE, enabled=True)
        tasks.append(task_factory(TaskType.ONCE, enabled=True))
        tasks.append(task_factory(TaskType.REPEATABLE, enabled=True, instance_only=True))
        res = json.dumps([j.to_dict() for j in tasks])
        self.tmpfile.write(res)
        self.tmpfile.flush()
        # act
        call_command("import", filename=self.tmpfile.name, reset=True)
        # assert
        self.assertEqual(1, Task.objects.filter(task_type=TaskType.ONCE).count())
        task = Task.objects.filter(task_type=TaskType.ONCE).first()
        attrs = ["name", "queue", "callable", "enabled", "timeout"]
        for attr in attrs:
            self.assertEqual(getattr(tasks[0], attr), getattr(task, attr))
        self.assertEqual(1, Task.objects.filter(task_type=TaskType.REPEATABLE).count())
        task = Task.objects.filter(task_type=TaskType.REPEATABLE).first()
        attrs = ["name", "queue", "callable", "enabled", "timeout"]
        for attr in attrs:
            self.assertEqual(getattr(tasks[1], attr), getattr(task, attr))

    def test_import__creates_the_arguments(self):
        task = task_factory(TaskType.ONCE, enabled=True)
        taskarg_factory(TaskArg, val="one", content_object=task)
        taskarg_factory(TaskArg, arg_type="int", val="2", content_object=task)
        taskarg_factory(TaskKwarg, key="k", val="three", content_object=task)
        exported = task.to_dict()
        task.delete()
        self.tmpfile.write(json.dumps([exported]))
        self.tmpfile.flush()

        call_command("import", filename=self.tmpfile.name)

        imported = Task.objects.get(name=exported["name"]).to_dict()
        self.assertEqual(exported["callable_args"], imported["callable_args"])
        self.assertEqual(exported["callable_kwargs"], imported["callable_kwargs"])

    def test_import__invalid_entry__changes_nothing(self):
        existing = task_factory(TaskType.ONCE, enabled=True)
        new = task_factory(TaskType.ONCE, enabled=True, instance_only=True).to_dict()
        self.tmpfile.write(json.dumps([new, {**new, "name": "broken", "model": "NoSuchTask"}]))
        self.tmpfile.flush()

        with self.assertRaises(ValueError):
            call_command("import", filename=self.tmpfile.name, reset=True)

        self.assertEqual([existing.name], list(Task.objects.values_list("name", flat=True)))

    def test_import__reset__unschedules_the_removed_tasks(self):
        removed = task_factory(TaskType.ONCE, enabled=True)
        self.tmpfile.write(json.dumps([]))
        self.tmpfile.flush()

        call_command("import", filename=self.tmpfile.name, reset=True)

        queue = removed.rqueue
        self.assertFalse(queue.scheduled_job_registry.exists(queue.connection, removed.job_name))

    def test_import__should_schedule_job_update_existing(self):
        tasks = []
        tasks.append(task_factory(TaskType.ONCE, enabled=True))
        tasks.append(task_factory(TaskType.ONCE, enabled=True))
        res = json.dumps([j.to_dict() for j in tasks])
        self.tmpfile.write(res)
        self.tmpfile.flush()
        # act
        call_command("import", filename=self.tmpfile.name, update=True)
        # assert
        self.assertEqual(2, Task.objects.filter(task_type=TaskType.ONCE).count())
        task = Task.objects.filter(task_type=TaskType.ONCE).get(name=tasks[0].name)
        attrs = ["name", "queue", "callable", "enabled", "timeout"]
        for attr in attrs:
            self.assertEqual(getattr(tasks[0], attr), getattr(task, attr))

    def test_import__should_schedule_job_without_update_existing(self):
        tasks = []
        tasks.append(task_factory(TaskType.ONCE, enabled=True))
        tasks.append(task_factory(TaskType.ONCE, enabled=True))
        res = json.dumps([j.to_dict() for j in tasks])
        self.tmpfile.write(res)
        self.tmpfile.flush()
        # act
        call_command("import", filename=self.tmpfile.name)
        # assert
        self.assertEqual(2, Task.objects.filter(task_type=TaskType.ONCE).count())
        task = Task.objects.get(name=tasks[0].name)
        attrs = ["id", "name", "queue", "callable", "enabled", "timeout"]
        for attr in attrs:
            self.assertEqual(getattr(tasks[0], attr), getattr(task, attr))
