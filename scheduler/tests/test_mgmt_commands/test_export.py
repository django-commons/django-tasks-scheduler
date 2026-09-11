import json
import os
import tempfile
from unittest import mock

import yaml
from django.core.management import call_command
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

from scheduler.models import TaskType
from scheduler.tests import conf  # noqa
from scheduler.tests.testtools import task_factory


class ExportTest(TestCase):
    def setUp(self) -> None:
        super().setUp()
        # tearDown removes the file itself. With delete=True (The default behaviour), __del__ unlinks it a
        # second time and python<3.12 dumps that FileNotFoundError to stderr.
        # https://github.com/python/cpython/blob/3.11/Lib/tempfile.py#L463
        self.tmpfile = tempfile.NamedTemporaryFile(delete=False)

    def tearDown(self) -> None:
        super().tearDown()
        os.remove(self.tmpfile.name)

    def test_export__should_export_job(self):
        tasks = []
        tasks.append(task_factory(TaskType.ONCE, enabled=True))
        tasks.append(task_factory(TaskType.REPEATABLE, enabled=True))

        # act
        call_command("export", filename=self.tmpfile.name)
        # assert
        result = json.load(self.tmpfile)
        self.assertEqual(len(tasks), len(result))
        self.assertEqual(result[0], tasks[0].to_dict())
        self.assertEqual(result[1], tasks[1].to_dict())

    def test_export__query_count_does_not_grow_with_tasks(self):
        task_factory(TaskType.ONCE, enabled=True)
        with CaptureQueriesContext(connection) as one_task:
            call_command("export", filename=self.tmpfile.name)
        for _ in range(3):
            task_factory(TaskType.ONCE, enabled=True)

        with CaptureQueriesContext(connection) as four_tasks:
            call_command("export", filename=self.tmpfile.name)

        self.assertEqual(len(one_task), len(four_tasks))

    def test_export__should_export_enabled_jobs_only(self):
        tasks = []
        tasks.append(task_factory(TaskType.ONCE, enabled=True))
        tasks.append(task_factory(TaskType.REPEATABLE, enabled=False))

        # act
        call_command("export", filename=self.tmpfile.name, enabled=True)
        # assert
        result = json.load(self.tmpfile)
        self.assertEqual(len(tasks) - 1, len(result))
        self.assertEqual(result[0], tasks[0].to_dict())

    def test_export__should_export_job_yaml_without_yaml_lib(self):
        tasks = []
        tasks.append(task_factory(TaskType.ONCE, enabled=True))
        tasks.append(task_factory(TaskType.REPEATABLE, enabled=True))

        # act
        with mock.patch.dict("sys.modules", {"yaml": None}):
            with self.assertRaises(SystemExit) as cm:
                call_command("export", filename=self.tmpfile.name, format="yaml")
            self.assertEqual(cm.exception.code, 1)

    def test_export__should_export_job_yaml_green(self):
        tasks = []
        tasks.append(task_factory(TaskType.ONCE, enabled=True))
        tasks.append(task_factory(TaskType.REPEATABLE, enabled=True))
        tasks.append(task_factory(TaskType.CRON, enabled=True))

        # act
        call_command("export", filename=self.tmpfile.name, format="yaml")
        # assert
        result = yaml.load(self.tmpfile, yaml.SafeLoader)
        self.assertEqual(len(tasks), len(result))
        self.assertEqual(result[0], tasks[0].to_dict())
        self.assertEqual(result[1], tasks[1].to_dict())
        self.assertEqual(result[2], tasks[2].to_dict())
