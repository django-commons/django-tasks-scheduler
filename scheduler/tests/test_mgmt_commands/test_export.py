import json
import os
import tempfile
from unittest import mock

import yaml
from django.core.management import call_command
from django.test import TestCase

from scheduler.tests import test_settings  # noqa
from scheduler.tests.testtools import task_factory
from scheduler.tools import TaskType


class ExportTest(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.tmpfile = tempfile.NamedTemporaryFile()

    def tearDown(self) -> None:
        super().tearDown()
        os.remove(self.tmpfile.name)

    def test_export__should_export_job(self):
        jobs = list()
        jobs.append(task_factory(TaskType.ONCE, enabled=True))
        jobs.append(task_factory(TaskType.REPEATABLE, enabled=True))

        # act
        call_command("export", filename=self.tmpfile.name)
        # assert
        result = json.load(self.tmpfile)
        self.assertEqual(len(jobs), len(result))
        self.assertEqual(result[0], jobs[0].to_dict())
        self.assertEqual(result[1], jobs[1].to_dict())

    def test_export__should_export_enabled_jobs_only(self):
        jobs = list()
        jobs.append(task_factory(TaskType.ONCE, enabled=True))
        jobs.append(task_factory(TaskType.REPEATABLE, enabled=False))

        # act
        call_command("export", filename=self.tmpfile.name, enabled=True)
        # assert
        result = json.load(self.tmpfile)
        self.assertEqual(len(jobs) - 1, len(result))
        self.assertEqual(result[0], jobs[0].to_dict())

    def test_export__should_export_job_yaml_without_yaml_lib(self):
        jobs = list()
        jobs.append(task_factory(TaskType.ONCE, enabled=True))
        jobs.append(task_factory(TaskType.REPEATABLE, enabled=True))

        # act
        with mock.patch.dict("sys.modules", {"yaml": None}):
            with self.assertRaises(SystemExit) as cm:
                call_command("export", filename=self.tmpfile.name, format="yaml")
            self.assertEqual(cm.exception.code, 1)

    def test_export__should_export_job_yaml_green(self):
        jobs = list()
        jobs.append(task_factory(TaskType.ONCE, enabled=True))
        jobs.append(task_factory(TaskType.REPEATABLE, enabled=True))
        jobs.append(task_factory(TaskType.CRON, enabled=True))

        # act
        call_command("export", filename=self.tmpfile.name, format="yaml")
        # assert
        result = yaml.load(self.tmpfile, yaml.SafeLoader)
        self.assertEqual(len(jobs), len(result))
        self.assertEqual(result[0], jobs[0].to_dict())
        self.assertEqual(result[1], jobs[1].to_dict())
        self.assertEqual(result[2], jobs[2].to_dict())
