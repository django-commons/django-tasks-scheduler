import json
import sys
from io import StringIO

import yaml
from django.core.management import call_command
from django.test import TestCase, override_settings

from scheduler import settings
from scheduler.helpers.queues import get_queue


@override_settings(SCHEDULER_QUEUES=dict(default={"HOST": "localhost", "PORT": 6379, "DB": 0}))
class SchedulerStatsTest(TestCase):
    EXPECTED_OUTPUT = {
        "queues": [
            {
                "canceled_jobs": 0,
                "failed_jobs": 0,
                "finished_jobs": 0,
                "name": "default",
                "oldest_job_timestamp": None,
                "queued_jobs": 0,
                "scheduled_jobs": 0,
                "scheduler_pid": None,
                "started_jobs": 0,
                "workers": 0,
            }
        ]
    }
    OLD_QUEUES = None

    def setUp(self):
        super(SchedulerStatsTest, self).setUp()
        SchedulerStatsTest.OLD_QUEUES = settings._QUEUES
        settings._QUEUES = dict()
        settings.conf_settings()
        get_queue("default").connection.flushall()

    def tearDown(self):
        super(SchedulerStatsTest, self).tearDown()
        settings._QUEUES = SchedulerStatsTest.OLD_QUEUES

    def test_scheduler_stats__json_output(self):
        test_stdout = StringIO()
        sys.stdout = test_stdout
        # act
        call_command("scheduler_stats", "-j")
        # assert
        res = test_stdout.getvalue()
        self.assertEqual(json.loads(res), SchedulerStatsTest.EXPECTED_OUTPUT)

    def test_scheduler_stats__yaml_output(self):
        # arrange
        test_stdout = StringIO()
        sys.stdout = test_stdout
        # act
        call_command("scheduler_stats", "-y")
        # assert
        res = test_stdout.getvalue()
        self.assertEqual(yaml.load(res, yaml.SafeLoader), SchedulerStatsTest.EXPECTED_OUTPUT)

    def test_scheduler_stats__plain_text_output(self):
        test_stdout = StringIO()
        sys.stdout = test_stdout
        # act
        call_command("scheduler_stats", "--no-color")
        # assert
        res = test_stdout.getvalue()
        self.assertEqual(
            res,
            """
Django-Scheduler CLI Dashboard

--------------------------------------------------------------------------------
| Name             |    Queued |    Active |  Finished |  Canceled |   Workers |
--------------------------------------------------------------------------------
| default          |         0 |         0 |         0 |         0 |         0 |
--------------------------------------------------------------------------------
""",
        )

    def test_scheduler_stats__bad_args(self):
        # arrange
        sys.stderr = StringIO()
        sys.stdout = StringIO()
        # act
        with self.assertRaises(SystemExit):
            call_command("scheduler_stats", "-y", "-j")
        # assert
        res = sys.stdout.getvalue()
        self.assertEqual(res, """""")
        err = sys.stderr.getvalue()
        self.assertEqual(err, """Aborting. Cannot output as both json and yaml\n""")
