from django.core.management import call_command
from django.test import TestCase

from scheduler.tests import test_settings  # noqa


class SchedulerStatsTest(TestCase):
    def test_scheduler_stats__does_not_fail(self):
        call_command("scheduler_stats", "-j")
        call_command("scheduler_stats", "-y")
        call_command("scheduler_stats")
