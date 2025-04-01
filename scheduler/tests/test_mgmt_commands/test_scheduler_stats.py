from django.core.management import call_command
from django.test import TestCase, override_settings

from scheduler.tests import test_settings  # noqa
from scheduler.types import SchedulerConfiguration


class SchedulerStatsTest(TestCase):
    @override_settings(SCHEDULER_CONFIG=SchedulerConfiguration(SCHEDULER_INTERVAL=1))
    def test_scheduler_stats__does_not_fail(self):
        call_command("scheduler_stats", "-j")
        call_command("scheduler_stats", "-y")
        call_command("scheduler_stats")
