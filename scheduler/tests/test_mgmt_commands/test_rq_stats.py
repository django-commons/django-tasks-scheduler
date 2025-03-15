from django.core.management import call_command
from django.test import TestCase

from scheduler.tests import test_settings  # noqa


class RqstatsTest(TestCase):
    def test_rqstats__does_not_fail(self):
        call_command("rqstats", "-j")
        call_command("rqstats", "-y")
        call_command("rqstats")
