from django.contrib.auth.models import User
from django.test import TestCase
from django.test.client import Client

from scheduler.helpers.queues import get_queue
from scheduler.tests import test_settings  # noqa


class BaseTestCase(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("user", password="pass")
        self.client = Client()
        self.client.login(username=self.user.username, password="pass")
        get_queue("django_tasks_scheduler_test").connection.flushall()
