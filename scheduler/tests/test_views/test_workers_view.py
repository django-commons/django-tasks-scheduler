from django.urls import reverse

from scheduler.helpers.tools import create_worker
from scheduler.tests import test_settings  # noqa
from scheduler.tests.test_views.base import BaseTestCase


class TestViewWorkers(BaseTestCase):

    def test_workers_home(self):
        res = self.client.get(reverse("workers_home"))
        prev_workers = res.context["workers"]
        worker1 = create_worker("django_tasks_scheduler_test")
        worker1.register_birth()
        worker2 = create_worker("test3")
        worker2.register_birth()

        res = self.client.get(reverse("workers_home"))
        self.assertEqual(res.context["workers"], prev_workers + [worker1._model, worker2._model])
