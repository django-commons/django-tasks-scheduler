from django.urls import reverse

from scheduler.worker import create_worker
from scheduler.tests import test_settings  # noqa
from scheduler.tests.test_views.base import BaseTestCase


class TestViewWorkers(BaseTestCase):
    def test_workers_home(self):
        res = self.client.get(reverse("workers_home"))
        prev_workers = res.context["workers"]
        worker1 = create_worker("django_tasks_scheduler_test")
        worker1.worker_start()
        worker2 = create_worker("test3")
        worker2.worker_start()

        res = self.client.get(reverse("workers_home"))
        self.assertEqual(res.context["workers"], prev_workers + [worker1._model, worker2._model])
