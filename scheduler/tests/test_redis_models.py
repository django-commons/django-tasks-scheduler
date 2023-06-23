from django.urls import reverse

from scheduler.tests.testtools import SchedulerBaseCase


class TestWorkerAdmin(SchedulerBaseCase):
    def test_admin_list_view(self):
        # arrange
        self.client.login(username='admin', password='admin')
        model = 'worker'
        url = reverse(f'admin:scheduler_{model}_changelist')

        # act
        res = self.client.get(url)
        # assert
        self.assertEqual(200, res.status_code)


class TestQueueAdmin(SchedulerBaseCase):
    def test_admin_list_view(self):
        # arrange
        self.client.login(username='admin', password='admin')
        model = 'queue'
        url = reverse(f'admin:scheduler_{model}_changelist')

        # act
        res = self.client.get(url)
        # assert
        self.assertEqual(200, res.status_code)
