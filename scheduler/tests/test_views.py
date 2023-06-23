import uuid
from datetime import datetime
from unittest.mock import patch, PropertyMock

from django.contrib.auth.models import User
from django.test import TestCase
from django.test.client import Client
from django.urls import reverse

from scheduler.queues import get_queue
from scheduler.tools import create_worker
from . import test_settings  # noqa
from .jobs import failing_job, long_job, test_job
from .testtools import assert_message_in_response, job_factory, _get_job_from_scheduled_registry
from ..models import ScheduledJob
from ..rq_classes import JobExecution, ExecutionStatus


class BaseTestCase(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser('user', password='pass')
        self.client = Client()
        self.client.login(username=self.user.username, password='pass')
        get_queue('django_rq_scheduler_test').connection.flushall()


class SingleJobActionViewsTest(BaseTestCase):

    def test_single_job_action_unknown_job(self):
        res = self.client.get(reverse('queue_job_action', args=['unknown', 'cancel']), follow=True)
        self.assertEqual(400, res.status_code)

    def test_single_job_action_unknown_action(self):
        queue = get_queue('default')
        job = queue.enqueue(failing_job)
        worker = create_worker('default')
        worker.work(burst=True)
        job.refresh()
        self.assertTrue(job.is_failed)
        res = self.client.get(reverse('queue_job_action', args=[job.id, 'unknown']), follow=True)
        self.assertEqual(404, res.status_code)

    def test_single_job_action_requeue_job(self):
        queue = get_queue('default')
        job = queue.enqueue(failing_job)
        worker = create_worker('default')
        worker.work(burst=True)
        job.refresh()
        self.assertTrue(job.is_failed)
        res = self.client.get(reverse('queue_job_action', args=[job.id, 'requeue']), follow=True)
        self.assertEqual(200, res.status_code)
        self.client.post(reverse('queue_job_action', args=[job.id, 'requeue']), {'requeue': 'Requeue'}, follow=True)
        self.assertIn(job, queue.jobs)
        job.delete()

    def test_single_job_action_delete_job(self):
        queue = get_queue('django_rq_scheduler_test')
        job = queue.enqueue(test_job)
        res = self.client.get(reverse('queue_job_action', args=[job.id, 'delete']), follow=True)
        self.assertEqual(200, res.status_code)
        self.client.post(reverse('queue_job_action', args=[job.id, 'delete']), {'post': 'yes'}, follow=True)
        self.assertFalse(JobExecution.exists(job.id, connection=queue.connection))
        self.assertNotIn(job.id, queue.get_job_ids())

    def test_single_job_action_cancel_job(self):
        queue = get_queue('django_rq_scheduler_test')
        job = queue.enqueue(long_job)
        res = self.client.get(reverse('queue_job_action', args=[job.id, 'cancel']), follow=True)
        self.assertEqual(200, res.status_code)
        res = self.client.post(reverse('queue_job_action', args=[job.id, 'cancel']), {'post': 'yes'}, follow=True)
        self.assertEqual(200, res.status_code)
        tmp = JobExecution.fetch(job.id, connection=queue.connection)
        self.assertTrue(tmp.is_canceled)
        self.assertNotIn(job.id, queue.get_job_ids())

    def test_single_job_action_cancel_job_that_is_already_cancelled(self):
        queue = get_queue('django_rq_scheduler_test')
        job = queue.enqueue(long_job)
        res = self.client.post(reverse('queue_job_action', args=[job.id, 'cancel']), {'post': 'yes'}, follow=True)
        self.assertEqual(200, res.status_code)
        tmp = JobExecution.fetch(job.id, connection=queue.connection)
        self.assertTrue(tmp.is_canceled)
        self.assertNotIn(job.id, queue.get_job_ids())
        res = self.client.post(reverse('queue_job_action', args=[job.id, 'cancel']), {'post': 'yes'}, follow=True)
        self.assertEqual(200, res.status_code)
        assert_message_in_response(res, f'Could not perform action: Cannot cancel already canceled job: {job.id}')

    def test_single_job_action_enqueue_job(self):
        queue = get_queue('django_rq_scheduler_test')
        job_list = []
        # enqueue some jobs that depends on other
        previous_job = None
        for _ in range(0, 3):
            job = queue.enqueue(test_job, depends_on=previous_job)
            job_list.append(job)
            previous_job = job

        # This job is deferred

        self.assertEqual(job_list[-1].get_status(), ExecutionStatus.DEFERRED)
        self.assertIsNone(job_list[-1].enqueued_at)

        # Try to force enqueue last job should do nothing
        res = self.client.get(reverse('queue_job_action', args=[job_list[-1].id, 'enqueue']), follow=True)
        self.assertEqual(200, res.status_code)
        res = self.client.post(reverse('queue_job_action', args=[job_list[-1].id, 'enqueue']), follow=True)

        # Check that job is still deferred because it has dependencies (rq 1.14 change)
        self.assertEqual(200, res.status_code)
        tmp = queue.fetch_job(job_list[-1].id)
        self.assertEqual(tmp.get_status(), ExecutionStatus.QUEUED)
        self.assertIsNotNone(tmp.enqueued_at)


class JobListActionViewsTest(BaseTestCase):
    def test_job_list_action_delete_jobs__with_bad_next_url(self):
        queue = get_queue('django_rq_scheduler_test')

        # enqueue some jobs
        job_ids = []
        for _ in range(0, 3):
            job = queue.enqueue(test_job)
            job_ids.append(job.id)

        # remove those jobs using view
        res = self.client.post(
            reverse('queue_actions', args=[queue.name, ]), {
                'action': 'delete', 'job_ids': job_ids,
                'next_url': 'bad_url',
            }, follow=True)
        assert_message_in_response(res, 'Bad followup URL')
        # check if jobs are removed
        self.assertEqual(200, res.status_code)
        for job_id in job_ids:
            self.assertFalse(JobExecution.exists(job_id, connection=queue.connection))
            self.assertNotIn(job_id, queue.job_ids)

    def test_job_list_action_delete_jobs(self):
        queue = get_queue('django_rq_scheduler_test')

        # enqueue some jobs
        job_ids = []
        for _ in range(0, 3):
            job = queue.enqueue(test_job)
            job_ids.append(job.id)

        # remove those jobs using view
        res = self.client.post(
            reverse('queue_actions', args=[queue.name, ]), {'action': 'delete', 'job_ids': job_ids}, follow=True)

        # check if jobs are removed
        self.assertEqual(200, res.status_code)
        for job_id in job_ids:
            self.assertFalse(JobExecution.exists(job_id, connection=queue.connection))
            self.assertNotIn(job_id, queue.job_ids)

    def test_job_list_action_requeue_jobs(self):
        queue = get_queue('django_rq_scheduler_test')
        queue_name = 'django_rq_scheduler_test'

        # enqueue some jobs that will fail
        jobs = []
        job_ids = []
        for _ in range(0, 3):
            job = queue.enqueue(failing_job)
            jobs.append(job)
            job_ids.append(job.id)

        # do those jobs = fail them
        worker = create_worker('django_rq_scheduler_test')
        worker.work(burst=True)

        # check if all jobs are really failed
        for job in jobs:
            self.assertTrue(job.is_failed)

        # re-nqueue failed jobs from failed queue
        self.client.post(reverse('queue_actions', args=[queue_name]), {'action': 'requeue', 'job_ids': job_ids})

        # check if we requeue all failed jobs
        for job in jobs:
            self.assertFalse(job.is_failed)

    def test_job_list_action_stop_jobs(self):
        queue_name = 'django_rq_scheduler_test'
        queue = get_queue(queue_name)

        # Enqueue some jobs
        job_ids = []
        worker = create_worker('django_rq_scheduler_test')
        for _ in range(3):
            job = queue.enqueue(test_job)
            job_ids.append(job.id)
            worker.prepare_job_execution(job)

        # Check if the jobs are started
        for job_id in job_ids:
            job = JobExecution.fetch(job_id, connection=queue.connection)
            self.assertEqual(job.get_status(), ExecutionStatus.STARTED)

        # Stop those jobs using the view
        started_job_registry = queue.started_job_registry
        self.assertEqual(len(started_job_registry), len(job_ids))
        self.client.post(reverse('queue_actions', args=[queue_name]), {'action': 'stop', 'job_ids': job_ids})
        self.assertEqual(len(started_job_registry), 0)

        canceled_job_registry = queue.canceled_job_registry
        self.assertEqual(len(canceled_job_registry), len(job_ids))

        for job_id in job_ids:
            self.assertIn(job_id, canceled_job_registry)


class QueueRegistryJobsViewTest(BaseTestCase):
    def test_queue_jobs_unknown_registry(self):
        queue_name = 'default'
        res = self.client.get(reverse('queue_registry_jobs', args=[queue_name, 'unknown']), follow=True)
        self.assertEqual(404, res.status_code)

    def test_queue_jobs_unknown_queue(self):
        res = self.client.get(reverse('queue_registry_jobs', args=['UNKNOWN', 'queued']))
        self.assertEqual(404, res.status_code)

    def test_queued_jobs(self):
        """Jobs in queue are displayed properly"""
        queue = get_queue('default')
        job = queue.enqueue(test_job)
        queue_name = 'default'
        res = self.client.get(reverse('queue_registry_jobs', args=[queue_name, 'queued']))
        self.assertEqual(res.context['jobs'], [job])

    def test_finished_jobs(self):
        """Ensure that finished jobs page works properly."""
        queue = get_queue('django_rq_scheduler_test')
        queue_name = 'django_rq_scheduler_test'

        job = queue.enqueue(test_job)
        registry = queue.finished_job_registry
        registry.add(job, 2)
        res = self.client.get(reverse('queue_registry_jobs', args=[queue_name, 'finished']))
        self.assertEqual(res.context['jobs'], [job])

    def test_failed_jobs(self):
        """Ensure that failed jobs page works properly."""
        queue = get_queue('django_rq_scheduler_test')
        queue_name = 'django_rq_scheduler_test'

        # Test that page doesn't fail when FailedJobRegistry is empty
        res = self.client.get(reverse('queue_registry_jobs', args=[queue_name, 'failed']))
        self.assertEqual(res.status_code, 200)

        job = queue.enqueue(test_job)
        registry = queue.failed_job_registry
        registry.add(job, 2)
        res = self.client.get(reverse('queue_registry_jobs', args=[queue_name, 'failed']))
        self.assertEqual(res.context['jobs'], [job])

    def test_scheduled_jobs(self):
        """Ensure that scheduled jobs page works properly."""
        queue = get_queue('django_rq_scheduler_test')
        queue_name = 'django_rq_scheduler_test'

        # Test that page doesn't fail when ScheduledJobRegistry is empty
        res = self.client.get(reverse('queue_registry_jobs', args=[queue_name, 'scheduled']))
        self.assertEqual(res.status_code, 200)

        job = queue.enqueue_at(datetime.now(), test_job)
        res = self.client.get(reverse('queue_registry_jobs', args=[queue_name, 'scheduled']))
        self.assertEqual(res.context['jobs'], [job])

    def test_scheduled_jobs_registry_removal(self):
        """Ensure that non-existing job is being deleted from registry by view"""
        queue = get_queue('django_rq_scheduler_test')
        queue_name = 'django_rq_scheduler_test'

        registry = queue.scheduled_job_registry
        job = queue.enqueue_at(datetime.now(), test_job)
        self.assertEqual(len(registry), 1)

        queue.connection.delete(job.key)
        res = self.client.get(reverse('queue_registry_jobs', args=[queue_name, 'scheduled']))
        self.assertEqual(res.context['jobs'], [])

        self.assertEqual(len(registry), 0)

    def test_started_jobs(self):
        """Ensure that active jobs page works properly."""
        queue = get_queue('django_rq_scheduler_test')
        queue_name = 'django_rq_scheduler_test'

        job = queue.enqueue(test_job)
        registry = queue.started_job_registry
        registry.add(job, 2)
        res = self.client.get(reverse('queue_registry_jobs', args=[queue_name, 'started']))
        self.assertEqual(res.context['jobs'], [job])

    def test_deferred_jobs(self):
        """Ensure that active jobs page works properly."""
        queue = get_queue('django_rq_scheduler_test')
        queue_name = 'django_rq_scheduler_test'

        job = queue.enqueue(test_job)
        registry = queue.deferred_job_registry
        registry.add(job, 2)
        res = self.client.get(reverse('queue_registry_jobs', args=[queue_name, 'deferred']))
        self.assertEqual(res.context['jobs'], [job])


class ViewTest(BaseTestCase):

    def test_job_details(self):
        """Job data is displayed properly"""
        queue = get_queue('default')
        job = queue.enqueue(test_job)

        url = reverse('job_details', args=[job.id, ])
        res = self.client.get(url)
        self.assertIn('job', res.context)
        self.assertEqual(res.context['job'], job)

        # This page shouldn't fail when job.data is corrupt
        queue.connection.hset(job.key, 'data', 'non-pickleable data')
        res = self.client.get(url)
        self.assertEqual(res.status_code, 200)
        self.assertIn('DeserializationError', res.content.decode())

        # Bad job-id should return 404
        url = reverse('job_details', args=['bad_job_id', ])
        res = self.client.get(url)
        self.assertEqual(400, res.status_code)

    def test_scheduled_job_details(self):
        """Job data is displayed properly"""
        scheduled_job = job_factory(ScheduledJob, enabled=True)
        job = _get_job_from_scheduled_registry(scheduled_job)

        url = reverse('job_details', args=[job.id, ])
        res = self.client.get(url, follow=True)
        self.assertIn('job', res.context)
        self.assertEqual(res.context['job'], job)

    def test_job_details_on_deleted_dependency(self):
        """Page doesn't crash even if job.dependency has been deleted"""
        queue = get_queue('default')

        job = queue.enqueue(test_job)
        second_job = queue.enqueue(test_job, depends_on=job)
        job.delete()
        url = reverse('job_details', args=[second_job.id])
        res = self.client.get(url)
        self.assertEqual(res.status_code, 200)
        self.assertIn(second_job._dependency_id, res.content.decode())

    def test_requeue_all(self):
        """
        Ensure that re-queuing all failed job work properly
        """
        queue = get_queue('default')
        queue_name = 'default'
        queue.enqueue(failing_job)
        queue.enqueue(failing_job)
        worker = create_worker('default')
        worker.work(burst=True)

        res = self.client.get(reverse('queue_requeue_all', args=[queue_name, 'failed']))
        self.assertEqual(res.context['total_jobs'], 2)
        # After requeue_all is called, jobs are enqueued
        res = self.client.post(reverse('queue_requeue_all', args=[queue_name, 'failed']))
        self.assertEqual(len(queue), 2)

    def test_requeue_all_if_deleted_job(self):
        """
        Ensure that re-queuing all failed job work properly
        """
        queue = get_queue('default')
        queue_name = 'default'
        job = queue.enqueue(failing_job)
        queue.enqueue(failing_job)
        worker = create_worker('default')
        worker.work(burst=True)

        res = self.client.get(reverse('queue_requeue_all', args=[queue_name, 'failed']))
        self.assertEqual(res.context['total_jobs'], 2)
        job.delete()

        # After requeue_all is called, jobs are enqueued
        res = self.client.post(reverse('queue_requeue_all', args=[queue_name, 'failed']))
        self.assertEqual(len(queue), 1)

    def test_clear_queue_unknown_registry(self):
        queue_name = 'django_rq_scheduler_test'
        res = self.client.post(reverse('queue_clear', args=[queue_name, 'unknown']), {'post': 'yes'})
        self.assertEqual(404, res.status_code)

    def test_clear_queue_enqueued(self):
        queue = get_queue('django_rq_scheduler_test')
        job = queue.enqueue(test_job)
        self.client.post(reverse('queue_clear', args=[queue.name, 'queued']), {'post': 'yes'})
        self.assertFalse(JobExecution.exists(job.id, connection=queue.connection))
        self.assertNotIn(job.id, queue.job_ids)

    def test_clear_queue_scheduled(self):
        queue = get_queue('django_rq_scheduler_test')
        job = queue.enqueue_at(datetime.now(), test_job)

        res = self.client.get(reverse('queue_clear', args=[queue.name, 'scheduled']), follow=True)
        self.assertEqual(200, res.status_code)
        self.assertEqual(res.context['jobs'], [job, ])

        res = self.client.post(reverse('queue_clear', args=[queue.name, 'scheduled']), {'post': 'yes'}, follow=True)
        assert_message_in_response(res, f'You have successfully cleared the scheduled jobs in queue {queue.name}')
        self.assertEqual(200, res.status_code)
        self.assertFalse(JobExecution.exists(job.id, connection=queue.connection))
        self.assertNotIn(job.id, queue.job_ids)

    def test_workers_home(self):
        res = self.client.get(reverse('workers_home'))
        prev_workers = res.context['workers']
        worker1 = create_worker('django_rq_scheduler_test')
        worker1.register_birth()
        worker2 = create_worker('test3')
        worker2.register_birth()

        res = self.client.get(reverse('workers_home'))
        self.assertEqual(res.context['workers'], prev_workers + [worker1, worker2])

    def test_queue_workers(self):
        """Worker index page should show workers for a specific queue"""
        queue_name = 'django_rq_scheduler_test'

        worker1 = create_worker('django_rq_scheduler_test')
        worker1.register_birth()
        worker2 = create_worker('test3')
        worker2.register_birth()

        res = self.client.get(reverse('queue_workers', args=[queue_name]))
        self.assertEqual(res.context['workers'], [worker1])

    def test_worker_details(self):
        """Worker index page should show workers for a specific queue"""

        worker = create_worker('django_rq_scheduler_test', name=uuid.uuid4().hex)
        worker.register_birth()

        url = reverse('worker_details', args=[worker.name, ])
        res = self.client.get(url)
        self.assertEqual(res.context['worker'], worker)

    def test_worker_details__non_existing_worker(self):
        """Worker index page should show workers for a specific queue"""

        worker = create_worker('django_rq_scheduler_test', name='WORKER')
        worker.register_birth()

        res = self.client.get(reverse('worker_details', args=['bad-worker-name']))
        self.assertEqual(404, res.status_code)

    def test_statistics_json_view(self):
        # Override testing SCHEDULER_QUEUES
        queues = {
            'default': {
                'DB': 0,
                'HOST': 'localhost',
                'PORT': 6379,
            }
        }
        with patch('scheduler.settings.QUEUES', new_callable=PropertyMock(return_value=queues)):
            res = self.client.get(reverse('queues_home'))
            self.assertEqual(res.status_code, 200)

            res = self.client.get(reverse('queues_home_json'))
            self.assertEqual(res.status_code, 200)

            # Not staff, only token
            self.user.is_staff = False
            self.user.save()

            res = self.client.get(reverse('queues_home'))
            self.assertEqual(res.status_code, 302)

            # 404 code for stats
            res = self.client.get(reverse('queues_home_json'))
            self.assertEqual(res.status_code, 404)
