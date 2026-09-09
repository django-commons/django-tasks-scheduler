"""Cron ownership regressions using real Task rows and broker registries."""

from datetime import timedelta
from unittest.mock import patch

from django.contrib import admin
from django.core import mail
from django.test import RequestFactory
from django.utils import timezone

from scheduler.admin.task_admin import TaskAdmin
from scheduler.models import Task, TaskType
from scheduler.models.task import failure_callback, run_task, success_callback
from scheduler.redis_models import JobModel
from scheduler.tests import conf  # noqa: F401
from scheduler.tests.testtools import SchedulerBaseCase, task_factory
from scheduler.worker.scheduler import _reschedule_tasks


class TestCronOwnership(SchedulerBaseCase):
    def setUp(self):
        super().setUp()
        self.task = task_factory(TaskType.CRON, cron_string="* * * * *")
        self.queue = self.task.rqueue

    def job(self, name):
        job = JobModel.get(name, connection=self.queue.connection)
        self.assertIsNotNone(job)
        return job

    def scheduled(self):
        return self.queue.scheduled_job_registry.all(self.queue.connection)

    def execute(self, job):
        self.queue.scheduled_job_registry.delete(self.queue.connection, job.name)
        self.queue.queued_job_registry.delete(self.queue.connection, job.name)
        self.queue.run_sync(job)

    def duplicate(self):
        return self.queue.create_and_enqueue_job(
            run_task,
            args=(self.task.task_type, self.task.pk),
            when=self.task.scheduled_time,
            **self.task._enqueue_args(),
        )

    def manual_job(self):
        before = set(self.queue.queued_job_registry.all(self.queue.connection))
        self.task.enqueue_to_run()
        names = set(self.queue.queued_job_registry.all(self.queue.connection)) - before
        self.assertEqual(len(names), 1)
        return self.job(names.pop())

    def test_manual_outcomes_preserve_the_recurring_owner(self):
        original = self.task.job_name
        for failed in (False, True):
            with self.subTest(failed=failed):
                job = self.manual_job()
                if failed:
                    with patch.object(Task, "callable_func", side_effect=ValueError("manual failure")):
                        self.execute(job)
                else:
                    self.execute(job)
                self.task.refresh_from_db()
                self.assertEqual(self.scheduled(), [original])
                self.assertEqual(self.task.job_name, original)
        self.assertEqual(self.task.successful_runs, 1)
        self.assertEqual(self.task.failed_runs, 1)

    def test_owner_completion_advances_once_with_callback_still_active(self):
        for failed in (False, True):
            with self.subTest(failed=failed):
                original = self.task.job_name
                job = self.job(original)
                if failed:
                    with patch.object(Task, "callable_func", side_effect=ValueError("scheduled failure")):
                        self.execute(job)
                else:
                    self.execute(job)
                self.task.refresh_from_db()
                self.assertNotEqual(self.task.job_name, original)
                self.assertEqual(self.scheduled(), [self.task.job_name])
        self.assertEqual(self.task.successful_runs, 1)
        self.assertEqual(self.task.failed_runs, 1)

    def test_stale_save_preserves_successor_and_run_count(self):
        self.execute(self.job(self.task.job_name))
        owner = Task.objects.get(pk=self.task.pk).job_name
        self.task.save(clean=False)
        self.task.refresh_from_db()
        self.assertEqual(self.task.job_name, owner)
        self.assertEqual(self.task.successful_runs, 1)
        self.assertEqual(self.scheduled(), [owner])

    def test_missing_database_pointer_adopts_existing_job(self):
        original = self.task.job_name
        Task.objects.filter(pk=self.task.pk).update(job_name=None)
        _reschedule_tasks()
        self.task.refresh_from_db()
        self.assertEqual(self.task.job_name, original)
        self.assertEqual(self.scheduled(), [original])

    def test_missing_job_record_is_replaced(self):
        original = self.task.job_name
        self.job(original).delete(self.queue.connection)
        self.task.save(clean=False)
        self.assertNotEqual(self.task.job_name, original)
        self.assertEqual(self.scheduled(), [self.task.job_name])

    def test_dequeue_handoff_does_not_create_a_second_chain(self):
        job = self.job(self.task.job_name)
        self.queue.enqueue_job(job)
        self.queue.queued_job_registry.delete(self.queue.connection, job.name)
        self.task.save(clean=False)
        self.assertEqual(self.task.job_name, job.name)
        self.assertEqual(self.scheduled(), [])

    def test_lost_dequeue_handoff_eventually_recovers(self):
        job = self.job(self.task.job_name)
        self.queue.enqueue_job(job)
        self.queue.queued_job_registry.delete(self.queue.connection, job.name)
        self.task.save(clean=False)
        with patch("django.utils.timezone.now", return_value=timezone.now() + timedelta(seconds=job.timeout + 61)):
            self.task.save(clean=False)
        self.assertNotEqual(self.task.job_name, job.name)
        self.assertEqual(self.scheduled(), [self.task.job_name])

    def test_admin_display_cannot_overwrite_new_state(self):
        self.execute(self.job(self.task.job_name))
        owner = Task.objects.get(pk=self.task.pk).job_name
        self.task.is_scheduled()
        current = Task.objects.get(pk=self.task.pk)
        self.assertEqual(current.job_name, owner)
        self.assertEqual(current.successful_runs, 1)

    def test_reconcile_removes_waiting_duplicates_but_preserves_manual_job(self):
        original = self.task.job_name
        self.duplicate()
        self.queue.enqueue_job(self.duplicate())
        manual = self.manual_job()
        _reschedule_tasks()
        self.assertEqual(self.scheduled(), [original])
        self.assertEqual(self.queue.queued_job_registry.all(self.queue.connection), [manual.name])

    def test_dequeued_duplicate_does_not_execute_callable_or_count_success(self):
        duplicate = self.duplicate()
        effects = []
        with patch.object(Task, "callable_func", return_value=lambda: effects.append("executed")):
            self.execute(duplicate)
        self.task.refresh_from_db()
        self.assertEqual(effects, [])
        self.assertEqual(self.task.successful_runs, 0)
        self.assertEqual(self.scheduled(), [self.task.job_name])

    def test_disabling_stale_task_removes_copies_and_preserves_new_configuration(self):
        self.duplicate()
        Task.objects.filter(pk=self.task.pk).update(cron_string="*/5 * * * *")
        self.task.unschedule(enabled=False)
        self.task.refresh_from_db()
        self.assertFalse(self.task.enabled)
        self.assertEqual(self.task.cron_string, "*/5 * * * *")
        self.assertIsNone(self.task.job_name)
        self.assertEqual(self.scheduled(), [])

    def test_unschedule_keeps_the_stored_enabled_flag(self):
        """Dequeuing must not let a stale instance rewrite ``enabled`` as a side effect."""
        self.task.enabled = False
        self.task.unschedule()
        self.task.refresh_from_db()
        self.assertTrue(self.task.enabled)
        self.assertIsNone(self.task.job_name)
        self.assertEqual(self.scheduled(), [])

    def test_admin_display_does_not_scan_the_registries(self):
        """The changelist column runs per row, so it checks ``job_name`` rather than reading
        every registry the way reconciliation does."""
        from scheduler.models import cron

        with patch.object(cron, "read_schedule", side_effect=AssertionError("registry sweep")):
            self.assertTrue(self.task.is_scheduled())
            self.queue.delete_job(self.task.job_name)
            self.assertFalse(self.task.is_scheduled())

    def test_unreadable_broker_reports_an_unknown_schedule(self):
        """A broker outage must not show as a scheduled checkmark in the admin."""
        from redis.exceptions import ConnectionError as RedisConnectionError

        with patch.object(type(self.queue.connection), "pipeline", side_effect=RedisConnectionError("offline")):
            self.assertIsNone(self.task.is_scheduled())

    def test_deleted_task_cannot_be_resurrected_by_stale_save(self):
        Task.objects.get(pk=self.task.pk).delete()
        with self.assertRaises(Task.DoesNotExist):
            self.task.save(clean=False)
        self.assertFalse(Task.objects.filter(pk=self.task.pk).exists())

    def test_jobs_from_old_database_generation_do_not_execute(self):
        jobs = [self.job(self.task.job_name), self.manual_job()]
        Task.objects.filter(pk=self.task.pk).update(created_at=timezone.now() + timedelta(seconds=1))
        effects = []
        with patch.object(Task, "callable_func", return_value=lambda: effects.append("executed")):
            for job in jobs:
                self.execute(job)
        self.task.refresh_from_db()
        self.assertEqual(effects, [])
        self.assertEqual(self.task.successful_runs, 0)
        self.assertEqual(self.task.failed_runs, 0)

    def test_manual_job_survives_queue_change_without_advancing_new_owner(self):
        job = self.manual_job()
        self.task.queue = "test3"
        self.task.save(clean=False)
        owner, when = self.task.job_name, self.task.scheduled_time
        effects = []
        with patch.object(Task, "callable_func", return_value=lambda: effects.append("executed")):
            self.execute(job)
        self.task.refresh_from_db()
        self.assertEqual(effects, ["executed"])
        self.assertEqual(self.task.successful_runs, 1)
        self.assertEqual(self.task.job_name, owner)
        self.assertEqual(self.task.scheduled_time, when)
        self.assertEqual(self.scheduled(), [])

    def test_queue_change_retires_old_recurring_payload(self):
        job = self.job(self.task.job_name)
        self.task.queue = "test3"
        self.task.save(clean=False)
        owner = self.task.job_name
        effects = []
        with patch.object(Task, "callable_func", return_value=lambda: effects.append("executed")):
            self.execute(job)
        self.task.refresh_from_db()
        self.assertNotEqual(owner, job.name)
        self.assertEqual(effects, [])
        self.assertEqual(self.task.job_name, owner)
        self.assertEqual(self.scheduled(), [])

    def test_partial_save_ignores_unsaved_type_change(self):
        owner = self.task.job_name
        self.task.task_type = TaskType.ONCE
        self.task.name = "renamed"
        self.task.save(update_fields=["name"], clean=False)
        self.task.refresh_from_db()
        self.assertEqual(self.task.task_type, TaskType.CRON)
        self.assertEqual(self.task.name, "renamed")
        self.assertEqual(self.task.job_name, owner)
        self.assertEqual(self.scheduled(), [owner])

    def test_partial_stale_save_preserves_current_once_job(self):
        current = Task.objects.get(pk=self.task.pk)
        current.task_type = TaskType.ONCE
        current.save(clean=False)
        self.task.name = "renamed-once"
        self.task.save(update_fields=["name"], clean=False)
        self.task.refresh_from_db()
        self.assertEqual(self.task.task_type, TaskType.ONCE)
        self.assertEqual(self.task.job_name, current.job_name)
        self.assertEqual(self.scheduled(), [current.job_name])

    def test_completion_after_disable_does_not_reenable(self):
        job = self.job(self.task.job_name)
        self.task.unschedule(enabled=False)
        success_callback(job, self.queue.connection, None)
        self.task.refresh_from_db()
        self.assertFalse(self.task.enabled)
        self.assertIsNone(self.task.job_name)
        self.assertEqual(self.scheduled(), [])

    def test_running_cron_keeps_outcome_after_conversion_to_once(self):
        job = self.job(self.task.job_name)
        self.queue.scheduled_job_registry.delete(self.queue.connection, job.name)
        job.prepare_for_execution("transition", self.queue.active_job_registry, self.queue.connection)
        self.task.task_type = TaskType.ONCE
        self.task.save(clean=False)
        owner, when = self.task.job_name, self.task.scheduled_time
        with self.settings(ADMINS=[("Admin", "admin@example.com")]):
            failure_callback(job, self.queue.connection, None)
        self.task.refresh_from_db()
        self.assertEqual(self.task.failed_runs, 1)
        self.assertIsNotNone(self.task.last_failed_run)
        self.assertEqual(len(mail.outbox), 1)
        self.assertEqual(self.task.job_name, owner)
        self.assertEqual(self.task.scheduled_time, when)

    def test_old_duplicate_and_owner_cannot_multiply_successors(self):
        for owner_first in (False, True):
            with self.subTest(owner_first=owner_first):
                owner, duplicate = self.job(self.task.job_name), self.duplicate()
                for job in [owner, duplicate] if owner_first else [duplicate, owner]:
                    self.execute(job)
                self.task.refresh_from_db()
                self.assertEqual(self.scheduled(), [self.task.job_name])
        self.assertEqual(self.task.successful_runs, 2)

    def test_broker_read_failure_neither_runs_nor_creates_another_job(self):
        from redis.exceptions import ConnectionError as RedisConnectionError

        owner = self.job(self.task.job_name)
        effects = []
        with (
            patch.object(type(self.queue.connection), "zscan_iter", side_effect=RedisConnectionError("offline")),
            patch.object(Task, "callable_func", return_value=lambda: effects.append("executed")),
        ):
            self.task.save(clean=False)
            self.execute(owner)
        self.assertEqual(effects, [])
        self.assertEqual(Task.objects.get(pk=self.task.pk).job_name, owner.name)

    def test_save_reports_that_an_unreachable_broker_left_the_schedule_stale(self):
        """A cron save that could not reach the broker must not report success silently."""
        from redis.exceptions import ConnectionError as RedisConnectionError

        with patch.object(type(self.queue.connection), "zscan_iter", side_effect=RedisConnectionError("offline")):
            self.task.save(clean=False)
        self.assertFalse(self.task.schedule_updated)
        self.task.save(clean=False)
        self.assertTrue(self.task.schedule_updated)

    def test_ambiguous_enqueue_is_adopted_on_next_tick(self):
        from redis.exceptions import ConnectionError as RedisConnectionError

        from scheduler.helpers.queues import Queue

        self.queue.delete_job(self.task.job_name)
        original = Queue.create_and_enqueue_job

        def enqueue(queue, *args, **kwargs):
            original(queue, *args, **kwargs)
            raise RedisConnectionError("reply lost after enqueue")

        with patch.object(Queue, "create_and_enqueue_job", enqueue):
            self.task.save(clean=False)
        (owner,) = self.scheduled()
        self.task.save(clean=False)
        self.assertEqual(self.task.job_name, owner)
        self.assertEqual(self.scheduled(), [owner])

    def test_scheduler_rechecks_enabled_after_enumerating_tasks(self):
        """A task disabled while the sweep is running is skipped, not rescheduled from a stale id."""
        from scheduler.worker import scheduler

        other = task_factory(TaskType.CRON)
        owner = other.job_name
        original = Task.reschedule_if_needed

        def reschedule_if_needed(task):
            if task.pk == self.task.pk:
                Task.objects.filter(pk=other.pk).update(enabled=False)
            return original(task)

        with patch.object(Task, "reschedule_if_needed", reschedule_if_needed):
            scheduler._reschedule_tasks()
        other.refresh_from_db()
        self.assertFalse(other.enabled)
        self.assertEqual(other.job_name, owner)

    def test_manual_payload_cannot_run_after_deletion_or_type_change(self):
        manual = self.manual_job()
        self.task.task_type = TaskType.ONCE
        self.task.save(clean=False)
        effects = []
        with patch.object(Task, "callable_func", return_value=lambda: effects.append("executed")):
            self.execute(manual)
            self.task.delete()
            self.execute(manual)
        self.assertEqual(effects, [])

    def test_running_once_job_keeps_outcome_after_conversion_to_cron(self):
        self.task.task_type = TaskType.ONCE
        self.task.save(clean=False)
        job = self.job(self.task.job_name)
        self.queue.scheduled_job_registry.delete(self.queue.connection, job.name)
        job.prepare_for_execution("transition", self.queue.active_job_registry, self.queue.connection)
        self.task.task_type = TaskType.CRON
        self.task.save(clean=False)
        owner = self.task.job_name
        success_callback(job, self.queue.connection, None)
        self.task.refresh_from_db()
        self.assertEqual(self.task.successful_runs, 1)
        self.assertEqual(self.task.job_name, owner)
        self.assertEqual(self.scheduled(), [owner])

    def test_reconcile_command_dry_run_preserves_duplicates_until_apply(self):
        from io import StringIO

        from django.core.management import call_command

        duplicate = self.duplicate()
        output = StringIO()
        call_command("reconcile_scheduler", stdout=output)
        self.assertIn("2 live recurring jobs", output.getvalue())
        self.assertIn(duplicate.name, self.scheduled())
        call_command("reconcile_scheduler", apply=True, stdout=StringIO())
        self.assertEqual(self.scheduled(), [self.task.job_name])

    def test_repeatable_recovers_when_worker_dies_after_dequeue(self):
        task = task_factory(TaskType.REPEATABLE)
        job = self.job(task.job_name)
        self.queue.enqueue_job(job)
        self.queue.queued_job_registry.delete(self.queue.connection, job.name)
        with patch("django.utils.timezone.now", return_value=timezone.now() + timedelta(days=2)):
            task.save(clean=False)
        self.assertNotEqual(task.job_name, job.name)
        self.assertIn(task.job_name, self.scheduled())

    def test_slow_scheduler_still_recovers_a_lost_handoff(self):
        from unittest.mock import PropertyMock

        import time_machine
        from fakeredis import FakeRedis, FakeServer

        from scheduler.helpers.queues import Queue

        # Advance the broker clock too, making premature marker expiry observable.
        queue = Queue(FakeRedis(server=FakeServer()), "default")
        with patch.object(Task, "rqueue", new_callable=PropertyMock, return_value=queue):
            task = task_factory(TaskType.CRON, timeout=1)
            job = JobModel.get(task.job_name, connection=queue.connection)
            queue.enqueue_job(job)
            queue.queued_job_registry.delete(queue.connection, job.name)
            task.save(clean=False)
            with time_machine.travel(timezone.now() + timedelta(seconds=300), tick=False):
                task.save(clean=False)
        self.assertNotEqual(task.job_name, job.name)

    def test_replacing_or_clearing_an_orphan_owner_releases_its_marker(self):
        from scheduler.redis_models.job import MISSING_REGISTRY_KEY_PREFIX

        for change in ("queue", "disabled", "missing_hash"):
            with self.subTest(change=change):
                task = task_factory(TaskType.CRON)
                job = self.job(task.job_name)
                self.queue.enqueue_job(job)
                self.queue.queued_job_registry.delete(self.queue.connection, job.name)
                task.save(clean=False)
                key = f"{MISSING_REGISTRY_KEY_PREFIX}{job.name}"
                self.assertIsNotNone(self.queue.connection.get(key))
                if change == "queue":
                    task.queue = "test3"
                elif change == "disabled":
                    task.enabled = False
                else:
                    job.delete(self.queue.connection)
                task.save(clean=False)
                self.assertIsNone(self.queue.connection.get(key))


class DefaultWriteRouter:
    def db_for_write(self, model, **hints):
        return "default"


class SplitReadWriteRouter:
    def db_for_read(self, model, **hints):
        return "other"

    def db_for_write(self, model, **hints):
        return "default"


class TestCronDatabaseIsolation(SchedulerBaseCase):
    databases = {"default", "other"}

    def test_admin_bulk_delete_uses_write_database(self):
        task = task_factory(TaskType.CRON)
        queue = task.rqueue
        task_admin = TaskAdmin(Task, admin.site)
        request = RequestFactory().post("/admin/scheduler/task/")

        with self.settings(DATABASE_ROUTERS=[SplitReadWriteRouter()]):
            task_admin.delete_queryset(request, Task.objects.filter(pk=task.pk))

        self.assertFalse(Task.objects.using("default").filter(pk=task.pk).exists())
        self.assertEqual(queue.scheduled_job_registry.all(queue.connection), [])

    def test_same_task_id_in_another_database_keeps_both_owners(self):
        first = task_factory(TaskType.CRON)
        second = task_factory(TaskType.CRON, instance_only=True, id=first.pk)
        second.save(using="other")
        queue = first.rqueue
        expected = {first.job_name, second.job_name}
        self.assertEqual(set(queue.scheduled_job_registry.all(queue.connection)), expected)
        first.save(clean=False)
        second.save(clean=False)
        self.assertEqual(set(queue.scheduled_job_registry.all(queue.connection)), expected)
        second.enqueue_to_run()
        (name,) = queue.queued_job_registry.all(queue.connection)
        job = JobModel.get(name, connection=queue.connection)
        queue.queued_job_registry.delete(queue.connection, name)
        queue.run_sync(job)
        first.refresh_from_db()
        second.refresh_from_db()
        self.assertEqual(first.successful_runs, 0)
        self.assertEqual(second.successful_runs, 1)
        second.delete(using="other")
        self.assertEqual(queue.scheduled_job_registry.all(queue.connection), [first.job_name])

    def test_explicit_database_is_preserved_when_router_prefers_default(self):
        with self.settings(DATABASE_ROUTERS=[DefaultWriteRouter()]):
            task = task_factory(TaskType.CRON, instance_only=True, id=20000)
            task.save(using="other")
            task.refresh_from_db(using="other")
            owner = task.job_name
            queue = task.rqueue
            job = JobModel.get(owner, connection=queue.connection)
            queue.scheduled_job_registry.delete(queue.connection, owner)
            queue.run_sync(job)
            task.refresh_from_db(using="other")
            self.assertEqual(task.successful_runs, 1)
            self.assertEqual(task.failed_runs, 0)
            self.assertNotEqual(task.job_name, owner)
            self.assertEqual(queue.scheduled_job_registry.all(queue.connection), [task.job_name])
            self.assertFalse(Task.objects.using("default").exists())

    def test_non_cron_creation_preserves_explicit_database_with_write_router(self):
        for task_type in (TaskType.ONCE, TaskType.REPEATABLE):
            with self.subTest(task_type=task_type), self.settings(DATABASE_ROUTERS=[DefaultWriteRouter()]):
                task = task_factory(task_type, instance_only=True)
                task.save(using="other")
                task.refresh_from_db(using="other")
                job = JobModel.get(task.job_name, connection=task.rqueue.connection)
                self.assertEqual(job.meta["scheduler_task_database"], "other")
                self.assertFalse(Task.objects.using("default").exists())

    def test_non_cron_completion_preserves_database_with_write_router(self):
        for task_type in (TaskType.ONCE, TaskType.REPEATABLE):
            with self.subTest(task_type=task_type):
                first = task_factory(task_type)
                second = task_factory(task_type, instance_only=True, id=first.pk)
                second.save(using="other")
                original = Task.objects.using("default").values().get(pk=first.pk)
                queue = second.rqueue
                job = JobModel.get(second.job_name, connection=queue.connection)
                queue.scheduled_job_registry.delete(queue.connection, job.name)
                with (
                    self.settings(DATABASE_ROUTERS=[DefaultWriteRouter()]),
                    patch("django.utils.timezone.now", return_value=second.scheduled_time + timedelta(seconds=1)),
                ):
                    queue.run_sync(job)
                self.assertEqual(Task.objects.using("default").values().get(pk=first.pk), original)
                second.refresh_from_db(using="other")
                self.assertEqual(second.successful_runs, 1)
                self.assertEqual(second.failed_runs, 0)
                if task_type == TaskType.ONCE:
                    self.assertIsNone(second.job_name)
                else:
                    self.assertNotEqual(second.job_name, job.name)
                    successor = JobModel.get(second.job_name, connection=queue.connection)
                    self.assertEqual(successor.meta["scheduler_task_database"], "other")
                    self.assertIn(second.job_name, queue.scheduled_job_registry.all(queue.connection))

    def test_same_id_and_timestamp_preserve_both_database_jobs(self):
        with patch("django.utils.timezone.now", return_value=timezone.now()):
            first = task_factory(TaskType.CRON)
            second = task_factory(TaskType.CRON, instance_only=True, id=first.pk)
            second.save(using="other")
        queue = first.rqueue
        self.assertCountEqual(queue.scheduled_job_registry.all(queue.connection), [first.job_name, second.job_name])
        for task, alias in ((first, "default"), (second, "other")):
            job = JobModel.get(task.job_name, connection=queue.connection)
            self.assertEqual(job.meta["scheduler_task_database"], alias)
            queue.scheduled_job_registry.delete(queue.connection, job.name)
            queue.run_sync(job)
            task.refresh_from_db(using=alias)
            self.assertEqual(task.successful_runs, 1)
            self.assertNotEqual(task.job_name, job.name)
        self.assertCountEqual(queue.scheduled_job_registry.all(queue.connection), [first.job_name, second.job_name])

    def test_manual_enqueue_preserves_selected_database_with_write_router(self):
        first = task_factory(TaskType.CRON)
        second = task_factory(TaskType.CRON, instance_only=True, id=first.pk)
        second.save(using="other")
        owners = [first.job_name, second.job_name]
        queue = second.rqueue
        with self.settings(DATABASE_ROUTERS=[DefaultWriteRouter()]):
            second.enqueue_to_run()
            [name] = queue.queued_job_registry.all(queue.connection)
            job = JobModel.get(name, connection=queue.connection)
            self.assertEqual(job.meta["scheduler_task_database"], "other")
            self.assertTrue(job.meta["scheduler_manual_run"])
            queue.queued_job_registry.delete(queue.connection, name)
            queue.run_sync(job)
        first.refresh_from_db()
        second.refresh_from_db(using="other")
        self.assertEqual(first.successful_runs, 0)
        self.assertEqual(second.successful_runs, 1)
        self.assertEqual([first.job_name, second.job_name], owners)
        self.assertCountEqual(queue.scheduled_job_registry.all(queue.connection), owners)

    def test_unschedule_preserves_selected_database_with_write_router(self):
        first = task_factory(TaskType.CRON)
        second = task_factory(TaskType.CRON, instance_only=True, id=first.pk)
        second.save(using="other")
        owner = first.job_name
        with self.settings(DATABASE_ROUTERS=[DefaultWriteRouter()]):
            second.unschedule(enabled=False)
        first.refresh_from_db()
        second.refresh_from_db(using="other")
        self.assertTrue(first.enabled)
        self.assertEqual(first.job_name, owner)
        self.assertFalse(second.enabled)
        self.assertIsNone(second.job_name)
        self.assertEqual(first.rqueue.scheduled_job_registry.all(first.rqueue.connection), [owner])
