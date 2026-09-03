from datetime import timedelta
from datetime import timezone as dt_timezone

from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings
from django.utils import timezone

from scheduler.helpers.callback import Callback, CallbackSetupError
from scheduler.helpers.queues import get_queue
from scheduler.helpers.utils import current_timestamp
from scheduler.models import TaskType, get_next_cron_time, get_scheduled_task
from scheduler.redis_models import QueuedJobRegistry
from scheduler.tests import conf  # noqa
from scheduler.tests.jobs import test_job
from scheduler.tests.testtools import SchedulerBaseCase, task_factory


class TestInternals(SchedulerBaseCase):
    def test_get_scheduled_job(self):
        task = task_factory(TaskType.ONCE, scheduled_time=timezone.now() + timedelta(hours=1))
        self.assertEqual(task, get_scheduled_task(TaskType.ONCE, task.id))
        with self.assertRaises(ValueError) as cm:
            get_scheduled_task(task.task_type, task.id + 1)
        self.assertIn("does not exist", str(cm.exception))
        self.assertNotIn("Invalid task type", str(cm.exception))
        with self.assertRaises(ValueError):
            get_scheduled_task("UNKNOWN_JOBTYPE", task.id)

    def test_task_update(self):
        task = task_factory(TaskType.ONCE)
        task.name = "new_name"
        task.save(update_fields=["name"])

    def test_callback_bad_arguments(self):
        with self.assertRaises(CallbackSetupError) as cm:
            Callback("scheduler.tests.jobs.test_job", "1m")
        self.assertEqual(str(cm.exception), "Callback `timeout` must be a positive int, but received 1m")
        with self.assertRaises(CallbackSetupError) as cm:
            Callback("scheduler.tests.jobs.non_existing_method")
        self.assertEqual(str(cm.exception), "Callback `func` is not callable: scheduler.tests.jobs.non_existing_method")
        with self.assertRaises(CallbackSetupError) as cm:
            Callback("scheduler.tests.non_existing_module.non_existing_method")
        self.assertEqual(
            str(cm.exception),
            "Callback `func` is not callable: scheduler.tests.non_existing_module.non_existing_method",
        )
        with self.assertRaises(CallbackSetupError) as cm:
            Callback("non_existing_method")
        self.assertEqual(str(cm.exception), "Callback `func` is not callable: non_existing_method")
        with self.assertRaises(CallbackSetupError) as cm:
            Callback(1)
        self.assertEqual(str(cm.exception), "Callback `func` must be a string or function, received 1")


class TestEnqueueJobScore(SchedulerBaseCase):
    def test_enqueue_job__score_is_current_time_not_registry_max(self):
        # arrange
        queue = get_queue("default")
        registry = queue.queued_job_registry
        first_job = queue.create_and_enqueue_job(test_job, name="z-job-enqueued-first")
        registry.add(queue.connection, first_job.name, current_timestamp() - 100)
        # act
        enqueue_time = current_timestamp()
        second_job = queue.create_and_enqueue_job(test_job, name="a-job-enqueued-second")
        # assert
        self.assertGreaterEqual(queue.connection.zscore(registry.key, second_job.name), enqueue_time)
        _, first_dequeued = QueuedJobRegistry.pop(queue.connection, [registry], timeout=None)
        _, second_dequeued = QueuedJobRegistry.pop(queue.connection, [registry], timeout=None)
        self.assertEqual([first_job.name, second_job.name], [first_dequeued, second_dequeued])


class TestCleanRegistries(SchedulerBaseCase):
    def test_active_registry_entry_without_job_is_removed(self):
        queue = get_queue("default")
        queue.active_job_registry.add(queue.connection, "orphan-job-name", current_timestamp() - 3600)

        queue.clean_registries()

        self.assertFalse(queue.active_job_registry.exists(queue.connection, "orphan-job-name"))


class TestConfSettings(SchedulerBaseCase):
    @override_settings(SCHEDULER_CONFIG=[])
    def test_conf_settings__bad_scheduler_config(self):
        from scheduler import settings

        with self.assertRaises(ImproperlyConfigured) as cm:
            settings.conf_settings()

        self.assertEqual(str(cm.exception), "SCHEDULER_CONFIG should be a SchedulerConfiguration or dict")

    @override_settings(SCHEDULER_QUEUES=[])
    def test_conf_settings__bad_scheduler_queues_config(self):
        from scheduler import settings

        with self.assertRaises(ImproperlyConfigured) as cm:
            settings.conf_settings()

        self.assertEqual(str(cm.exception), "You have to define SCHEDULER_QUEUES in settings.py as dict")

    @override_settings(SCHEDULER_QUEUES={"default": []})
    def test_conf_settings__bad_queue_config(self):
        from scheduler import settings

        with self.assertRaises(ImproperlyConfigured) as cm:
            settings.conf_settings()

        self.assertEqual(str(cm.exception), "Queue default configuration should be a QueueConfiguration or dict")

    @override_settings(SCHEDULER_CONFIG={"UNKNOWN_SETTING": 10})
    def test_conf_settings__unknown_setting(self):
        from scheduler import settings

        with self.assertRaises(ImproperlyConfigured) as cm:
            settings.conf_settings()

        self.assertEqual(str(cm.exception), "Unknown setting UNKNOWN_SETTING in SCHEDULER_CONFIG")

    @override_settings(USE_TZ=True, TIME_ZONE="EST")
    def test_get_next_cron_time(self):
        next_cron_time = get_next_cron_time("0 0 * * *")
        self.assertIsNotNone(next_cron_time)
        self.assertTrue(next_cron_time > timezone.now())
        self.assertEqual(dt_timezone.utc, next_cron_time.tzinfo)
