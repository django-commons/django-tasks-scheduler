from datetime import timedelta

from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings
from django.utils import timezone

from scheduler.helpers.callback import Callback, CallbackSetupError
from scheduler.models import TaskType, get_scheduled_task
from scheduler.tests.testtools import SchedulerBaseCase, task_factory


class TestInternals(SchedulerBaseCase):
    def test_get_scheduled_job(self):
        task = task_factory(TaskType.ONCE, scheduled_time=timezone.now() - timedelta(hours=1))
        self.assertEqual(task, get_scheduled_task(TaskType.ONCE, task.id))
        with self.assertRaises(ValueError):
            get_scheduled_task(task.task_type, task.id + 1)
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
