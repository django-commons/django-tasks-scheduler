from datetime import timedelta

from django.utils import timezone

from scheduler.helpers.callback import Callback, CallbackSetupError
from scheduler.helpers.tools import get_scheduled_task
from scheduler.models.task import TaskType
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
        self.assertEqual(str(cm.exception), "Invalid attribute name: non_existing_method")
        with self.assertRaises(CallbackSetupError) as cm:
            Callback("scheduler.tests.non_existing_module.non_existing_method")
        self.assertEqual(str(cm.exception), "Invalid attribute name: non_existing_method")
        with self.assertRaises(CallbackSetupError) as cm:
            Callback("non_existing_method")
        self.assertEqual(str(cm.exception), "Invalid attribute name: non_existing_method")
        with self.assertRaises(CallbackSetupError) as cm:
            Callback(1)
        self.assertEqual(str(cm.exception), "Callback `func` must be a string or function, received 1")
