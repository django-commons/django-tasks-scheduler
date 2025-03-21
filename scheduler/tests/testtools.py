from datetime import timedelta

from django.contrib.auth.models import User
from django.contrib.contenttypes.models import ContentType
from django.contrib.messages import get_messages
from django.test import Client, TestCase
from django.utils import timezone

from scheduler import settings
from scheduler.models.args import TaskKwarg
from scheduler.models.task import Task
from scheduler.queues import get_queue
from scheduler.tools import TaskType


def assert_message_in_response(response, message):
    messages = [m.message for m in get_messages(response.wsgi_request)]
    assert message in messages, f'expected "{message}" in {messages}'


def sequence_gen():
    n = 1
    while True:
        yield n
        n += 1


seq = sequence_gen()


def task_factory(
    task_type: TaskType, callable_name: str = "scheduler.tests.jobs.test_job", instance_only=False, **kwargs
):
    values = dict(
        name="Scheduled Job %d" % next(seq),
        job_id=None,
        queue=list(settings.QUEUES.keys())[0],
        callable=callable_name,
        enabled=True,
        timeout=None,
    )
    if task_type == TaskType.ONCE:
        values.update(
            dict(
                result_ttl=None,
                scheduled_time=timezone.now() + timedelta(days=1),
            )
        )
    elif task_type == TaskType.REPEATABLE:
        values.update(
            dict(
                result_ttl=None,
                interval=1,
                interval_unit="hours",
                repeat=None,
                scheduled_time=timezone.now() + timedelta(days=1),
            )
        )
    elif task_type == TaskType.CRON:
        values.update(
            dict(
                cron_string="0 0 * * *",
            )
        )
    values.update(kwargs)
    if instance_only:
        instance = Task(task_type=task_type, **values)
    else:
        instance = Task.objects.create(task_type=task_type, **values)
    return instance


def taskarg_factory(cls, **kwargs):
    content_object = kwargs.pop("content_object", None)
    if content_object is None:
        content_object = task_factory(TaskType.ONCE)
    values = dict(
        arg_type="str",
        val="",
        object_id=content_object.id,
        content_type=ContentType.objects.get_for_model(content_object),
        content_object=content_object,
    )
    if cls == TaskKwarg:
        values["key"] = ("key%d" % next(seq),)
    values.update(kwargs)
    instance = cls.objects.create(**values)
    return instance


def _get_task_job_execution_from_registry(django_task: Task):
    jobs_to_schedule = django_task.rqueue.scheduled_job_registry.get_job_ids()
    entry = next(i for i in jobs_to_schedule if i == django_task.job_id)
    return django_task.rqueue.fetch_job(entry)


def _get_executions(django_job: Task):
    job_ids = django_job.rqueue.get_all_job_ids()
    return list(
        filter(lambda j: j.is_execution_of(django_job), map(lambda jid: django_job.rqueue.fetch_job(jid), job_ids))
    )


class SchedulerBaseCase(TestCase):
    @classmethod
    def setUpTestData(cls) -> None:
        super().setUpTestData()
        try:
            User.objects.create_superuser("admin", "admin@a.com", "admin")
        except Exception:
            pass
        cls.client = Client()

    def setUp(self) -> None:
        super(SchedulerBaseCase, self).setUp()
        queue = get_queue("default")
        queue.empty()

    def tearDown(self) -> None:
        super(SchedulerBaseCase, self).tearDown()
        queue = get_queue("default")
        queue.empty()

    @classmethod
    def setUpClass(cls):
        super(SchedulerBaseCase, cls).setUpClass()
        queue = get_queue("default")
        queue.connection.flushall()
