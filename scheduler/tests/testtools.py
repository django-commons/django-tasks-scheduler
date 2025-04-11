import multiprocessing
from datetime import timedelta
from typing import List, Tuple

from django.contrib.auth.models import User
from django.contrib.contenttypes.models import ContentType
from django.contrib.messages import get_messages
from django.test import Client, TestCase
from django.utils import timezone

from scheduler import settings
from scheduler.admin.task_admin import job_execution_of
from scheduler.helpers.queues import get_queue
from scheduler.models import Task, TaskType
from scheduler.models import TaskKwarg
from scheduler.redis_models import JobModel
from scheduler.worker import Worker
from scheduler.worker import create_worker

multiprocessing.set_start_method("fork")


def _run_worker_process(worker: Worker, **kwargs):
    worker.work(**kwargs)


def run_worker_in_process(*args, name="test-worker") -> Tuple[multiprocessing.Process, str]:
    worker = create_worker(*args, name=name, fork_job_execution=False)
    process = multiprocessing.Process(target=_run_worker_process, args=(worker,), kwargs=dict(with_scheduler=False))
    process.start()
    return process, name


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
        queue=list(settings._QUEUES.keys())[0],
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


def _get_task_scheduled_job_from_registry(django_task: Task) -> JobModel:
    jobs_to_schedule = django_task.rqueue.scheduled_job_registry.all()
    entry = next(i for i in jobs_to_schedule if i == django_task.job_name)
    return JobModel.get(entry, connection=django_task.rqueue.connection)


def _get_executions(task: Task):
    job_names = task.rqueue.get_all_job_names()
    job_list: List[JobModel] = JobModel.get_many(job_names, connection=task.rqueue.connection)
    return list(filter(lambda j: job_execution_of(j, task), job_list))


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
        queue.connection.flushall()

    def tearDown(self) -> None:
        super(SchedulerBaseCase, self).tearDown()
