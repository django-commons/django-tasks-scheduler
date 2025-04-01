import sys
from typing import Dict, Any, Optional

import click
from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.core.management.base import BaseCommand
from django.utils import timezone

from scheduler.models import TaskArg, TaskKwarg, Task
from scheduler.models import TaskType


def job_model_str(model_str: str) -> str:
    if model_str.find("Job") == len(model_str) - 3:
        return model_str[:-3] + "Task"
    return model_str


def get_task_type(model_str: str) -> TaskType:
    model_str = job_model_str(model_str)
    try:
        return TaskType(model_str)
    except ValueError:
        pass
    if model_str == "CronTask":
        return TaskType.CRON
    elif model_str == "RepeatableTask":
        return TaskType.REPEATABLE
    elif model_str in {"ScheduledTask", "OnceTask"}:
        return TaskType.ONCE
    raise ValueError(f"Invalid model {model_str}")


def create_task_from_dict(task_dict: Dict[str, Any], update: bool) -> Optional[Task]:
    existing_task = Task.objects.filter(name=task_dict["name"]).first()
    task_type = get_task_type(task_dict["model"])
    if existing_task:
        if update:
            click.echo(f'Found existing job "{existing_task}, removing it to be reinserted"')
            existing_task.delete()
        else:
            click.echo(f'Found existing job "{existing_task}", skipping')
            return None
    kwargs = dict(task_dict)
    kwargs["task_type"] = task_type
    del kwargs["model"]
    del kwargs["callable_args"]
    del kwargs["callable_kwargs"]
    if kwargs.get("scheduled_time", None):
        target = timezone.datetime.fromisoformat(kwargs["scheduled_time"])
        if not settings.USE_TZ and not timezone.is_naive(target):
            target = timezone.make_naive(target)
        kwargs["scheduled_time"] = target
    model_fields = filter(lambda field: hasattr(field, "attname"), Task._meta.get_fields())
    model_fields = set(map(lambda field: field.attname, model_fields))
    keys_to_ignore = list(filter(lambda _k: _k not in model_fields, kwargs.keys()))
    for k in keys_to_ignore:
        del kwargs[k]
    task = Task.objects.create(**kwargs)
    click.echo(f"Created task {task}")
    content_type = ContentType.objects.get_for_model(task)

    for arg in task_dict["callable_args"]:
        TaskArg.objects.create(
            content_type=content_type,
            object_id=task.id,
            **arg,
        )
    for arg in task_dict["callable_kwargs"]:
        TaskKwarg.objects.create(
            content_type=content_type,
            object_id=task.id,
            **arg,
        )
    return task


class Command(BaseCommand):
    """
    Import scheduled jobs
    """

    help = __doc__

    def add_arguments(self, parser):
        parser.add_argument(
            "-f",
            "--format",
            action="store",
            choices=["json", "yaml"],
            default="json",
            dest="format",
            help="format of input",
        )
        parser.add_argument(
            "--filename",
            action="store",
            dest="filename",
            help="File name to load (otherwise loads from standard input)",
        )

        parser.add_argument(
            "-r",
            "--reset",
            action="store_true",
            dest="reset",
            help="Remove all currently scheduled jobs before importing",
        )
        parser.add_argument(
            "-u",
            "--update",
            action="store_true",
            dest="update",
            help="Update existing records",
        )

    def handle(self, *args, **options):
        file = open(options.get("filename")) if options.get("filename") else sys.stdin
        jobs = list()
        if options.get("format") == "json":
            import json

            try:
                jobs = json.load(file)
            except json.decoder.JSONDecodeError:
                click.echo("Error decoding json", err=True)
                exit(1)
        elif options.get("format") == "yaml":
            try:
                import yaml
            except ImportError:
                click.echo("Aborting. LibYAML is not installed.")
                exit(1)
            # Disable YAML alias
            yaml.Dumper.ignore_aliases = lambda *x: True
            jobs = yaml.load(file, yaml.SafeLoader)

        if options.get("reset"):
            Task.objects.all().delete()

        for job in jobs:
            create_task_from_dict(job, update=options.get("update"))
