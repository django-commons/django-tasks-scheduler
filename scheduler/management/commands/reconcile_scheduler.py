"""Inspect or repair recurring schedules using the same locks as the worker."""

from typing import Any

from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.db import router, transaction

from scheduler.models import Task, TaskType
from scheduler.models.cron import read_schedule, reconcile
from scheduler.types.broker_types import BrokerErrorTypes


class Command(BaseCommand):  # type: ignore[misc]
    help = "Inspect cron schedules; --apply adopts one live job and removes waiting duplicates."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--apply", action="store_true", help="Reconcile schedules under the task row lock")
        parser.add_argument("--database", help="Task database alias (defaults to the write router)")
        parser.add_argument("--queue", help="Limit inspection and repair to one queue")

    def handle(self, *args: Any, **options: Any) -> None:
        using = options["database"] or router.db_for_write(Task)
        tasks = Task.objects.using(using).filter(task_type=TaskType.CRON)
        if options["queue"]:
            tasks = tasks.filter(queue=options["queue"])
        ids = list(tasks.order_by("pk").values_list("pk", flat=True))
        try:
            for task_id in ids:
                with transaction.atomic(using=using):
                    task = tasks.select_for_update().filter(pk=task_id).first()
                    if task is None:
                        continue
                    schedule = read_schedule(task, task.rqueue)
                    self.stdout.write(f"{task.pk} {task.name}: {len(schedule.jobs)} live recurring jobs")
                    if options["apply"] and not reconcile(task):
                        raise CommandError(f"Could not reconcile {task.name}; check the scheduler log.")
        except BrokerErrorTypes as exc:
            raise CommandError(f"Cannot read scheduler state: {exc}") from exc
        self.stdout.write("Schedules reconciled." if options["apply"] else "Dry run. Add --apply to reconcile.")
