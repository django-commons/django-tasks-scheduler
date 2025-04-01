from typing import List

from django.contrib import admin, messages
from django.contrib.contenttypes.admin import GenericStackedInline
from django.db.models import QuerySet
from django.http import HttpRequest
from django.utils.translation import gettext_lazy as _

from scheduler.helpers.queues import get_queue
from scheduler.models import TaskArg, TaskKwarg, Task, TaskType, get_next_cron_time
from scheduler.redis_models import JobModel
from scheduler.settings import SCHEDULER_CONFIG, logger
from scheduler.types import ConnectionErrorTypes


def job_execution_of(job: JobModel, task: Task) -> bool:
    return job.scheduled_task_id == task.id and job.task_type == task.task_type


def get_job_executions_for_task(queue_name: str, scheduled_task: Task) -> List[JobModel]:
    queue = get_queue(queue_name)
    job_list: List[JobModel] = JobModel.get_many(queue.get_all_job_names(), connection=queue.connection)
    res = sorted(
        list(filter(lambda j: job_execution_of(j, scheduled_task), job_list)), key=lambda j: j.created_at, reverse=True
    )
    return res


class JobArgInline(GenericStackedInline):
    model = TaskArg
    extra = 0
    fieldsets = ((None, dict(fields=("arg_type", "val"))),)


class JobKwargInline(GenericStackedInline):
    model = TaskKwarg
    extra = 0
    fieldsets = ((None, dict(fields=("key", ("arg_type", "val")))),)


def get_message_bit(rows_updated: int) -> str:
    message_bit = "1 task was" if rows_updated == 1 else f"{rows_updated} tasks were"
    return message_bit


@admin.register(Task)
class TaskAdmin(admin.ModelAdmin):
    """TaskAdmin admin view for all task models."""

    class Media:
        js = (
            "admin/js/jquery.init.js",
            "admin/js/select-fields.js",
        )

    save_on_top = True
    change_form_template = "admin/scheduler/change_form.html"
    actions = [
        "disable_selected",
        "enable_selected",
        "enqueue_job_now",
    ]
    inlines = [
        JobArgInline,
        JobKwargInline,
    ]
    list_filter = ("enabled", "task_type", "queue")
    list_display = (
        "enabled",
        "name",
        "function_string",
        "is_scheduled",
        "queue",
        "task_schedule",
        "next_run",
        "successful_runs",
        "last_successful_run",
        "failed_runs",
        "last_failed_run",
    )
    list_display_links = ("name",)
    readonly_fields = (
        "job_name",
        "successful_runs",
        "last_successful_run",
        "failed_runs",
        "last_failed_run",
    )
    # radio_fields = {"task_type": admin.HORIZONTAL}
    fieldsets = (
        (
            None,
            dict(
                fields=(
                    "name",
                    "callable",
                    ("enabled", "timeout", "result_ttl"),
                    "task_type",
                )
            ),
        ),
        (
            None,
            dict(fields=("scheduled_time",), classes=("tasktype-OnceTaskType",)),
        ),
        (
            None,
            dict(fields=("cron_string",), classes=("tasktype-CronTaskType",)),
        ),
        (
            None,
            dict(
                fields=(
                    (
                        "interval",
                        "interval_unit",
                    ),
                    "repeat",
                ),
                classes=("tasktype-RepeatableTaskType",),
            ),
        ),
        (_("Queue settings"), dict(fields=(("queue", "at_front"), "job_name"))),
        (
            _("Previous runs info"),
            dict(fields=(("successful_runs", "last_successful_run"), ("failed_runs", "last_failed_run"))),
        ),
    )

    @admin.display(description="Schedule")
    def task_schedule(self, o: Task) -> str:
        if o.task_type == TaskType.ONCE.value:
            return f"Run once: {o.scheduled_time:%Y-%m-%d %H:%M:%S}"
        elif o.task_type == TaskType.CRON.value:
            return f"Cron: {o.cron_string}"
        else:  # if o.task_type == TaskType.REPEATABLE.value:
            if o.interval is None or o.interval_unit is None:
                return ""
            return f"Repeatable: {o.interval} {o.get_interval_unit_display()}"

    @admin.display(description="Next run")
    def next_run(self, o: Task) -> str:
        return get_next_cron_time(o.cron_string)

    def change_view(self, request: HttpRequest, object_id, form_url="", extra_context=None):
        extra = extra_context or {}
        obj = self.get_object(request, object_id)
        try:
            execution_list = get_job_executions_for_task(obj.queue, obj)
        except ConnectionErrorTypes as e:
            logger.warn(f"Could not get job executions: {e}")
            execution_list = list()
        paginator = self.get_paginator(request, execution_list, SCHEDULER_CONFIG.EXECUTIONS_IN_PAGE)
        page_number = request.GET.get("p", 1)
        page_obj = paginator.get_page(page_number)
        page_range = paginator.get_elided_page_range(page_obj.number)

        extra.update(
            {
                "pagination_required": paginator.count > SCHEDULER_CONFIG.EXECUTIONS_IN_PAGE,
                "executions": page_obj,
                "page_range": page_range,
                "page_var": "p",
            }
        )

        return super(TaskAdmin, self).change_view(request, object_id, form_url, extra_context=extra)

    def delete_queryset(self, request: HttpRequest, queryset: QuerySet) -> None:
        for job in queryset:
            job.unschedule()
        super(TaskAdmin, self).delete_queryset(request, queryset)

    def delete_model(self, request: HttpRequest, obj: Task) -> None:
        obj.unschedule()
        super(TaskAdmin, self).delete_model(request, obj)

    @admin.action(description=_("Disable selected %(verbose_name_plural)s"), permissions=("change",))
    def disable_selected(self, request: HttpRequest, queryset: QuerySet) -> None:
        rows_updated = 0
        for obj in queryset.filter(enabled=True).iterator():
            obj.enabled = False
            obj.unschedule()
            rows_updated += 1

        level = messages.WARNING if not rows_updated else messages.INFO
        self.message_user(
            request, f"{get_message_bit(rows_updated)} successfully disabled and unscheduled.", level=level
        )

    @admin.action(description=_("Enable selected %(verbose_name_plural)s"), permissions=("change",))
    def enable_selected(self, request: HttpRequest, queryset: QuerySet) -> None:
        rows_updated = 0
        for obj in queryset.filter(enabled=False).iterator():
            obj.enabled = True
            obj.save()
            rows_updated += 1

        level = messages.WARNING if not rows_updated else messages.INFO
        self.message_user(request, f"{get_message_bit(rows_updated)} successfully enabled and scheduled.", level=level)

    @admin.action(description="Enqueue now", permissions=("change",))
    def enqueue_job_now(self, request: HttpRequest, queryset: QuerySet) -> None:
        task_names = []
        for task in queryset:
            task.enqueue_to_run()
            task_names.append(task.name)
        self.message_user(
            request,
            f"The following jobs have been enqueued: {', '.join(task_names)}",
        )
