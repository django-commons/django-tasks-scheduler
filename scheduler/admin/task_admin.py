import redis
import valkey
from django.contrib import admin, messages
from django.contrib.contenttypes.admin import GenericStackedInline
from django.utils.translation import gettext_lazy as _

from scheduler import tools
from scheduler.models import TaskArg, TaskKwarg, Task
from scheduler.settings import SCHEDULER_CONFIG, logger
from scheduler.tools import get_job_executions_for_task


class HiddenMixin(object):
    class Media:
        js = ("admin/js/jquery.init.js",)


class JobArgInline(HiddenMixin, GenericStackedInline):
    model = TaskArg
    extra = 0
    fieldsets = ((None, dict(fields=("arg_type", "val"))),)


class JobKwargInline(HiddenMixin, GenericStackedInline):
    model = TaskKwarg
    extra = 0
    fieldsets = ((None, dict(fields=("key", ("arg_type", "val")))),)


@admin.register(Task)
class TaskAdmin(admin.ModelAdmin):
    """TaskAdmin admin view for all task models."""

    class Media:
        js = ("admin/js/jquery.init.js", "admin/js/select-fields.js",)

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
    list_filter = ("enabled",)
    list_display = (
        "enabled",
        "name",
        "job_id",
        "function_string",
        "is_scheduled",
        "queue",
        "scheduled_time",
        "interval_display",
        "cron_string",
        "next_run",
        "successful_runs",
        "last_successful_run",
        "failed_runs",
        "last_failed_run",
    )
    list_display_links = ("name",)
    readonly_fields = (
        "job_id",
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
                    "task_type",
                    ("enabled", "timeout", "result_ttl"),
                )
            ),
        ),
        (
            None,
            dict(fields=("scheduled_time",), classes=("tasktype-OnceTask",)),
        ),
        (
            None,
            dict(fields=("cron_string",), classes=("tasktype-CronTask",)),
        ), (
            None,
            dict(fields=("interval", "interval_unit", "repeat"), classes=("tasktype-RepeatableTask",)),
        ),
        (_("RQ Settings"), dict(fields=(("queue", "at_front"), "job_id"))),
        (
            _("Previous runs info"),
            dict(fields=(("successful_runs", "last_successful_run"), ("failed_runs", "last_failed_run"))),
        ),
    )

    @admin.display(description="Next run")
    def next_run(self, o: Task):
        return tools.get_next_cron_time(o.cron_string)

    def change_view(self, request, object_id, form_url="", extra_context=None):
        extra = extra_context or {}
        obj = self.get_object(request, object_id)
        try:
            execution_list = get_job_executions_for_task(obj.queue, obj)
        except (redis.ConnectionError, valkey.ConnectionError) as e:
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

    def delete_queryset(self, request, queryset):
        for job in queryset:
            job.unschedule()
        super(TaskAdmin, self).delete_queryset(request, queryset)

    def delete_model(self, request, obj):
        obj.unschedule()
        super(TaskAdmin, self).delete_model(request, obj)

    @admin.action(description=_("Disable selected %(verbose_name_plural)s"), permissions=("change",))
    def disable_selected(self, request, queryset):
        rows_updated = 0
        for obj in queryset.filter(enabled=True).iterator():
            obj.enabled = False
            obj.unschedule()
            rows_updated += 1

        message_bit = "1 job was" if rows_updated == 1 else f"{rows_updated} jobs were"

        level = messages.WARNING if not rows_updated else messages.INFO
        self.message_user(request, f"{message_bit} successfully disabled and unscheduled.", level=level)

    @admin.action(description=_("Enable selected %(verbose_name_plural)s"), permissions=("change",))
    def enable_selected(self, request, queryset):
        rows_updated = 0
        for obj in queryset.filter(enabled=False).iterator():
            obj.enabled = True
            obj.save()
            rows_updated += 1

        message_bit = "1 job was" if rows_updated == 1 else f"{rows_updated} jobs were"
        level = messages.WARNING if not rows_updated else messages.INFO
        self.message_user(request, f"{message_bit} successfully enabled and scheduled.", level=level)

    @admin.action(description="Enqueue now", permissions=("change",))
    def enqueue_job_now(self, request, queryset):
        task_names = []
        for task in queryset:
            task.enqueue_to_run()
            task_names.append(task.name)
        self.message_user(
            request,
            f"The following jobs have been enqueued: {', '.join(task_names)}",
        )
