import redis
from django.contrib import admin, messages
from django.contrib.contenttypes.admin import GenericStackedInline
from django.utils.translation import gettext_lazy as _

from scheduler import tools
from scheduler.models import CronTask, TaskArg, TaskKwarg, RepeatableTask, ScheduledTask
from scheduler.settings import SCHEDULER_CONFIG, logger
from scheduler.tools import get_job_executions


class HiddenMixin(object):
    class Media:
        js = ['admin/js/jquery.init.js', ]


class JobArgInline(HiddenMixin, GenericStackedInline):
    model = TaskArg
    extra = 0
    fieldsets = (
        (None, {
            'fields': (('arg_type', 'val',),),
        }),
    )


class JobKwargInline(HiddenMixin, GenericStackedInline):
    model = TaskKwarg
    extra = 0
    fieldsets = (
        (None, {
            'fields': (('key',), ('arg_type', 'val',),),
        }),
    )


_LIST_DISPLAY_EXTRA = dict(
    CronTask=('cron_string', 'next_run', 'successful_runs', 'last_successful_run', 'failed_runs', 'last_failed_run',),
    ScheduledTask=('scheduled_time',),
    RepeatableTask=(
        'scheduled_time', 'interval_display', 'successful_runs', 'last_successful_run', 'failed_runs',
        'last_failed_run',),
)
_FIELDSET_EXTRA = dict(
    CronTask=(
        'cron_string', 'timeout', 'result_ttl',
        ('successful_runs', 'last_successful_run',),
        ('failed_runs', 'last_failed_run',),
    ),
    ScheduledTask=('scheduled_time', 'timeout', 'result_ttl'),
    RepeatableTask=(
        'scheduled_time',
        ('interval', 'interval_unit',),
        'repeat', 'timeout', 'result_ttl',
        ('successful_runs', 'last_successful_run',),
        ('failed_runs', 'last_failed_run',),
    ),
)


@admin.register(CronTask, ScheduledTask, RepeatableTask)
class TaskAdmin(admin.ModelAdmin):
    """TaskAdmin admin view for all task models.
    Using the _LIST_DISPLAY_EXTRA and _FIELDSET_EXTRA additional data for each model.
    """

    save_on_top = True
    change_form_template = 'admin/scheduler/change_form.html'
    actions = ['disable_selected', 'enable_selected', 'enqueue_job_now', ]
    inlines = [JobArgInline, JobKwargInline, ]
    list_filter = ('enabled',)
    list_display = ('enabled', 'name', 'job_id', 'function_string', 'is_scheduled', 'queue',)
    list_display_links = ('name',)
    readonly_fields = ('job_id',)
    fieldsets = (
        (None, {
            'fields': ('name', 'callable', 'enabled', 'at_front',),
        }),
        (_('RQ Settings'), {
            'fields': ('queue', 'job_id',),
        }),
    )

    def get_list_display(self, request):
        if self.model.__name__ not in _LIST_DISPLAY_EXTRA:
            raise ValueError(f'Unrecognized model {self.model}')
        return TaskAdmin.list_display + _LIST_DISPLAY_EXTRA[self.model.__name__]

    def get_fieldsets(self, request, obj=None):
        if self.model.__name__ not in _FIELDSET_EXTRA:
            raise ValueError(f'Unrecognized model {self.model}')
        return TaskAdmin.fieldsets + ((_('Scheduling'), {
            'fields': _FIELDSET_EXTRA[self.model.__name__],
        }),)

    @admin.display(description='Next run')
    def next_run(self, o: CronTask):
        return tools.get_next_cron_time(o.cron_string)

    def change_view(self, request, object_id, form_url='', extra_context=None):
        extra = extra_context or {}
        obj = self.get_object(request, object_id)
        try:
            execution_list = get_job_executions(obj.queue, obj)
        except redis.ConnectionError as e:
            logger.warn(f'Could not get job executions: {e}')
            execution_list = list()
        paginator = self.get_paginator(request, execution_list, SCHEDULER_CONFIG['EXECUTIONS_IN_PAGE'])
        page_number = request.GET.get('p', 1)
        page_obj = paginator.get_page(page_number)
        page_range = paginator.get_elided_page_range(page_obj.number)

        extra.update({
            "pagination_required": paginator.count > SCHEDULER_CONFIG['EXECUTIONS_IN_PAGE'],
            'executions': page_obj,
            'page_range': page_range,
            'page_var': 'p',
        })

        return super(TaskAdmin, self).change_view(
            request, object_id, form_url, extra_context=extra)

    def delete_queryset(self, request, queryset):
        for job in queryset:
            job.unschedule()
        super(TaskAdmin, self).delete_queryset(request, queryset)

    def delete_model(self, request, obj):
        obj.unschedule()
        super(TaskAdmin, self).delete_model(request, obj)

    @admin.action(description=_("Disable selected %(verbose_name_plural)s"), permissions=('change',))
    def disable_selected(self, request, queryset):
        rows_updated = 0
        for obj in queryset.filter(enabled=True).iterator():
            obj.enabled = False
            obj.unschedule()
            rows_updated += 1

        message_bit = "1 job was" if rows_updated == 1 else f"{rows_updated} jobs were"

        level = messages.WARNING if not rows_updated else messages.INFO
        self.message_user(request, f"{message_bit} successfully disabled and unscheduled.", level=level)

    @admin.action(description=_("Enable selected %(verbose_name_plural)s"), permissions=('change',))
    def enable_selected(self, request, queryset):
        rows_updated = 0
        for obj in queryset.filter(enabled=False).iterator():
            obj.enabled = True
            obj.save()
            rows_updated += 1

        message_bit = "1 job was" if rows_updated == 1 else f"{rows_updated} jobs were"
        level = messages.WARNING if not rows_updated else messages.INFO
        self.message_user(request, f"{message_bit} successfully enabled and scheduled.", level=level)

    @admin.action(description="Enqueue now", permissions=('change',))
    def enqueue_job_now(self, request, queryset):
        task_names = []
        for task in queryset:
            task.enqueue_to_run()
            task_names.append(task.name)
        self.message_user(request, f"The following jobs have been enqueued: {', '.join(task_names)}", )
