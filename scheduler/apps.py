from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class SchedulerConfig(AppConfig):
    default_auto_field = 'django.db.models.AutoField'
    name = 'scheduler'
    verbose_name = _('Tasks Scheduler')

    def ready(self):
        pass
