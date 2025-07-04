from typing import Any

from django.contrib import admin
from django.http import HttpResponse, HttpRequest

from scheduler import views
from scheduler.models.ephemeral_models import Queue, Worker


class ImmutableAdmin(admin.ModelAdmin):
    def has_add_permission(self, request: HttpRequest) -> bool:
        return False  # Hide the admin "+ Add" link for Queues

    def has_change_permission(self, request: HttpRequest, obj: Any = None) -> bool:
        return True

    def has_module_permission(self, request: HttpRequest) -> bool:
        """Returns True if the given request has any permission in the given app label.

        Can be overridden by the user in subclasses. In such case, it should return True if the given request has
        permission to view the module on the admin index page and access the module's index page. Overriding it does
        not restrict access to the add, change or delete views. Use `ModelAdmin.has_(add|change|delete)_permission` for
        that.
        """
        return request.user.has_module_perms("django-tasks-scheduler")  # type: ignore


@admin.register(Queue)
class QueueAdmin(ImmutableAdmin):
    """Admin View for queues"""

    def changelist_view(self, request: HttpRequest, extra_context: Any = None) -> HttpResponse:
        """The 'change list' admin view for this model."""
        return views.stats(request)


@admin.register(Worker)
class WorkerAdmin(ImmutableAdmin):
    """Admin View for workers"""

    def changelist_view(self, request: HttpRequest, extra_context: Any = None) -> HttpResponse:
        """The 'change list' admin view for this model."""
        return views.workers_list(request)
