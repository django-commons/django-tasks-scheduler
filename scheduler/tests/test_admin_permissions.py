from django.contrib import admin
from django.contrib.auth.models import Permission, User
from django.test import RequestFactory, TestCase

from scheduler.admin.ephemeral_models import QueueAdmin
from scheduler.models.ephemeral_models import Queue


class TestImmutableAdminModulePermission(TestCase):
    def setUp(self) -> None:
        self.request_factory = RequestFactory()
        self.admin = QueueAdmin(Queue, admin.site)

    def _make_staff_user(self, username: str = "staff", with_scheduler_perm: bool = False) -> User:
        user = User.objects.create_user(username=username, password="pwd", is_staff=True)
        if with_scheduler_perm:
            # Any permission in the 'scheduler' app should grant module perms
            perm = Permission.objects.get(codename="view_task", content_type__app_label="scheduler")
            user.user_permissions.add(perm)
        return user

    def test_has_module_permission_without_any_scheduler_perms_is_false(self) -> None:
        user = self._make_staff_user("no_perm", with_scheduler_perm=False)
        request = self.request_factory.get("/")
        request.user = user

        assert self.admin.has_module_permission(request) is False

    def test_has_module_permission_with_any_scheduler_perm_is_true(self) -> None:
        user = self._make_staff_user("with_perm", with_scheduler_perm=True)
        request = self.request_factory.get("/")
        request.user = user

        assert self.admin.has_module_permission(request) is True
