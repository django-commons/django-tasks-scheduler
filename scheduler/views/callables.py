from django.contrib.admin.views.decorators import staff_member_required
from django.http import HttpResponse, HttpRequest, JsonResponse

from scheduler.decorators import JOB_METHODS_LIST


@staff_member_required
def list_callables(request: HttpRequest) -> HttpResponse:
    return JsonResponse({"items": JOB_METHODS_LIST})
