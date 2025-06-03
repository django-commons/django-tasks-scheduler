from django.http.response import HttpResponse
from django.views.decorators.cache import cache_page

from scheduler import job


@cache_page(timeout=500)
def my_view(request):
    return HttpResponse("Yeah")

@job()
def my_job():
    return "This is a job"