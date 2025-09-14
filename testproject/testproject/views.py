from django.http import HttpResponse
from django.views.decorators.cache import cache_page

from scheduler import job
import time

@cache_page(timeout=500)
def my_view(request):
    return HttpResponse("Yeah")

@job("low")
def long_running_func():
    print("start the function")
    time.sleep(30)
    print("function finished")


def run_job(request):
    if request.method == "GET":
        print("got a GET-request")
        long_running_func.delay()
        return HttpResponse("OK - got a GET request")
    return HttpResponse(status=405)
