from django.http.response import HttpResponse
from django.views.decorators.cache import cache_page


@cache_page(timeout=500)
def my_view(request):
    return HttpResponse("Yeah")
