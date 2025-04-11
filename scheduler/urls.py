from django.urls import path

from . import views

urlpatterns = [
    path("queues/", views.stats, name="queues_home"),
    path("queues/stats.json", views.stats_json, name="queues_home_json"),
    path("queues/<str:queue_name>/workers/", views.queue_workers, name="queue_workers"),
    path("queues/<str:queue_name>/<str:registry_name>/jobs", views.list_registry_jobs, name="queue_registry_jobs"),
    path(
        "queues/<str:queue_name>/<str:registry_name>/<str:action>/",
        views.queue_registry_actions,
        name="queue_registry_action",
    ),
    path("queues/<str:queue_name>/confirm-action/", views.queue_confirm_job_action, name="queue_confirm_job_action"),
    path("queues/<str:queue_name>/actions/", views.queue_job_actions, name="queue_job_actions"),
]

urlpatterns += [
    path("workers/", views.workers_list, name="workers_home"),
    path("workers/<str:name>/", views.worker_details, name="worker_details"),
    path("jobs/<str:job_name>/", views.job_detail, name="job_details"),
    path("jobs/<str:job_name>/<str:action>/", views.job_action, name="job_detail_action"),
]
