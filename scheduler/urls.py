from django.urls import path

from . import views

urlpatterns = [
    path("queues/", views.stats, name="queues_home"),
    path("queues/stats.json", views.stats_json, name="queues_home_json"),
    path("queues/<str:queue_name>/workers/", views.queue_workers, name="queue_workers"),
    path("queues/<str:queue_name>/<str:registry_name>/jobs", views.registry_jobs, name="queue_registry_jobs"),
    path("queues/<str:queue_name>/<str:registry_name>/empty/", views.clear_queue_registry, name="queue_clear"),
    path("queues/<str:queue_name>/<str:registry_name>/requeue-all/", views.requeue_all, name="queue_requeue_all"),
    path("queues/<str:queue_name>/confirm-action/", views.queue_confirm_action, name="queue_confirm_action"),
    path("queues/<str:queue_name>/actions/", views.queue_actions, name="queue_actions"),
]

urlpatterns += [
    path("workers/", views.workers_list, name="workers_home"),
    path("workers/<str:name>/", views.worker_details, name="worker_details"),
    path("jobs/<str:job_name>/", views.job_detail, name="job_details"),
    path("jobs/<str:job_name>/<str:action>/", views.job_action, name="queue_job_action"),
]
