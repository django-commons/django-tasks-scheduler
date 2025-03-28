import os

from django.conf import settings

from scheduler.settings import conf_settings

settings.SCHEDULER_QUEUES = {
    "default": {"HOST": "localhost", "PORT": 6379, "DB": 0},
    "test": {"HOST": "localhost", "PORT": 1, "DB": 1},
    "sentinel": {
        "SENTINELS": [("localhost", 26736), ("localhost", 26737)],
        "MASTER_NAME": "testmaster",
        "DB": 1,
        "USERNAME": "redis-user",
        "PASSWORD": "secret",
        "SENTINEL_KWARGS": {},
    },
    "test1": {
        "HOST": "localhost",
        "PORT": 1,
        "DB": 1,
    },
    "test2": {
        "HOST": "localhost",
        "PORT": 1,
        "DB": 1,
    },
    "test3": {
        "HOST": "localhost",
        "PORT": 6379,
        "DB": 1,
    },
    "async": {
        "HOST": "localhost",
        "PORT": 6379,
        "DB": 1,
        "ASYNC": False,
    },
    "url": {
        "URL": "redis://username:password@host:1234/",
        "DB": 4,
    },
    "url_with_db": {
        "URL": "redis://username:password@host:1234/5",
    },
    "url_default_db": {
        "URL": "redis://username:password@host:1234",
    },
    "django_tasks_scheduler_test": {
        "HOST": "localhost",
        "PORT": 6379,
        "DB": 0,
    },
    "scheduler_scheduler_active_test": {
        "HOST": "localhost",
        "PORT": 6379,
        "DB": 0,
        "ASYNC": False,
    },
    "scheduler_scheduler_inactive_test": {
        "HOST": "localhost",
        "PORT": 6379,
        "DB": 0,
        "ASYNC": False,
    },
    "worker_scheduler_active_test": {
        "HOST": "localhost",
        "PORT": 6379,
        "DB": 0,
        "ASYNC": False,
    },
    "worker_scheduler_inactive_test": {
        "HOST": "localhost",
        "PORT": 6379,
        "DB": 0,
        "ASYNC": False,
    },
    "django_tasks_scheduler_test2": {
        "HOST": "localhost",
        "PORT": 6379,
        "DB": 0,
    },
    "test_scheduler": {
        "HOST": "localhost",
        "PORT": 6379,
        "DB": 0,
    },
}
if os.getenv("FAKEREDIS", "False") == "True":  # pragma: no cover
    for name, queue_settings in settings.SCHEDULER_QUEUES:  # pragma: no cover
        queue_settings["BROKER"] = "fakeredis"  # pragma: no cover

conf_settings()
