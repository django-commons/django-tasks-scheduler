"""Optional PostgreSQL configuration for the concurrent cron ownership tests."""
import os

from .settings import *  # noqa: F403

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.getenv("PGDATABASE", "scheduler"),
        "USER": os.getenv("PGUSER", "scheduler"),
        "PASSWORD": os.getenv("PGPASSWORD", "scheduler-test"),
        "HOST": os.getenv("PGHOST", "127.0.0.1"),
        "PORT": os.getenv("PGPORT", "5432"),
    }
}
USE_TZ = True
DATABASES["other"] = {**DATABASES["default"], "NAME": DATABASES["default"]["NAME"] + "_other"}
