import logging
import os
import sys

import click
from django.core.management.base import BaseCommand
from django.db import connections
from rq.logutils import setup_loghandlers

from scheduler.broker_types import ConnectionErrorTypes
from scheduler.rq_classes import register_sentry
from scheduler.tools import create_worker

VERBOSITY_TO_LOG_LEVEL = {
    0: logging.CRITICAL,
    1: logging.WARNING,
    2: logging.INFO,
    3: logging.DEBUG,
}

WORKER_ARGUMENTS = {
    "name",
    "default_result_ttl",
    "connection",
    "exc_handler",
    "exception_handlers",
    "default_worker_ttl",
    "maintenance_interval",
    "job_class",
    "queue_class",
    "log_job_description",
    "job_monitoring_interval",
    "disable_default_exception_handler",
    "prepare_for_work",
    "serializer",
    "work_horse_killed_handler",
}


def reset_db_connections():
    for c in connections.all():
        c.close()


class Command(BaseCommand):
    """
    Runs RQ workers on specified queues. Note that all queues passed into a
    single rqworker command must share the same connection.

    Example usage:
    python manage.py rqworker high medium low
    """

    args = "<queue queue ...>"

    def add_arguments(self, parser):
        parser.add_argument(
            "--pid", action="store", dest="pidfile", default=None, help="file to write the worker`s pid into"
        )
        parser.add_argument(
            "--burst", action="store_true", dest="burst", default=False, help="Run worker in burst mode"
        )
        parser.add_argument("--name", action="store", dest="name", default=None, help="Name of the worker")
        parser.add_argument(
            "--worker-ttl",
            action="store",
            type=int,
            dest="worker_ttl",
            default=420,
            help="Default worker timeout to be used",
        )
        parser.add_argument(
            "--max-jobs",
            action="store",
            default=None,
            dest="max_jobs",
            type=int,
            help="Maximum number of jobs to execute before terminating worker",
        )
        parser.add_argument(
            "--fork-job-execution",
            action="store",
            default=True,
            dest="fork_job_execution",
            type=bool,
            help="Fork job execution to another process",
        )
        parser.add_argument("--job-class", action="store", dest="job_class", help="Jobs class to use")
        parser.add_argument(
            "queues",
            nargs="*",
            type=str,
            help="The queues to work on, separated by space, all queues should be using the same redis",
        )
        parser.add_argument("--sentry-dsn", action="store", dest="sentry_dsn", help="Sentry DSN to use")
        parser.add_argument("--sentry-debug", action="store_true", dest="sentry_debug", help="Enable Sentry debug mode")
        parser.add_argument("--sentry-ca-certs", action="store", dest="sentry_ca_certs", help="Path to CA certs file")

    def handle(self, **options):
        queues = options.pop("queues", [])
        if not queues:
            queues = [
                "default",
            ]
        click.echo(f"Starting worker for queues {queues}")
        pidfile = options.pop("pidfile")
        if pidfile:
            with open(os.path.expanduser(pidfile), "w") as fp:
                fp.write(str(os.getpid()))

        # Verbosity is defined by default in BaseCommand for all commands
        verbosity = options.pop("verbosity", 1)
        log_level = VERBOSITY_TO_LOG_LEVEL.get(verbosity, logging.INFO)
        setup_loghandlers(log_level)

        init_options = {k: v for k, v in options.items() if k in WORKER_ARGUMENTS}

        try:
            # Instantiate a worker
            w = create_worker(*queues, **init_options)

            # Close any opened DB connection before any fork
            reset_db_connections()

            # Check whether sentry is enabled
            if options.get("sentry_dsn") is not None:
                sentry_opts = dict(ca_certs=options.get("sentry_ca_certs"), debug=options.get("sentry_debug"))
                register_sentry(options.get("sentry_dsn"), **sentry_opts)

            w.work(
                burst=options.get("burst", False),
                logging_level=log_level,
                max_jobs=options["max_jobs"],
            )
        except ConnectionErrorTypes as e:
            click.echo(str(e), err=True)
            sys.exit(1)
