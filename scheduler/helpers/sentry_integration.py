import weakref
from typing import Any, Callable

import sentry_sdk
from sentry_sdk._types import EventProcessor, Event, ExcInfo
from sentry_sdk.api import continue_trace
from sentry_sdk.consts import OP
from sentry_sdk.integrations import _check_minimum_version, Integration
from sentry_sdk.integrations.logging import ignore_logger
from sentry_sdk.tracing import TransactionSource
from sentry_sdk.utils import (
    capture_internal_exceptions,
    ensure_integration_enabled,
    event_from_exception,
    parse_version,
)

import scheduler
from scheduler.helpers.queues import Queue
from scheduler.redis_models import JobModel
from scheduler.redis_models import JobStatus
from scheduler.timeouts import JobTimeoutException
from scheduler.worker import Worker


class SentryIntegration(Integration):
    identifier = "rq"
    origin = f"auto.queue.{identifier}"

    @staticmethod
    def setup_once() -> None:
        version = parse_version(scheduler.__version__)
        _check_minimum_version(SentryIntegration, version)

        old_perform_job = Worker.worker_perform_job

        @ensure_integration_enabled(SentryIntegration, old_perform_job)
        def sentry_patched_perform_job(self: Any, job_model: JobModel, *args: Queue, **kwargs: Any) -> bool:
            with sentry_sdk.new_scope() as scope:
                scope.clear_breadcrumbs()
                scope.add_event_processor(_make_event_processor(weakref.ref(job_model)))

                transaction = continue_trace(
                    job_model.meta.get("_sentry_trace_headers") or {},
                    op=OP.QUEUE_TASK_RQ,
                    name="unknown RQ task",
                    source=TransactionSource.TASK,
                    origin=SentryIntegration.origin,
                )

                with capture_internal_exceptions():
                    transaction.name = job_model.func_name

                with sentry_sdk.start_transaction(
                    transaction,
                    custom_sampling_context={"rq_job": job_model},
                ):
                    rv = old_perform_job(self, job_model, *args, **kwargs)

            if self.is_horse:
                # We're inside of a forked process and RQ is
                # about to call `os._exit`. Make sure that our
                # events get sent out.
                sentry_sdk.get_client().flush()

            return rv

        Worker.worker_perform_job = sentry_patched_perform_job  # type: ignore[method-assign]

        old_handle_exception = Worker.handle_exception

        def sentry_patched_handle_exception(self: Worker, job: Any, *exc_info: Any, **kwargs: Any) -> Any:
            retry = hasattr(job, "retries_left") and job.retries_left and job.retries_left > 0
            failed = job._status == JobStatus.FAILED or job.is_failed
            if failed and not retry:
                _capture_exception(exc_info)

            return old_handle_exception(self, job, *exc_info, **kwargs)

        Worker.handle_exception = sentry_patched_handle_exception  # type: ignore[method-assign]

        old_enqueue_job = Queue.enqueue_job

        @ensure_integration_enabled(SentryIntegration, old_enqueue_job)
        def sentry_patched_enqueue_job(self: Queue, job: Any, **kwargs: Any) -> Any:
            scope = sentry_sdk.get_current_scope()
            if scope.span is not None:
                job.meta["_sentry_trace_headers"] = dict(scope.iter_trace_propagation_headers())

            return old_enqueue_job(self, job, **kwargs)

        Queue.enqueue_job = sentry_patched_enqueue_job  # type: ignore[method-assign]

        ignore_logger("rq.worker")


def _make_event_processor(weak_job: Callable[[], JobModel]) -> EventProcessor:
    def event_processor(event: Event, hint: dict[str, Any]) -> Event:
        job = weak_job()
        if job is not None:
            with capture_internal_exceptions():
                extra = event.setdefault("extra", {})
                extra["job"] = job.serialize()

        if "exc_info" in hint:
            with capture_internal_exceptions():
                if issubclass(hint["exc_info"][0], JobTimeoutException):
                    event["fingerprint"] = ["django-tasks-scheduler", "JobTimeoutException", job.func_name]

        return event

    return event_processor


def _capture_exception(exc_info: ExcInfo, **kwargs: Any) -> None:
    client = sentry_sdk.get_client()

    event, hint = event_from_exception(
        exc_info,
        client_options=client.options,
        mechanism={"type": "rq", "handled": False},
    )

    sentry_sdk.capture_event(event, hint=hint)
