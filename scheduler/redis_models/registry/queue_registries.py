import time
from datetime import datetime, timedelta, timezone
from typing import ClassVar, Optional, List, Tuple

from scheduler.helpers.utils import current_timestamp
from scheduler.types import ConnectionType
from .base_registry import JobNamesRegistry
from .. import as_str
from ..job import JobModel


class QueuedJobRegistry(JobNamesRegistry):
    _element_key_template: ClassVar[str] = ":registry:{}:queued_jobs"

    def cleanup(self, connection: ConnectionType, timestamp: Optional[float] = None) -> None:
        """This method is only here to prevent errors because this method is automatically called by `count()`
        and `all()` methods implemented in JobIdsRegistry."""
        pass

    def compact(self) -> None:
        """Removes all "dead" jobs from the queue by cycling through it, while guaranteeing FIFO semantics."""
        jobs_with_ts = self.all_with_timestamps()
        for job_name, timestamp in jobs_with_ts:
            if not JobModel.exists(job_name, self.connection):
                self.delete(connection=self.connection, job_name=job_name)

    def empty(self) -> None:
        queued_jobs_count = self.count(connection=self.connection)
        with self.connection.pipeline() as pipe:
            for offset in range(0, queued_jobs_count, 1000):
                job_names = self.all(offset, 1000)
                for job_name in job_names:
                    self.delete(connection=pipe, job_name=job_name)
                JobModel.delete_many(job_names, connection=pipe)
            pipe.execute()


class FinishedJobRegistry(JobNamesRegistry):
    _element_key_template: ClassVar[str] = ":registry:{}:finished_jobs"


class FailedJobRegistry(JobNamesRegistry):
    _element_key_template: ClassVar[str] = ":registry:{}:failed_jobs"


class CanceledJobRegistry(JobNamesRegistry):
    _element_key_template: ClassVar[str] = ":registry:{}:canceled_jobs"

    def cleanup(self, connection: ConnectionType, timestamp: Optional[float] = None) -> None:
        """This method is only here to prevent errors because this method is automatically called by `count()`
        and `all()` methods implemented in JobIdsRegistry."""
        pass


class ScheduledJobRegistry(JobNamesRegistry):
    _element_key_template: ClassVar[str] = ":registry:{}:scheduled_jobs"

    def cleanup(self, connection: ConnectionType, timestamp: Optional[float] = None) -> None:
        """This method is only here to prevent errors because this method is automatically called by `count()`
        and `all()` methods implemented in JobIdsRegistry."""
        pass

    def schedule(self, connection: ConnectionType, job_name: str, scheduled_datetime: datetime) -> int:
        """Adds job_name to registry, scored by its execution time (in UTC).
        If datetime has no tzinfo, it will assume localtimezone.

        :param connection: Broker connection
        :param job_name: Job name to schedule
        :param scheduled_datetime: datetime to schedule job
        """
        # If datetime has no timezone, assume server's local timezone
        if not scheduled_datetime.tzinfo:
            tz = timezone(timedelta(seconds=-(time.timezone if time.daylight == 0 else time.altzone)))
            scheduled_datetime = scheduled_datetime.replace(tzinfo=tz)

        timestamp = scheduled_datetime.timestamp()
        return self.add(connection=connection, job_name=job_name, score=timestamp)

    def get_jobs_to_schedule(self, timestamp: int, chunk_size: int = 1000) -> List[str]:
        """Gets a list of job names that should be scheduled.

        :param timestamp: timestamp/score of jobs in SortedSet.
        :param chunk_size: Max results to return.
        :returns: A list of job names
        """
        jobs_to_schedule = self.connection.zrangebyscore(self._key, 0, max=timestamp, start=0, num=chunk_size)
        return [as_str(job_name) for job_name in jobs_to_schedule]

    def get_scheduled_time(self, job_name: str) -> Optional[datetime]:
        """Returns datetime (UTC) at which job is scheduled to be enqueued

        :param job_name: Job name
        :returns: The scheduled time as datetime object, or None if job is not found
        """

        score: Optional[float] = self.connection.zscore(self._key, job_name)
        if not score:
            return None

        return datetime.fromtimestamp(score, tz=timezone.utc)


class ActiveJobRegistry(JobNamesRegistry):
    """Registry of currently executing jobs. Each queue maintains a ActiveJobRegistry."""

    _element_key_template: ClassVar[str] = ":registry:{}:active"

    def get_job_names_before(self, connection: ConnectionType, timestamp: Optional[float]) -> List[Tuple[str, float]]:
        """Returns job names whose score is lower than a timestamp timestamp.

        Returns names for jobs with an expiry time earlier than timestamp,
        specified as seconds since the Unix epoch.
        timestamp defaults to calltime if unspecified.
        """
        score = timestamp or current_timestamp()
        jobs_before = connection.zrangebyscore(self._key, 0, score, withscores=True)
        return [(as_str(job_name), score) for (job_name, score) in jobs_before]
