from typing import Tuple, Optional

from django.http import Http404

from scheduler.helpers.queues import Queue
from scheduler.helpers.queues import get_queue as get_queue_base
from scheduler.redis_models import JobModel
from scheduler.settings import get_queue_names, logger, QueueNotFoundError

_QUEUES_WITH_BAD_CONFIGURATION = set()


def get_queue(queue_name: str) -> Queue:
    try:
        return get_queue_base(queue_name)
    except QueueNotFoundError as e:
        logger.error(e)
        raise Http404(e)


def _find_job(job_name: str) -> Tuple[Optional[Queue], Optional[JobModel]]:
    queue_names = get_queue_names()
    for queue_name in queue_names:
        if queue_name in _QUEUES_WITH_BAD_CONFIGURATION:
            continue
        try:
            queue = get_queue(queue_name)
            job = JobModel.get(job_name, connection=queue.connection)
            if job is not None and job.queue_name == queue_name:
                return queue, job
        except Exception as e:
            _QUEUES_WITH_BAD_CONFIGURATION.add(queue_name)
            logger.debug(f"Queue {queue_name} added to bad configuration - Got exception: {e}")
            pass
    return None, None
