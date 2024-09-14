from time import sleep

from scheduler.queues import get_queue

_counter = 0


def arg_callable():
    global _counter
    _counter += 1
    return _counter


def test_args_kwargs(*args, **kwargs):
    func = "test_args_kwargs({})"
    args_list = [repr(arg) for arg in args]
    kwargs_list = [f"{k}={v}" for (k, v) in kwargs.items()]
    return func.format(", ".join(args_list + kwargs_list))


def long_job():
    sleep(10)


test_non_callable = "I am a teapot"


def failing_job():
    raise ValueError


def test_job():
    return 1 + 1


def enqueue_jobs():
    queue = get_queue()
    for i in range(20):
        queue.enqueue(test_job, job_id=f"job_{i}", args=())
