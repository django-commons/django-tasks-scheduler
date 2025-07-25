import ctypes
import logging
import signal
import threading

logger = logging.getLogger("scheduler")


class BaseTimeoutException(Exception):
    """Base exception for timeouts."""

    pass


class JobTimeoutException(BaseTimeoutException):
    """Raised when a job takes longer to complete than the allowed maximum timeout value."""

    pass


class JobExecutionMonitorTimeoutException(BaseTimeoutException):
    """Raised when waiting for a job-execution-process exiting takes longer than the maximum timeout value."""

    pass


class BaseDeathPenalty:
    """Base class to setup job timeouts."""

    def __init__(self, timeout, exception=BaseTimeoutException, **kwargs):
        self._timeout = timeout
        self._exception = exception

    def __enter__(self):
        self.setup_death_penalty()

    def __exit__(self, type, value, traceback):
        # Always cancel immediately, since we're done
        try:
            self.cancel_death_penalty()
        except BaseTimeoutException:
            # Weird case: we're done with the with body, but now the alarm is fired. We may safely ignore this
            # situation and consider the body done.
            pass

        # __exit__ may return True to supress further exception handling.  We don't want to suppress any exceptions
        # here, since all errors should just pass through, BaseTimeoutException being handled normally to the invoking
        # context.
        return False

    def setup_death_penalty(self):
        raise NotImplementedError()

    def cancel_death_penalty(self):
        raise NotImplementedError()


class UnixSignalDeathPenalty(BaseDeathPenalty):
    def handle_death_penalty(self, signum, frame) -> None:
        raise self._exception(f"Task exceeded maximum timeout value ({self._timeout} seconds)")

    def setup_death_penalty(self) -> None:
        """Sets up an alarm signal and a signal handler that raises an exception after the timeout amount
        (expressed in seconds)."""
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGALRM, self.handle_death_penalty)
            signal.alarm(self._timeout)
        else:
            logger.warning(f"Ignoring death penalty setup in non-main thread `{threading.current_thread().name}`.")

    def cancel_death_penalty(self) -> None:
        """Removes the death penalty alarm and puts back the system into default signal handling."""
        if threading.current_thread() is threading.main_thread():
            signal.alarm(0)
            signal.signal(signal.SIGALRM, signal.SIG_DFL)
        else:
            logger.warning(f"Ignoring death penalty cancel in non-main thread `{threading.current_thread().name}`.")


class TimerDeathPenalty(BaseDeathPenalty):
    def __init__(self, timeout, exception=JobTimeoutException, **kwargs):
        super().__init__(timeout, exception, **kwargs)
        self._target_thread_id = threading.current_thread().ident
        self._timer = None

        # Monkey-patch exception with the message ahead of time
        # since PyThreadState_SetAsyncExc can only take a class
        def init_with_message(self, *args, **kwargs):  # noqa
            super(exception, self).__init__(f"Task exceeded maximum timeout value ({timeout} seconds)")

        self._exception.__init__ = init_with_message

    def new_timer(self):
        """Returns a new timer since timers can only be used once."""
        return threading.Timer(self._timeout, self.handle_death_penalty)

    def handle_death_penalty(self):
        """Raises an asynchronous exception in another thread.

        Reference http://docs.python.org/c-api/init.html#PyThreadState_SetAsyncExc for more info.
        """
        ret = ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_long(self._target_thread_id), ctypes.py_object(self._exception)
        )
        if ret == 0:
            raise ValueError(f"Invalid thread ID {self._target_thread_id}")
        elif ret > 1:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(self._target_thread_id), 0)
            raise SystemError("PyThreadState_SetAsyncExc failed")

    def setup_death_penalty(self):
        """Starts the timer."""
        if self._timeout <= 0:
            return
        self._timer = self.new_timer()
        self._timer.start()

    def cancel_death_penalty(self):
        """Cancels the timer."""
        if self._timeout <= 0:
            return
        self._timer.cancel()
        self._timer = None
