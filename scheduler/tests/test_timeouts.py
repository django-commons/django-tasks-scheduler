import time

from django.test import SimpleTestCase

from scheduler.helpers.timeouts import JobTimeoutException, TimerDeathPenalty


class TestTimerDeathPenalty(SimpleTestCase):
    def test_exception_message_is_per_timeout(self):
        short, long = TimerDeathPenalty(1), TimerDeathPenalty(60)

        self.assertEqual("Task exceeded maximum timeout value (1 seconds)", str(short._exception()))
        self.assertEqual("Task exceeded maximum timeout value (60 seconds)", str(long._exception()))

    def test_exception_class_is_left_untouched(self):
        TimerDeathPenalty(1)

        self.assertEqual("custom", str(JobTimeoutException("custom")))

    def test_timeout__raises_the_exception_class_in_the_timed_thread(self):
        with self.assertRaises(JobTimeoutException) as cm, TimerDeathPenalty(1):
            # The exception is delivered between bytecodes, so wait in short sleeps rather than one long one.
            for _ in range(50):
                time.sleep(0.1)

        self.assertEqual("Task exceeded maximum timeout value (1 seconds)", str(cm.exception))
