import importlib
import unittest

try:
    import sentry_sdk  # noqa: F401

    SENTRY_SDK_INSTALLED = True
except ImportError:
    SENTRY_SDK_INSTALLED = False


@unittest.skipUnless(SENTRY_SDK_INSTALLED, "sentry-sdk is not installed (it is the optional `sentry` extra)")
class SentryIntegrationImportTest(unittest.TestCase):
    def test_the_integration_module_can_be_imported(self) -> None:
        """`register_sentry` swallows ImportError, so an unimportable integration is invisible at runtime.

        It pointed at `scheduler.timeouts`, a module that does not exist, so every `scheduler_worker
        --sentry-dsn` run reported "Sentry SDK not installed" and sent nothing, SDK installed or not.
        """
        module = importlib.import_module("scheduler.helpers.sentry_integration")

        self.assertTrue(hasattr(module, "SentryIntegration"))
