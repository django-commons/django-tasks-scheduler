"""Django database connection handling for the worker.

The worker runs each job in a forked child, and a forked child must not inherit an open
Django DB connection. Parent and child would hold the same socket, and a TLS-wrapped
stream - the default on a managed Postgres or MySQL - breaks as soon as both use it: the
first child's queries advance the TLS stream state server-side, and every later child
starts from the parent's now-stale in-memory TLS state and dies on its first query with
an opaque transport error such as
``consuming input failed: SSL error: unexpected eof while reading``.

``scheduler_worker`` closes connections once at startup for this reason, but any ORM call
the worker makes afterwards opens a new one - notably the failure callbacks that
``Queue.clean_registries`` runs for abandoned jobs, which read and save ``Task`` rows in
the worker process itself. Nothing closes that connection again, so one maintenance pass
can leave every job that follows failing until the worker is restarted.

Django's connection handler is thread-local, so this closes the calling thread's
connections and leaves the scheduler thread's own connection alone. Closing is cheap:
whichever process needs the database next opens a fresh connection.
"""

from django.db import connections

from scheduler.settings import logger


def close_db_connections() -> None:
    """Close the calling thread's Django DB connections.

    ``connections.close_all()`` does the same loop but stops at the first connection that
    raises, and closing a connection whose socket is already broken can raise - which is
    one of the states this exists to clear. A connection we cannot close must not stop the
    remaining ones closing, nor stop the caller forking.
    """
    for connection in connections.all(initialized_only=True):
        try:
            connection.close()
        except Exception:
            # A connection we cannot close must not stop the caller forking.
            logger.warning(f"Could not close DB connection {connection.alias}", exc_info=True)
