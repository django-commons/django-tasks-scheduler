import click
from django.core.management.base import BaseCommand

from scheduler.queues import get_queue


class Command(BaseCommand):
    """
    Queues the function given with the first argument with the
    parameters given with the rest of the argument list.
    """
    help = __doc__
    args = '<function arg arg ...>'

    def add_arguments(self, parser):
        parser.add_argument(
            '--queue', '-q', dest='queue', default='default',
            help='Specify the queue [default]')
        parser.add_argument(
            '--timeout', '-t', type=int, dest='timeout',
            help='A timeout in seconds')
        parser.add_argument(
            '--result-ttl', '-r', type=int, dest='result_ttl',
            help='Time to store job results in seconds')
        parser.add_argument('callable', help='Method to call', )
        parser.add_argument('args', nargs='*', help='Args for callable')

    def handle(self, **options):
        verbosity = int(options.get('verbosity', 1))
        timeout = options.get('timeout')
        result_ttl = options.get('result_ttl')
        queue = get_queue(options.get('queue'))
        func = options.get('callable')
        args = options.get('args')
        job = queue.enqueue_call(func, args=args, timeout=timeout, result_ttl=result_ttl)
        if verbosity:
            click.echo(f'Job {job.id} created')
