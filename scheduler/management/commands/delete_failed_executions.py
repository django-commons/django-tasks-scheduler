import click
from django.core.management.base import BaseCommand

from scheduler.queues import get_queue
from scheduler.rq_classes import JobExecution


class Command(BaseCommand):
    help = 'Delete failed jobs from Django queue.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--queue', '-q', dest='queue', default='default',
            help='Specify the queue [default]')
        parser.add_argument('-f', '--func', help='optional job function name, e.g. "app.tasks.func"')
        parser.add_argument('--dry-run', action='store_true', help='Do not actually delete failed jobs')

    def handle(self, *args, **options):
        queue = get_queue(options.get('queue', 'default'))
        job_ids = queue.failed_job_registry.get_job_ids()
        jobs = [JobExecution.fetch(job_id, connection=queue.connection) for job_id in job_ids]
        func_name = options.get('func', None)
        if func_name is not None:
            jobs = [job for job in jobs if job.func_name == func_name]
        dry_run = options.get('dry_run', False)
        click.echo(f'Found {len(jobs)} failed jobs')
        for job in jobs:
            click.echo(f'Deleting {job.id}')
            if not dry_run:
                job.delete()
        click.echo(f'Deleted {len(jobs)} failed jobs')
