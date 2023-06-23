import time

import click
from django.core.management.base import BaseCommand
from scheduler.views import get_statistics

ANSI_LIGHT_GREEN = "\033[1;32m"
ANSI_LIGHT_WHITE = "\033[1;37m"
ANSI_RESET = "\033[0m"

KEYS = ('jobs', 'started_jobs', 'deferred_jobs', 'finished_jobs', 'canceled_jobs', 'workers')


class Command(BaseCommand):
    """
    Print statistics
    """
    help = __doc__

    def __init__(self, *args, **kwargs):
        super(Command, self).__init__(*args, **kwargs)
        self.table_width = 80
        self.interval = None

    def add_arguments(self, parser):
        parser.add_argument(
            '-j', '--json', action='store_true', dest='json',
            help='Output statistics as JSON', )

        parser.add_argument(
            '-y', '--yaml', action='store_true', dest='yaml',
            help='Output statistics as YAML',
        )

        parser.add_argument(
            '-i', '--interval', dest='interval', type=float,
            help='Poll statistics every N seconds',
        )

    def _print_separator(self):
        click.echo('-' * self.table_width)

    def _print_stats_dashboard(self, statistics, prev_stats=None):
        if self.interval:
            click.clear()
        click.echo()
        click.echo("Django-Scheduler CLI Dashboard")
        click.echo()
        self._print_separator()
        click.echo(f'| {"Name":<16} |    Queued |    Active |  Deferred |'
                   f'  Finished |'
                   f'  Canceled |'
                   f'   Workers |')
        self._print_separator()
        for ind, queue in enumerate(statistics["queues"]):
            vals = list((queue[k] for k in KEYS))
            # Deal with colors
            if prev_stats and len(prev_stats['queues']) > ind:
                prev = prev_stats["queues"][ind]
                prev_vals = (prev[k] for k in KEYS)
                colors = [ANSI_LIGHT_GREEN
                          if vals[i] != prev_vals[i] else ANSI_LIGHT_WHITE
                          for i in range(len(prev_vals))
                          ]
            else:
                colors = [ANSI_LIGHT_WHITE for _ in range(len(vals))]
            to_print = ' | '.join([f'{colors[i]}{vals[i]:9}{ANSI_RESET}' for i in range(len(vals))])
            click.echo(f'| {queue["name"]:<16} | {to_print} |', color=True)

        self._print_separator()

        if self.interval:
            click.echo()
            click.echo("Press 'Ctrl+c' to quit")

    def handle(self, *args, **options):

        if options.get("json"):
            import json
            click.secho(json.dumps(get_statistics(), indent=2), )
            return

        if options.get("yaml"):
            try:
                import yaml
            except ImportError:
                click.secho("Aborting. yaml not supported", err=True, fg='red')
                return

            click.secho(yaml.dump(get_statistics(), default_flow_style=False), )
            return

        self.interval = options.get("interval")

        if not self.interval or self.interval < 0:
            self._print_stats_dashboard(get_statistics())
            return

        try:
            prev = None
            while True:
                statistics = get_statistics()
                self._print_stats_dashboard(statistics, prev)
                prev = statistics
                time.sleep(self.interval)
        except KeyboardInterrupt:
            pass
