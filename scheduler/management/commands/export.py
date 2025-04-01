import sys

import click
from django.core.management.base import BaseCommand

from scheduler.models import Task


class Command(BaseCommand):
    """Export all scheduled jobs"""

    help = __doc__

    def add_arguments(self, parser):
        parser.add_argument(
            "-o",
            "--output",
            action="store",
            choices=["json", "yaml"],
            default="json",
            dest="format",
            help="format of output",
        )

        parser.add_argument(
            "-e",
            "--enabled",
            action="store_true",
            dest="enabled",
            help="Export only enabled jobs",
        )
        parser.add_argument(
            "-f",
            "--filename",
            action="store",
            dest="filename",
            help="File name to load (otherwise writes to standard output)",
        )

    def handle(self, *args, **options):
        file = open(options.get("filename"), "w") if options.get("filename") else sys.stdout
        res = list()

        tasks = Task.objects.all()
        if options.get("enabled"):
            tasks = tasks.filter(enabled=True)
        for task in tasks:
            res.append(task.to_dict())

        if options.get("format") == "json":
            import json

            click.echo(json.dumps(res, indent=2, default=str), file=file)
            return

        if options.get("format") == "yaml":
            try:
                import yaml
            except ImportError:
                click.echo("Aborting. LibYAML is not installed.")
                exit(1)
            # Disable YAML alias
            yaml.Dumper.ignore_aliases = lambda *x: True
            click.echo(yaml.dump(res, default_flow_style=False), file=file)
            return
