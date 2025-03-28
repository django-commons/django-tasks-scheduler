# Generated by Django 5.1.7 on 2025-03-24 14:30

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('scheduler', '0020_remove_repeatabletask_new_task_id_and_more'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='task',
            name='job_id',
        ),
        migrations.AddField(
            model_name='task',
            name='job_name',
            field=models.CharField(blank=True, editable=False, help_text='Current job_name on queue', max_length=128, null=True, verbose_name='job name'),
        ),
    ]
