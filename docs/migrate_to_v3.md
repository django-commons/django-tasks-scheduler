Migration from v2 to v3
=======================

Version 3.0.0 introduced a major design change. Instead of three separate models, there is one new `Task` model. The
goal is to have one centralized admin view for all your scheduled tasks, regardless of the scheduling type.

You need to migrate the scheduled tasks using the old models (`ScheduledTask`, `RepeatableTask`, `CronTask`) to the new
model. It can be done using the export/import commands provided.

After upgrading to django-tasks-scheduler v3.0.0, you will notice you are not able to create new scheduled tasks in the
old models, that is intentional. In the next version of django-tasks-scheduler (v3.1), the old models will be deleted,
so make sure you migrate your old models.

!!! Note
    While we tested different scenarios heavily and left the code for old tasks, we could not account for all different
    use cases, therefore, please [open an issue][issues] if you encounter any.

There are two ways to migrate your existing scheduled tasks:

# Using the admin views of the old models

If you go to the admin view of the old models, you will notice there is a new action in the actions drop down menu for
migrating the selected tasks. Use it, and you will also have a link to the new task to compare the migration result.

Note once you migrate using this method, the old task will be disabled automatically.

# Export/Import management commands

Run in your project directory:

```shell
python manage.py export > scheduled_tasks.json
python manage.py import --filename scheduled_tasks.json
```

[issues]: https://github.com/django-commons/django-tasks-scheduler/issues
