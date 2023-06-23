from django.db import models


class Worker(models.Model):
    """Placeholder model with no database table, but with django admin page
    and contenttype permission"""

    class Meta:
        managed = False  # not in Django's database
        default_permissions = ()
        permissions = [['view', 'Access admin page']]
        verbose_name_plural = " Workers"
