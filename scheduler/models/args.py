from datetime import datetime
from typing import Callable

from django.contrib.contenttypes.fields import GenericForeignKey
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.db import models
from django.utils.translation import gettext_lazy as _

from scheduler import tools

ARG_TYPE_TYPES_DICT = {
    'str': str,
    'int': int,
    'bool': bool,
    'datetime': datetime,
    'callable': Callable,
}


class BaseJobArg(models.Model):
    class ArgType(models.TextChoices):
        STR = 'str', _('string')
        INT = 'int', _('int')
        BOOL = 'bool', _('boolean')
        DATETIME = 'datetime', _('datetime')
        CALLABLE = 'callable', _('callable')

    arg_type = models.CharField(
        _('Argument Type'), max_length=12, choices=ArgType.choices, default=ArgType.STR,
    )
    val = models.CharField(_('Argument Value'), blank=True, max_length=255)
    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    object_id = models.PositiveIntegerField()
    content_object = GenericForeignKey()

    def clean(self):
        if self.arg_type not in ARG_TYPE_TYPES_DICT:
            raise ValidationError({
                'arg_type': ValidationError(
                    _(f'Could not parse {self.arg_type}, options are: {ARG_TYPE_TYPES_DICT.keys()}'), code='invalid')
            })
        try:
            if self.arg_type == 'callable':
                tools.callable_func(self.val)
            elif self.arg_type == 'datetime':
                datetime.fromisoformat(self.val)
            elif self.arg_type == 'bool':
                if self.val.lower() not in {'true', 'false'}:
                    raise ValidationError
            elif self.arg_type == 'int':
                int(self.val)
        except Exception:
            raise ValidationError({
                'arg_type': ValidationError(
                    _(f'Could not parse {self.val} as {self.arg_type}'), code='invalid')
            })

    def save(self, **kwargs):
        super(BaseJobArg, self).save(**kwargs)
        self.content_object.save()

    def delete(self, **kwargs):
        super(BaseJobArg, self).delete(**kwargs)
        self.content_object.save()

    def value(self):
        if self.arg_type == 'callable':
            res = tools.callable_func(self.val)()
        elif self.arg_type == 'datetime':
            res = datetime.fromisoformat(self.val)
        elif self.arg_type == 'bool':
            res = self.val.lower() == 'true'
        else:
            res = ARG_TYPE_TYPES_DICT[self.arg_type](self.val)
        return res

    class Meta:
        abstract = True
        ordering = ['id']


class JobArg(BaseJobArg):
    def __str__(self):
        return f'JobArg[arg_type={self.arg_type},value={self.value()}]'


class JobKwarg(BaseJobArg):
    key = models.CharField(max_length=255)

    def __str__(self):
        key, value = self.value()
        return f'JobKwarg[key={key},arg_type={self.arg_type},value={self.val}]'

    def value(self):
        return self.key, super(JobKwarg, self).value()
