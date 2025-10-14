from datetime import datetime
from typing import Callable, Any, Tuple, Dict, Type

from django.contrib.contenttypes.fields import GenericForeignKey
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.db import models
from django.utils.translation import gettext_lazy as _

from scheduler.helpers import utils

ARG_TYPE_TYPES_DICT: Dict[str, Type] = {
    "str": str,
    "int": int,
    "bool": bool,
    "datetime": datetime,
    "callable": Callable,
}


class BaseTaskArg(models.Model):
    class ArgType(models.TextChoices):
        STR = "str", _("string")
        INT = "int", _("int")
        BOOL = "bool", _("boolean")
        DATETIME = "datetime", _("datetime")
        CALLABLE = "callable", _("callable")

    arg_type = models.CharField(
        _("Argument Type"),
        max_length=12,
        choices=ArgType.choices,
        default=ArgType.STR,
    )
    val = models.CharField(_("Argument Value"), blank=True, max_length=255)
    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    object_id = models.PositiveIntegerField()
    content_object = GenericForeignKey()

    def clean(self) -> None:
        if self.arg_type not in ARG_TYPE_TYPES_DICT:
            msg = _("Could not parse %s, options are: %s") % (self.arg_type, ARG_TYPE_TYPES_DICT.keys())
            raise ValidationError({"arg_type": ValidationError(msg, code="invalid")})
        try:
            if self.arg_type == "callable":
                utils.callable_func(self.val)
            elif self.arg_type == "datetime":
                datetime.fromisoformat(self.val)
            elif self.arg_type == "bool":
                if self.val.lower() not in {"true", "false"}:
                    raise ValidationError
            elif self.arg_type == "int":
                int(self.val)
        except Exception:
            msg = _("Could not parse %s as %s") % (self.val, self.arg_type)
            raise ValidationError({"arg_type": ValidationError(msg, code="invalid")})

    def save(self, **kwargs: Any) -> None:
        super(BaseTaskArg, self).save(**kwargs)
        self.content_object.save()

    def delete(self, **kwargs: Any) -> None:
        super(BaseTaskArg, self).delete(**kwargs)
        self.content_object.save()

    def value(self) -> Any:
        if self.arg_type == "callable":
            res = utils.callable_func(self.val)()
        elif self.arg_type == "datetime":
            res = datetime.fromisoformat(self.val)
        elif self.arg_type == "bool":
            res = self.val.lower() == "true"
        else:
            res = ARG_TYPE_TYPES_DICT[self.arg_type](self.val)
        return res

    class Meta:
        abstract = True
        ordering = ["id"]


class TaskArg(BaseTaskArg):
    def __str__(self) -> str:
        return f"TaskArg[arg_type={self.arg_type},value={self.value()}]"


class TaskKwarg(BaseTaskArg):
    key = models.CharField(max_length=255)

    def __str__(self) -> str:
        key, value = self.value()
        return f"TaskKwarg[key={key},arg_type={self.arg_type},value={self.val}]"

    def value(self) -> Tuple[str, Any]:
        return self.key, super(TaskKwarg, self).value()
