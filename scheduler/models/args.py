import json
from collections.abc import Callable
from datetime import datetime
from typing import Any

from django.contrib.contenttypes.fields import GenericForeignKey
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.db import models
from django.utils.translation import gettext_lazy as _

from scheduler.helpers import utils

ARG_TYPE_TYPES_DICT: dict[str, type] = {
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "datetime": datetime,
    "json": object,  # any JSON value: dict, list, str, number, bool or None
    "callable": Callable,
}


class BaseTaskArg(models.Model):
    class ArgType(models.TextChoices):
        STR = "str", _("string")
        INT = "int", _("int")
        FLOAT = "float", _("float")
        BOOL = "bool", _("boolean")
        DATETIME = "datetime", _("datetime")
        JSON = "json", _("JSON")
        CALLABLE = "callable", _("callable")

    arg_type = models.CharField(
        _("Argument Type"),
        max_length=12,
        choices=ArgType.choices,
        default=ArgType.STR,
    )
    val = models.CharField(_("Argument Value"), blank=True, max_length=2048)
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
            elif self.arg_type == "float":
                float(self.val)
            elif self.arg_type == "json":
                json.loads(self.val)
        except Exception:
            msg = _("Could not parse %s as %s") % (self.val, self.arg_type)
            raise ValidationError({"arg_type": ValidationError(msg, code="invalid")})

    def value(self) -> Any:
        """The argument to pass to the task. For a callable argument this calls the callable."""
        if self.arg_type == "callable":
            return utils.callable_func(self.val)()
        return self._parsed_val()

    def display_value(self) -> str:
        """A representation of the argument for display. Unlike `value()`, it never calls a callable argument."""
        if self.arg_type == "callable":
            return f"{self.val}()"
        return repr(self._parsed_val())

    def _parsed_val(self) -> Any:
        if self.arg_type == "datetime":
            return datetime.fromisoformat(self.val)
        if self.arg_type == "bool":
            return self.val.lower() == "true"
        if self.arg_type == "json":
            return json.loads(self.val)
        return ARG_TYPE_TYPES_DICT[self.arg_type](self.val)

    class Meta:
        abstract = True
        ordering = ["id"]


class TaskArg(BaseTaskArg):
    def __str__(self) -> str:
        # `val`, not `value()`: logging or inspecting an argument must not call a callable argument.
        return f"TaskArg[arg_type={self.arg_type},value={self.val}]"


class TaskKwarg(BaseTaskArg):
    key = models.CharField(max_length=255)

    def __str__(self) -> str:
        return f"TaskKwarg[key={self.key},arg_type={self.arg_type},value={self.val}]"

    def value(self) -> tuple[str, Any]:
        return self.key, super().value()
