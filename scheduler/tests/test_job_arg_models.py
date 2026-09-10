from unittest.mock import patch

from django.core.exceptions import ValidationError
from django.test import TestCase
from django.utils import timezone

from scheduler.models import TaskArg, TaskKwarg, TaskType
from scheduler.tests.testtools import task_factory, taskarg_factory

from .jobs import arg_callable


class TestAllTaskArg(TestCase):
    TaskArgClass = TaskArg

    def test_bad_arg_type(self):
        arg = taskarg_factory(self.TaskArgClass, arg_type="bad_arg_type", val="something")
        with self.assertRaises(ValidationError):
            arg.clean()

    def test_clean_one_value_invalid_str_int(self):
        arg = taskarg_factory(
            self.TaskArgClass,
            arg_type="int",
            val="not blank",
        )
        with self.assertRaises(ValidationError):
            arg.clean()

    def test_clean_callable_invalid(self):
        arg = taskarg_factory(
            self.TaskArgClass,
            arg_type="callable",
            val="bad_callable",
        )
        with self.assertRaises(ValidationError):
            arg.clean()

    def test_clean_datetime_invalid(self):
        arg = taskarg_factory(
            self.TaskArgClass,
            arg_type="datetime",
            val="bad datetime",
        )
        with self.assertRaises(ValidationError):
            arg.clean()

    def test_clean_bool_invalid(self):
        arg = taskarg_factory(
            self.TaskArgClass,
            arg_type="bool",
            val="bad bool",
        )
        with self.assertRaises(ValidationError):
            arg.clean()

    def test_clean_int_invalid(self):
        arg = taskarg_factory(self.TaskArgClass, arg_type="int", val="str")
        with self.assertRaises(ValidationError):
            arg.clean()

    def test_str_clean(self):
        arg = taskarg_factory(self.TaskArgClass, val="something")
        self.assertIsNone(arg.clean())

    def test_save_and_delete_do_not_cascade_to_content_object(self):
        task = task_factory(TaskType.ONCE)
        kwargs = {"key": "k1"} if self.TaskArgClass == TaskKwarg else {}
        with patch.object(task, "save") as mock_save:
            arg = taskarg_factory(self.TaskArgClass, content_object=task, val="val1", **kwargs)
            mock_save.assert_not_called()
            arg.val = "val2"
            arg.save()
            mock_save.assert_not_called()
            arg.delete()
            mock_save.assert_not_called()

    def test_str__callable_arg__does_not_call_it(self):
        kwargs = {"key": "k1"} if self.TaskArgClass == TaskKwarg else {}
        arg = taskarg_factory(self.TaskArgClass, arg_type="callable", val="scheduler.tests.jobs.arg_callable", **kwargs)

        with patch("scheduler.tests.jobs.arg_callable") as arg_callable:
            str(arg)
            self.assertEqual("scheduler.tests.jobs.arg_callable()", arg.display_value())

        arg_callable.assert_not_called()


class TestTaskArg(TestCase):
    TaskArgClass = TaskArg

    def test_str(self):
        arg = taskarg_factory(self.TaskArgClass)
        self.assertEqual(f"TaskArg[arg_type={arg.arg_type},value={arg.value()}]", str(arg))

    def test_value(self):
        arg = taskarg_factory(self.TaskArgClass, arg_type="str", val="something")
        self.assertEqual(arg.value(), "something")

    def test__str__str_val(self):
        arg = taskarg_factory(self.TaskArgClass, arg_type="str", val="something")
        self.assertEqual("something", str(arg.value()))

    def test__str__int_val(self):
        arg = taskarg_factory(self.TaskArgClass, arg_type="int", val="1")
        self.assertEqual("1", str(arg.value()))

    def test__str__datetime_val(self):
        _time = timezone.now()
        arg = taskarg_factory(self.TaskArgClass, arg_type="datetime", val=str(_time))
        self.assertEqual(str(_time), str(arg.value()))

    def test__str__bool_val(self):
        arg = taskarg_factory(self.TaskArgClass, arg_type="bool", val="True")
        self.assertEqual("True", str(arg.value()))

    def test__repr__str_val(self):
        arg = taskarg_factory(self.TaskArgClass, arg_type="str", val="something")
        self.assertEqual("'something'", repr(arg.value()))

    def test__repr__int_val(self):
        arg = taskarg_factory(self.TaskArgClass, arg_type="int", val="1")
        self.assertEqual("1", repr(arg.value()))

    def test__repr__datetime_val(self):
        _time = timezone.now()
        arg = taskarg_factory(self.TaskArgClass, arg_type="datetime", val=str(_time))
        self.assertEqual(repr(_time), repr(arg.value()))

    def test__repr__bool_val(self):
        arg = taskarg_factory(self.TaskArgClass, arg_type="bool", val="False")
        self.assertEqual("False", repr(arg.value()))

    def test_callable_arg_type__clean(self):
        method = arg_callable
        arg = taskarg_factory(
            self.TaskArgClass,
            arg_type="callable",
            val=f"{method.__module__}.{method.__name__}",
        )
        self.assertIsNone(arg.clean())
        self.assertEqual(1, arg.value())
        self.assertEqual(2, arg.value())

    def test_value__float(self):
        arg = taskarg_factory(self.TaskArgClass, arg_type="float", val="1.5")

        self.assertIsNone(arg.clean())
        self.assertEqual(1.5, arg.value())

    def test_value__json(self):
        arg = taskarg_factory(self.TaskArgClass, arg_type="json", val='{"ids": [1, 2], "dry_run": null}')

        self.assertIsNone(arg.clean())
        self.assertEqual({"ids": [1, 2], "dry_run": None}, arg.value())
        self.assertEqual("{'ids': [1, 2], 'dry_run': None}", arg.display_value())

    def test_clean__invalid_float_or_json(self):
        for arg_type, val in (("float", "one"), ("json", "{not json")):
            arg = taskarg_factory(self.TaskArgClass, arg_type=arg_type, val=val)
            with self.subTest(arg_type=arg_type), self.assertRaises(ValidationError):
                arg.clean()


class TestTaskKwarg(TestAllTaskArg):
    TaskArgClass = TaskKwarg

    def test_str(self):
        arg = taskarg_factory(self.TaskArgClass)
        self.assertEqual(f"TaskKwarg[key={arg.key},arg_type={arg.arg_type},value={arg.val}]", str(arg))

    def test_value(self):
        kwarg = taskarg_factory(self.TaskArgClass, key="key", arg_type="str", val="value")
        self.assertEqual(kwarg.value(), ("key", "value"))

    def test__str__str_val(self):
        kwarg = taskarg_factory(self.TaskArgClass, key="key", arg_type="str", val="something")
        self.assertEqual("TaskKwarg[key=key,arg_type=str,value=something]", str(kwarg))

    def test__str__int_val(self):
        kwarg = taskarg_factory(self.TaskArgClass, key="key", arg_type="int", val=1)
        self.assertEqual("TaskKwarg[key=key,arg_type=int,value=1]", str(kwarg))

    def test__str__datetime_val(self):
        _time = timezone.now()
        kwarg = taskarg_factory(self.TaskArgClass, key="key", arg_type="datetime", val=str(_time))
        self.assertEqual(f"TaskKwarg[key=key,arg_type=datetime,value={_time}]", str(kwarg))

    def test__str__bool_val(self):
        kwarg = taskarg_factory(self.TaskArgClass, key="key", arg_type="bool", val="True")
        self.assertEqual("TaskKwarg[key=key,arg_type=bool,value=True]", str(kwarg))

    def test__repr__str_val(self):
        kwarg = taskarg_factory(self.TaskArgClass, key="key", arg_type="str", val="something")
        self.assertEqual("('key', 'something')", repr(kwarg.value()))

    def test__repr__int_val(self):
        kwarg = taskarg_factory(self.TaskArgClass, key="key", arg_type="int", val="1")
        self.assertEqual("('key', 1)", repr(kwarg.value()))

    def test__repr__datetime_val(self):
        _time = timezone.now()
        kwarg = taskarg_factory(self.TaskArgClass, key="key", arg_type="datetime", val=str(_time))
        self.assertEqual(f"('key', {_time!r})", repr(kwarg.value()))

    def test__repr__bool_val(self):
        kwarg = taskarg_factory(self.TaskArgClass, key="key", arg_type="bool", val="True")
        self.assertEqual("('key', True)", repr(kwarg.value()))
