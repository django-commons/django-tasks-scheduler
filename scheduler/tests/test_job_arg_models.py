from django.core.exceptions import ValidationError
from django.test import TestCase
from django.utils import timezone

from scheduler.models import JobArg, JobKwarg
from .jobs import arg_callable
from .testtools import jobarg_factory


class TestAllJobArg(TestCase):
    JobArgClass = JobArg

    def test_bad_arg_type(self):
        arg = jobarg_factory(self.JobArgClass, arg_type='bad_arg_type', val='something')
        with self.assertRaises(ValidationError):
            arg.clean()

    def test_clean_one_value_invalid_str_int(self):
        arg = jobarg_factory(self.JobArgClass, arg_type='int', val='not blank', )
        with self.assertRaises(ValidationError):
            arg.clean()

    def test_clean_callable_invalid(self):
        arg = jobarg_factory(self.JobArgClass, arg_type='callable', val='bad_callable', )
        with self.assertRaises(ValidationError):
            arg.clean()

    def test_clean_datetime_invalid(self):
        arg = jobarg_factory(self.JobArgClass, arg_type='datetime', val='bad datetime', )
        with self.assertRaises(ValidationError):
            arg.clean()

    def test_clean_bool_invalid(self):
        arg = jobarg_factory(self.JobArgClass, arg_type='bool', val='bad bool', )
        with self.assertRaises(ValidationError):
            arg.clean()

    def test_clean_int_invalid(self):
        arg = jobarg_factory(self.JobArgClass, arg_type='int', val='str')
        with self.assertRaises(ValidationError):
            arg.clean()

    def test_str_clean(self):
        arg = jobarg_factory(self.JobArgClass, val='something')
        self.assertIsNone(arg.clean())


class TestJobArg(TestCase):
    JobArgClass = JobArg

    def test_str(self):
        arg = jobarg_factory(self.JobArgClass)
        self.assertEqual(
            f'JobArg[arg_type={arg.arg_type},value={arg.value()}]', str(arg))

    def test_value(self):
        arg = jobarg_factory(self.JobArgClass, arg_type='str', val='something')
        self.assertEqual(arg.value(), 'something')

    def test__str__str_val(self):
        arg = jobarg_factory(self.JobArgClass, arg_type='str', val='something')
        self.assertEqual('something', str(arg.value()))

    def test__str__int_val(self):
        arg = jobarg_factory(self.JobArgClass, arg_type='int', val='1')
        self.assertEqual('1', str(arg.value()))

    def test__str__datetime_val(self):
        _time = timezone.now()
        arg = jobarg_factory(self.JobArgClass, arg_type='datetime', val=str(_time))
        self.assertEqual(str(_time), str(arg.value()))

    def test__str__bool_val(self):
        arg = jobarg_factory(self.JobArgClass, arg_type='bool', val='True')
        self.assertEqual('True', str(arg.value()))

    def test__repr__str_val(self):
        arg = jobarg_factory(self.JobArgClass, arg_type='str', val='something')
        self.assertEqual("'something'", repr(arg.value()))

    def test__repr__int_val(self):
        arg = jobarg_factory(self.JobArgClass, arg_type='int', val='1')
        self.assertEqual('1', repr(arg.value()))

    def test__repr__datetime_val(self):
        _time = timezone.now()
        arg = jobarg_factory(self.JobArgClass, arg_type='datetime', val=str(_time))
        self.assertEqual(repr(_time), repr(arg.value()))

    def test__repr__bool_val(self):
        arg = jobarg_factory(self.JobArgClass, arg_type='bool', val='False')
        self.assertEqual('False', repr(arg.value()))

    def test_callable_arg_type__clean(self):
        method = arg_callable
        arg = jobarg_factory(
            self.JobArgClass, arg_type='callable',
            val=f'{method.__module__}.{method.__name__}', )
        self.assertIsNone(arg.clean())
        self.assertEqual(1, arg.value())
        self.assertEqual(2, arg.value())


class TestJobKwarg(TestAllJobArg):
    JobArgClass = JobKwarg

    def test_str(self):
        arg = jobarg_factory(self.JobArgClass)
        self.assertEqual(
            f'JobKwarg[key={arg.key},arg_type={arg.arg_type},value={arg.val}]', str(arg))

    def test_value(self):
        kwarg = jobarg_factory(self.JobArgClass, key='key', arg_type='str', val='value')
        self.assertEqual(kwarg.value(), ('key', 'value'))

    def test__str__str_val(self):
        kwarg = jobarg_factory(self.JobArgClass, key='key', arg_type='str', val='something')
        self.assertEqual('JobKwarg[key=key,arg_type=str,value=something]', str(kwarg))

    def test__str__int_val(self):
        kwarg = jobarg_factory(self.JobArgClass, key='key', arg_type='int', val=1)
        self.assertEqual('JobKwarg[key=key,arg_type=int,value=1]', str(kwarg))

    def test__str__datetime_val(self):
        _time = timezone.now()
        kwarg = jobarg_factory(self.JobArgClass, key='key', arg_type='datetime', val=str(_time))
        self.assertEqual(f'JobKwarg[key=key,arg_type=datetime,value={_time}]', str(kwarg))

    def test__str__bool_val(self):
        kwarg = jobarg_factory(self.JobArgClass, key='key', arg_type='bool', val='True')
        self.assertEqual('JobKwarg[key=key,arg_type=bool,value=True]', str(kwarg))

    def test__repr__str_val(self):
        kwarg = jobarg_factory(self.JobArgClass, key='key', arg_type='str', val='something')
        self.assertEqual("('key', 'something')", repr(kwarg.value()))

    def test__repr__int_val(self):
        kwarg = jobarg_factory(self.JobArgClass, key='key', arg_type='int', val='1')
        self.assertEqual("('key', 1)", repr(kwarg.value()))

    def test__repr__datetime_val(self):
        _time = timezone.now()
        kwarg = jobarg_factory(self.JobArgClass, key='key', arg_type='datetime', val=str(_time))
        self.assertEqual("('key', {})".format(repr(_time)), repr(kwarg.value()))

    def test__repr__bool_val(self):
        kwarg = jobarg_factory(self.JobArgClass, key='key', arg_type='bool', val='True')
        self.assertEqual("('key', True)", repr(kwarg.value()))
