import pprint
import textwrap


class ClassNotFound(KeyError):
    pass


class MethodNotFound(KeyError):
    pass


class ResourceNotFound(KeyError):
    pass


class HandlerNotFound(KeyError):
    pass


class ExtensionHandlerNotFound(KeyError):
    pass


class ValidationError(ValueError):
    pass


class SchemaTypeMismatchValidationError(ValidationError):
    msg = 'schema type mismatch: expected={expected} actual={actual}'

    def __init__(self, expected, actual):
        self.expected = expected
        self.actual = actual
        super().__init__()

    def __str__(self):
        return self.msg.format(expected=self.expected, actual=self.actual)


class DataFrameValidationError(ValidationError):
    def __init__(self, missing_columns, type_mismatches):
        self.missing_columns = missing_columns
        self.type_mismatches = type_mismatches
        super().__init__()

    def __str__(self):
        desc = textwrap.dedent(f'''\
        ValidationError
        missing_columns: {repr(self.missing_columns)}
        type_mismatches:
        {pprint.pformat(self.type_mismatches, width=20)}
        ''')
        return desc