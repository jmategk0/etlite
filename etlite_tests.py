import unittest
import etlite
from decimal import Decimal
# import time
# from pprint import pprint


class BaseTestCaseData(unittest.TestCase):
    pass


class TestListFunctions(unittest.TestCase):

    def setUp(self):
        self.test_floats_as_strings = ["0.45", "0.50", "0.80"]
        self.test_ints_as_strings = ["1", "2", "3"]
        self.test_bools_as_strings = ["True", "False", "true", "false"]
        self.test_strings_as_ints = [1, 2, 3, 4, 5, 6]

    def test_list_to_floats(self):
        my_floats = etlite.list_to_floats(list_of_values=self.test_floats_as_strings, in_place=False)

        for value in my_floats:
            self.assertEqual(type(value), float)

    def test_list_to_ints(self):
        my_ints = etlite.list_to_integers(list_of_values=self.test_ints_as_strings, in_place=False)

        for value in my_ints:
            self.assertEqual(type(value), int)

    def test_list_to_dec(self):
        my_decimal = etlite.list_to_decimals(list_of_values=self.test_floats_as_strings, in_place=False)

        for value in my_decimal:
            self.assertEqual(type(value), Decimal)

    def test_list_to_bool(self):
        my_booleans = etlite.list_to_booleans(list_of_values=self.test_bools_as_strings, in_place=False)

        for value in my_booleans:
            self.assertEqual(type(value), bool)

    def test_list_to_strings(self):
        my_strings = etlite.list_to_strings(list_of_values=self.test_strings_as_ints, in_place=False)

        for value in my_strings:
            self.assertEqual(type(value), str)


class TestDataSchema(unittest.TestCase):

    def setUp(self):
        self.test_data = [
            {"fname": "John", "lname": "Doe", "dob": "1960-01-01", "amount": "100", "status": 0},
            {"fname": "Jane", "lname": "Doe", "dob": "1961-01-01", "amount": "200", "status": 0},
            {"fname": "Jamie", "lname": "Doe", "dob": "1962-01-01", "amount": "300", "status": 0}]
        self.minimal_data = {"fname": str, "lname": str, "dob": str, "amount": float, "status": str}
        self.schema = {
            'fname': {
                'name': 'fname',
                'data_type': str,
                'default': '',
                'null': True,
                'blank': False,
                'unique': False,
                'read_only': False,
                'required': False,
                'label': 'fname',
                'choices': None,
                'max_length': 256},
            'lname': {
                'name': 'lname',
                'data_type': str,
                'default': '',
                'null': True,
                'blank': False,
                'unique': False,
                'read_only': False,
                'required': False,
                'label': 'lname',
                'choices': None,
                'max_length': 256},
            'dob': {
                'name': 'dob',
                'data_type': str,
                'default': '',
                'null': True,
                'blank': False,
                'unique': False,
                'read_only': False,
                'required': False,
                'label': 'dob',
                'choices': None,
                'max_length': 256},
            'amount': {
                'name': 'amount',
                'data_type': float,
                'default': '',
                'null': True,
                'blank': False,
                'unique': False,
                'read_only': False,
                'required': False,
                'label': 'amount',
                'choices': None,
                'decimal_places': 2,
                'max_digits': 14,
                'min_value': 0,
                'max_value': 0},
            'status': {
                'name': 'status',
                'data_type': str,
                'default': '',
                'null': True,
                'blank': False,
                'unique': False,
                'read_only': False,
                'required': False,
                'label': 'status',
                'choices': None,
                'max_length': 256}}

    def test_schema_definition_with_minimal_data(self):
        schema = etlite.DataSchema()
        schema.define_fields(self.minimal_data, is_minimal_data=True)

        self.assertEqual(self.schema, schema.field_names)

    def test_schema_definition_with_full_data(self):
        schema = etlite.DataSchema()
        schema.define_fields(self.schema, is_minimal_data=False)

        self.assertEqual(self.schema, schema.field_names)


class TestETL(unittest.TestCase):

    def setUp(self):
        self.test_data = [
            {"fname": "John", "lname": "Doe", "dob": "1960-01-01", "amount": "100", "status": 0},
            {"fname": "Jane", "lname": "Doe", "dob": "1961-01-01", "amount": "200", "status": 0},
            {"fname": "Jamie", "lname": "Doe", "dob": "1962-01-01", "amount": "300", "status": 0}]
