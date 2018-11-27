import os
import unittest

from solr2es.__main__ import _get_dict_from_string_or_file


class TestMain(unittest.TestCase):
    def test_get_dict_from_string(self):
        self.assertEqual({'key': 'value'}, _get_dict_from_string_or_file('{"key": "value"}'))

    def test_get_translation_map_when_input_is_none(self):
        self.assertEqual({}, _get_dict_from_string_or_file(None))

    def test_get_dict_from_file(self):
        filename = '@' + os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data/translation_map.json')
        self.assertEqual({'field1': {'name': 'value1'}}, _get_dict_from_string_or_file(filename))
