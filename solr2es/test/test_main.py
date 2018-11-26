import os
import unittest

from solr2es.__main__ import _get_translation_map


class TestMain(unittest.TestCase):
    def test_get_translation_map_as_string(self):
        self.assertEqual({'key': 'value'}, _get_translation_map('{"key": "value"}'))

    def test_get_translation_map_as_none(self):
        self.assertEqual({}, _get_translation_map(None))

    def test_get_translation_map_as_file(self):
        filename = '@' + os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data/translation_map.json')
        self.assertEqual({'field1': {'name': 'value1'}}, _get_translation_map(filename))
