import unittest
from json import loads

import redis as redis

from solr2es.solr2es import RedisConsumer


class TestRedisConsumer(unittest.TestCase):

    def setUp(self):
        self.redis = redis.Redis(host='redis')

    def tearDown(self):
        self.redis.delete('solr2es:queue')

    def test_consume(self):
        def producer():
            yield [{'foo': 'bar'}, {'toot': 'toot'}]
            yield [{'baz': 'qux'}]

        RedisConsumer(self.redis).consume(producer)

        self.assertEqual(3, self.redis.llen('solr2es:queue'))
        self.assertEqual({'baz': 'qux'}, loads(self.redis.lpop('solr2es:queue')))
        self.assertEqual({'toot': 'toot'}, loads(self.redis.lpop('solr2es:queue')))
        self.assertEqual({'foo': 'bar'}, loads(self.redis.lpop('solr2es:queue')))
