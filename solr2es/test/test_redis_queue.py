import unittest
from json import loads

import redis as redis

from solr2es.redis_queue import RedisQueue


class TestRedisQueue(unittest.TestCase):

    def setUp(self):
        self.redis = redis.Redis(host='redis')

    def tearDown(self):
        self.redis.delete('solr2es:queue')

    def test_consume(self):
        def producer():
            yield [{'foo': 'bar'}, {'toot': 'toot'}]
            yield [{'baz': 'qux'}]

        RedisQueue(self.redis).push(producer)

        self.assertEqual(3, self.redis.llen('solr2es:queue'))
        self.assertEqual({'baz': 'qux'}, loads(self.redis.lpop('solr2es:queue')))
        self.assertEqual({'toot': 'toot'}, loads(self.redis.lpop('solr2es:queue')))
        self.assertEqual({'foo': 'bar'}, loads(self.redis.lpop('solr2es:queue')))
