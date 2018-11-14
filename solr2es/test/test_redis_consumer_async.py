from json import loads

import asyncio_redis
import asynctest

from solr2es.solr2es import RedisConsumerAsync


class TestRedisConsumer(asynctest.TestCase):

    async def setUp(self):
        self.redis = await asyncio_redis.Pool.create(host='redis', port=6379, poolsize=10)

    async def tearDown(self):
        await self.redis.delete(['solr2es:queue'])
        self.redis.close()

    async def test_consume(self):
        async def producer():
            yield [{'foo': 'bar'}, {'toot': 'toot'}]
            yield [{'baz': 'qux'}]

        await RedisConsumerAsync(self.redis).consume(producer)

        self.assertEqual(3, await self.redis.llen('solr2es:queue'))
        self.assertEqual({'baz': 'qux'}, loads(await self.redis.lpop('solr2es:queue')))
        self.assertEqual({'toot': 'toot'}, loads(await self.redis.lpop('solr2es:queue')))
        self.assertEqual({'foo': 'bar'}, loads(await self.redis.lpop('solr2es:queue')))
