from json import dumps


class RedisQueue(object):
    def __init__(self, redis) -> None:
        self.redis = redis

    def push_loop(self, producer):
        for results in producer():
            self.push(map(dumps, results))

    def push(self, value_list):
        self.redis.lpush('solr2es:queue', *value_list)


class RedisQueueAsync(object):
    def __init__(self, redis) -> None:
        self.redis = redis

    async def push_loop(self, producer):
        async for results in producer():
            await self.push(list(map(dumps, results)))

    async def push(self, value_list):
        await self.redis.lpush('solr2es:queue', value_list)
