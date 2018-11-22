from json import dumps


class RedisQueue(object):
    def __init__(self, redis) -> None:
        self.redis = redis

    def push(self, producer):
        for results in producer():
            self.redis.lpush('solr2es:queue', *map(dumps, results))


class RedisQueueAsync(object):
    def __init__(self, redis) -> None:
        self.redis = redis

    async def push(self, producer):
        async for results in producer():
            await self.redis.lpush('solr2es:queue', list(map(dumps, results)))
