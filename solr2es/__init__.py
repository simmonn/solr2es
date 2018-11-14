import itertools
from json import loads, dumps

DEFAULT_ES_DOC_TYPE = 'doc'


class Solr2Es(object):
    def __init__(self, solr, es, refresh=False) -> None:
        super().__init__()
        self.solr = solr
        self.es = es
        self.refresh = refresh

    def migrate(self, index_name) -> int:
        nb_results = 0
        for results in self.produce_results():
            actions = create_es_actions(index_name, results)
            errors = self.es.bulk(actions, index_name, DEFAULT_ES_DOC_TYPE, refresh=self.refresh)
            nb_results += len(results)
        return nb_results

    def produce_results(self):
        cursor_ended = False
        kwargs = dict(cursorMark='*', sort='id asc')
        while not cursor_ended:
            results = self.solr.search('*:*', **kwargs)
            if kwargs['cursorMark'] != results.nextCursorMark:
                kwargs['cursorMark'] = results.nextCursorMark
                yield results
            else:
                cursor_ended = True


class Solr2EsAsync(object):
    def __init__(self, aiohttp_session, aes, solr_url, refresh=False) -> None:
        super().__init__()
        self.solr_url = solr_url
        self.aiohttp_session = aiohttp_session
        self.aes = aes
        self.refresh = refresh

    async def migrate(self, index_name) -> int:
        nb_results = 0
        async for results in self.produce_results():
            actions = create_es_actions(index_name, results)
            await self.aes.bulk(actions, index_name, DEFAULT_ES_DOC_TYPE, refresh=self.refresh)
            nb_results += len(results)
        return nb_results

    async def produce_results(self):
        cursor_ended = False
        kwargs = dict(cursorMark='*', sort='id asc', q='*:*', wt='json')
        while not cursor_ended:
            async with self.aiohttp_session.get(self.solr_url + '/select/', params=kwargs) as resp:
                json = loads(await resp.text())
                if kwargs['cursorMark'] != json['nextCursorMark']:
                    kwargs['cursorMark'] = json['nextCursorMark']
                    yield json['response']['docs']
                else:
                    cursor_ended = True


class RedisConsumer(object):
    def __init__(self, redis) -> None:
        self.redis = redis

    def consume(self, producer):
        for results in producer():
            self.redis.lpush('solr2es:queue', *map(dumps, results))


def create_es_actions(index_name, solr_results):
    results_ = [({'index': {'_index': index_name, '_type': DEFAULT_ES_DOC_TYPE, '_id': row['id']}}, remove_arrays(row))
                for row in solr_results]
    return '\n'.join(list(map(lambda d: dumps(d), itertools.chain(*results_))))


def remove_arrays(row):
    def filter(value):
        if type(value) is list:
            return value[0]
        else:
            return value
    return {k: filter(v) for k, v in row.items()}
