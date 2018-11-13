import itertools
from json import loads, dumps

from elasticsearch.helpers import bulk

DEFAULT_ES_DOC_TYPE = 'doc'


class Solr2Es(object):
    def __init__(self, solr, es, refresh=False) -> None:
        super().__init__()
        self.solr = solr
        self.es = es
        self.refresh = refresh

    def migrate(self, index_name) -> int:
        cursor_ended = False
        nb_results = 0
        kwargs = dict(cursorMark='*', sort='id asc')
        while not cursor_ended:
            results = self.solr.search('*:*', **kwargs)
            actions = create_es_actions(index_name, results)
            errors = bulk(self.es, actions, False, None, refresh=self.refresh)
            nb_results += len(actions)
            if kwargs['cursorMark'] == results.nextCursorMark:
                cursor_ended = True
            kwargs['cursorMark'] = results.nextCursorMark
        return nb_results


class Solr2EsAsync(object):
    def __init__(self, aiohttp_session, aes, refresh=False) -> None:
        super().__init__()
        self.aiohttp_session = aiohttp_session
        self.aes = aes
        self.refresh = refresh

    async def migrate(self, index_name) -> int:
        cursor_ended = False
        nb_results = 0
        kwargs = dict(cursorMark='*', sort='id asc', q='*:*', wt='json')
        while not cursor_ended:
            async with self.aiohttp_session.get('http://solr:8983/solr/my_core/select/', params=kwargs) as resp:
                json = loads(await resp.text())
                if kwargs['cursorMark'] != json['nextCursorMark']:
                    actions = create_es_actions_async(index_name, json['response']['docs'])
                    await self.aes.bulk(actions, index_name, DEFAULT_ES_DOC_TYPE, refresh=self.refresh)
                    nb_results += len(json['response']['docs'])
                    kwargs['cursorMark'] = json['nextCursorMark']
                else:
                    cursor_ended = True
        return nb_results


def create_es_actions(index_name, solr_results):
    return [{'_index': index_name, '_type': DEFAULT_ES_DOC_TYPE, '_op_type': 'index',
             '_id': row['id'], '_source': remove_arrays(row)} for row in solr_results]


def create_es_actions_async(index_name, solr_results):
    results_ = [({'index': {'_index': index_name, '_type': DEFAULT_ES_DOC_TYPE, '_id': row['id']}}, remove_arrays(row)) for row
                in solr_results]
    l = list(map(lambda d: dumps(d), itertools.chain(*results_)))
    return '\n'.join(l)


def remove_arrays(row):
    def filter(value):
        if type(value) is list:
            return value[0]
        else:
            return value
    return {k: filter(v) for k, v in row.items()}


class IndexNotFoundException(RuntimeError):
    pass