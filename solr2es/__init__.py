from elasticsearch.helpers import bulk


class Solr2Es(object):
    DEFAULT_ES_DOC_TYPE = 'doc'

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
            actions = self.create_es_actions(index_name, results)
            errors = bulk(self.es, actions, False, *[], refresh=self.refresh)
            nb_results += len(actions)
            if kwargs['cursorMark'] == results.nextCursorMark:
                cursor_ended = True
            kwargs['cursorMark'] = results.nextCursorMark
        return nb_results

    @staticmethod
    def create_es_actions(index_name, solr_results):
        return [{'_index': index_name, '_type': Solr2Es.DEFAULT_ES_DOC_TYPE, '_op_type': 'index',
                 '_id': row['id'], '_source': Solr2Es.remove_arrays(row)} for row in solr_results]

    @staticmethod
    def remove_arrays(row):
        def filter(value):
            if type(value) is list:
                return value[0]
            else:
                return value
        return {k: filter(v) for k, v in row.items()}


class IndexNotFoundException(RuntimeError):
    pass