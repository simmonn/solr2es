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
            for row in results:
                self.es.create(index=index_name, doc_type=Solr2Es.DEFAULT_ES_DOC_TYPE, id=row['id'], body=self.remove_arrays(row), refresh=self.refresh)
                nb_results += 1
            if kwargs['cursorMark'] == results.nextCursorMark:
                cursor_ended = True
            kwargs['cursorMark'] = results.nextCursorMark
        return nb_results

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