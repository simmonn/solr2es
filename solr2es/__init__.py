class Solr2Es(object):
    DEFAULT_ES_DOC_TYPE = 'doc'

    def __init__(self, solr, es, refresh=False) -> None:
        super().__init__()
        self.solr = solr
        self.es = es
        self.refresh = refresh

    def migrate(self, index_name) -> int:
        results = self.solr.search('*:*')
        for row in results:
            self.es.create(index=index_name, doc_type=Solr2Es.DEFAULT_ES_DOC_TYPE, id=row['id'], body=row, refresh=self.refresh)
        return len(results)


class IndexNotFoundException(RuntimeError):
    pass