class Solr2Es(object):

    def __init__(self, solr_url, es_url) -> None:
        super().__init__()
        self.solr_url = solr_url
        self.es_url = es_url

    def migrate(self, index_name):
        return 0