from elasticsearch import Elasticsearch

import searchable_cipher


class SearchableCipherElasticsearch(searchable_cipher.SearchableCipher):
    ANALYZER_SETTINGS = {
        "settings": {
            "analysis": {
                "analyzer": {
                    "my_analyzer": {
                        "tokenizer": "my_tokenizer"
                    }
                },
                "tokenizer": {
                    "my_tokenizer": {
                        "type": "edge_ngram",
                        "min_gram": 2,
                        "max_gram": 10,
                        "token_chars": [
                            "letter",
                            "digit"
                        ]
                    }
                }
            }
        }
    }

    def __init__(self, key, index_name="test-index", es_urls=None):
        super(SearchableCipherElasticsearch, self).__init__(key)

        self.es = Elasticsearch(es_urls or ['http://elastic:changeme@localhost:9200/'])
        self.index_name = index_name

        if self.es.indices.exists(index=self.index_name):
            self.es.indices.delete(index=self.index_name)

        self.es.indices.create(index=self.index_name, body=self.ANALYZER_SETTINGS)

    def tokenize(self, str):
        """

        :param str:
        :return:
        """
        analysis = self.es.indices.analyze(index=self.index_name, body=str, params={"analyzer": "my_analyzer"})
        return [t["token"] for t in analysis["tokens"]]

    def index_doc(self, doc, raw_fields, fulltext_fields, date_fields, time_fields, int_fields, allow_unencrypted=False):
        """

        :param doc: dics
        :param raw_fields: list
        :param fulltext_fields: list
        :param date_fields: list
        :param time_fields: list
        :param int_fields: list
        :param allow_unencrypted: bool
        :return:
        """

        if not allow_unencrypted:
            if set(doc.keys()) != set(raw_fields + fulltext_fields + date_fields + time_fields + int_fields):
                raise RuntimeError("Some fields are left unencrypted")
            encrypted_doc = {}
        else:
            encrypted_doc = doc.clone()

        encryptors = (
            (raw_fields, self.encrypt_token),
            (fulltext_fields, self.encrypt_tokenize_text),
            (date_fields, self.encrypt_date),
            (time_fields, self.encrypt_time),
            (int_fields, self.encrypt_int),
        )

        for fields, handler in encryptors:
            for f in fields:
                encrypted_doc[f] = handler(doc[f])

        return self.es.index(index=self.index_name, doc_type='tweet', id=1, body=encrypted_doc)

    def search(self, query):
        """

        :param query:
        :return:
        """
        return self.es.search(index=self.index_name, body={"query": query})
