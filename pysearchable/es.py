import elasticsearch.helpers

from elasticsearch import Elasticsearch

from pysearchable import generic


class SearchableCipherElasticsearch(generic.SearchableCipher):
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

    def index_doc(self, doc, raw_fields, fulltext_fields, date_fields, time_fields, int_fields, doc_type, obj_id):
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

        encrypted_doc = doc.copy()

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
                if handler == self.encrypt_tokenize_text:
                    encrypted_doc[f + "_raw"] = self.encrypt_token(doc[f])

        return self.es.index(index=self.index_name, doc_type=doc_type, id=obj_id, body=encrypted_doc)

    def index_bulk(self, docs, raw_fields, fulltext_fields, date_fields, time_fields, int_fields):
        encryptors = (
            (raw_fields, lambda values: map(self.encrypt_token, values)),
            (fulltext_fields, lambda values: map(self.encrypt_tokenize_text, values)),
            (date_fields, lambda values: map(self.encrypt_date, values)),
            (time_fields, lambda values: map(self.encrypt_time, values)),
            (int_fields, lambda values: map(self.encrypt_int, values)),
        )

        bulk_encrypt = {}
        doc_types = []
        doc_ids = []
        for (doc_type, doc_id, doc) in docs:
            doc_types.append(doc_type)
            doc_ids.append(doc_id)
            for k,v in doc.items():
                bulk_encrypt.setdefault(k, []).append(v)

        prepared_fields = {}
        for fields, handler in encryptors:
            for f in fields:
                prepared_fields[f] = handler(bulk_encrypt[f])
                if handler == self.encrypt_tokenize_text:
                    prepared_fields[f + "_raw"] = map(self.encrypt_token, bulk_encrypt[f])

        bulk = []
        for i in range(len(docs)):
            doc_type = doc_types[i]
            obj_id = doc_ids[i]
            body = dict((f, prepared_fields[f][i]) for f in prepared_fields.keys())
            bulk.append({
                '_op_type': 'index',
                '_index': self.index_name,
                '_type': doc_type,
                '_id': obj_id,
                '_source': body
            })

        return elasticsearch.helpers.bulk(self.es, bulk)

    def search(self, query):
        """

        :param query:
        :return:
        """
        return self.es.search(index=self.index_name, body={"query": query})
