import elasticsearch.helpers
import itertools

from elasticsearch import Elasticsearch

from pysearchable import generic


class SearchableCipherElasticsearch(generic.SearchableCipher):
    ANALYZER_SETTINGS = {
        "settings": {
            "analysis": {
                "analyzer": {
                    "my_analyzer": {
                        "tokenizer": "edge_ngram_tokenizer"
                    },
                    "already_encrypted_analyzer": {
                        "tokenizer": "space_tokenizer",
                        "filter": ["encrypted"]
                    }
                },
                "filter": {
                    "encrypted": {
                        "type": "pattern_capture",
                        "preserve_original": 0,
                        "patterns": [
                            "([^\|\s]+==)"
                        ]
                    }
                },
                "tokenizer": {
                    "space_tokenizer": {
                        "type": "pattern",
                        "pattern": " "
                    },
                    "edge_ngram_tokenizer": {
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
        },
        "mappings": {
            "_default_": {
                "properties": {
                    "text": {
                        "type": "text",
                        "index": "analyzed",
                        "analyzer": "already_encrypted_analyzer"
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
        token_groups = itertools.groupby(analysis["tokens"], lambda t: t["start_offset"])
        token_merges = [map(lambda t: t['token'], g) for (k,g) in token_groups]
        return token_merges

    def encrypt_tokenize_text(self, text):
        """
        With tokenization

        :param str:
        :return:
        """
        all_tokens = self.tokenize(text)
        encrypted_tokens = map(self.encrypt_tokens, all_tokens)
        return " ".join("|".join(ngrams) for ngrams in encrypted_tokens)

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

    def tokenize_bulk(self, texts_list):
        bulk_text = " RANDOMTEXT ".join(texts_list)
        tokens = self.tokenize(bulk_text)
        doc_tokens = []
        i = 0

        for t in tokens:
            if "RANDOMTEXT" in t:
                i += 1
            else:
                if i >= len(doc_tokens):
                    doc_tokens.append([t])
                else:
                    doc_tokens[i].append(t)

        encrypted_tokens = [" ".join("|".join(self.encrypt_tokens(ngrams)) for ngrams in single_doc_tokens) for single_doc_tokens in doc_tokens]
        return encrypted_tokens

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
            for k, v in doc.items():
                bulk_encrypt.setdefault(k, []).append(v)

        prepared_fields = {}
        for fields, handler in encryptors:
            for f in fields:
                if fields == fulltext_fields:
                    prepared_fields[f + "_raw"] = map(self.encrypt_token, bulk_encrypt[f])
                    prepared_fields[f] = self.tokenize_bulk(bulk_encrypt[f])
                else:
                    prepared_fields[f] = handler(bulk_encrypt[f])

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
