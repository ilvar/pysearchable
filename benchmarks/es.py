import datetime
import json

from pysearchable.es import SearchableCipherElasticsearch


class BenchmarkES(object):
    encrypted_es = SearchableCipherElasticsearch(b'key goes here!!!', index_name="encrypted")

    # We'll be using ES from here
    unencrypted_es = SearchableCipherElasticsearch(b'bad', index_name="unencrypted")

    doc = {
        "author": "Kim Chong Un",
        "text": "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the "
                "industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type "
                "and scrambled it to make a type specimen book. It has survived not only five centuries, but also "
                "the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the "
                "1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with "
                "desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.",
        "date": datetime.date(2010, 1, 1),
        "time": datetime.time(11, 12, 13),
        "value": 1234,
    }

    field_params = ["author"], ["text"], ["date"], ["time"], ["value"]

    def index_encrypted(self):
        for obj_id in range(0, 1000):
            self.encrypted_es.index_doc(self.doc, *self.field_params, obj_id=obj_id)
            self.encrypted_es.es.indices.refresh(self.encrypted_es.index_name)

    def index_unencrypted(self):
        for obj_id in range(0, 1000):
            body = self.doc.copy()

            body['date'] = body['date'].strftime("%s")
            body['time'] = body['time'].strftime("%s")

            self.unencrypted_es.es.index(index=self.unencrypted_es.index_name, doc_type='tweet', id=obj_id, body=body)
            self.unencrypted_es.es.indices.refresh(self.unencrypted_es.index_name)


if __name__ == '__main__':
    bes = BenchmarkES()

    try:
        bes.encrypted_es.es.delete(bes.encrypted_es.index_name, "_all", {"query": {"match_all": {}}})
    except:
        pass
    try:
        bes.unencrypted_es.es.delete(bes.encrypted_es.index_name, "_all", {"query": {"match_all": {}}})
    except:
        pass

    begin = datetime.datetime.now()
    bes.index_encrypted()
    print "Encrypted", datetime.datetime.now() - begin

    begin = datetime.datetime.now()
    bes.index_unencrypted()
    print "Not encrypted", datetime.datetime.now() - begin
