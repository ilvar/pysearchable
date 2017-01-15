import datetime
import unittest

from pysearchable.es import SearchableCipherElasticsearch


class TestEncryption(unittest.TestCase):
    sce = SearchableCipherElasticsearch(b'key goes here!!!')

    doc = {
        "author": "Kim Chong Un",
        "text": "The quick brown fox jumps over the lazy dog!",
        "date": datetime.date(2010, 1, 1),
        "time": datetime.time(11, 12, 13),
        "value": 1234,
    }

    field_params = ["author"], ["text"], ["date"], ["time"], ["value"]

    # One time indexing
    sce.index_doc(doc, *field_params, doc_type="test", obj_id=1)
    sce.es.indices.refresh(sce.index_name)

    def search_total(self, query):
        result = self.sce.search(query)
        return result["hits"]["total"]

    def encrypt_phrase(self, phrase):
        return " ".join(map(self.sce.encrypt_token, phrase.split(" ")))

    def test_author(self):
        self.assertFalse(self.search_total({"match": {"author": self.sce.encrypt_token("Kim")}}))
        self.assertTrue(self.search_total({"match": {"author": self.sce.encrypt_token("Kim Chong Un")}}))

    def test_text(self):
        print "qui", self.sce.encrypt_token("qui")

        self.assertFalse(self.search_total({"match": {"text": self.sce.encrypt_token("foxie")}}))
        self.assertTrue(self.search_total({"match": {"text": self.sce.encrypt_token("qui")}}))

    def test_text_phrase(self):
        print "brown fox", self.encrypt_phrase("brown fox")

        self.assertFalse(self.search_total({"match_phrase": {"text": self.encrypt_phrase("The dog")}}))
        self.assertTrue(self.search_total({"match_phrase": {"text": self.encrypt_phrase("brown fox")}}))

    def test_date(self):
        low = self.sce.encrypt_date(datetime.date(2009, 12, 31))
        hi = self.sce.encrypt_date(datetime.date(2010, 1, 2))
        self.assertFalse(self.search_total({"range": {"date": {"gt": hi}}}))
        self.assertTrue(self.search_total({"range": {"date": {"gt": low, "lt": hi}}}))

    def test_time(self):
        low = self.sce.encrypt_time(datetime.time(11, 12, 12))
        hi = self.sce.encrypt_time(datetime.time(11, 12, 14))
        self.assertFalse(self.search_total({"range": {"time": {"gt": hi}}}))
        self.assertTrue(self.search_total({"range": {"time": {"gt": low, "lt": hi}}}))

    def test_value(self):
        low = self.sce.encrypt_int(1233)
        hi = self.sce.encrypt_int(1235)
        self.assertFalse(self.search_total({"range": {"value": {"gt": hi}}}))
        self.assertTrue(self.search_total({"range": {"value": {"gt": low, "lt": hi}}}))


if __name__ == '__main__':
    unittest.main()
