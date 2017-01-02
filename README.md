# Searchable encryption in python

This is proof of concept for implementing searchable encryption over Elasticsearch.
It's using industry standard AES encryption for strings and [pyope](https://pypi.python.org/pypi/pyope) for Order
Preserving Encryption (it's based on a paper by Boldyreva et al) of numbers, dates and times.

There are both generic `SearchableCipher` and Elasticsearch specific `SearchableCipherElasticsearch`. Aside from
indexing helper, latter provides proper Elasticsearch tokenization.

## Usage

Generic:
```
sc = SearchableCipher(b'key goes here!!!')

# Encrypts a list of tokens
self.sc.encrypt_tokens(["foo", "bar"])

# Splits text into tokens and encrypts them
self.sc.encrypt_tokenize_text("Foo BAR!")

# Encrypts one token as is
self.sc.encrypt_token("The quick brown fox jumps over the lazy dog!")

# Encrypts an integer
self.sc.encrypt_int(1234)

# Encrypts a datetime.date
self.sc.encrypt_date(datetime.date(2010, 11, 12))

# Encrypts a datetime.time
self.sc.encrypt_time(datetime.time(13, 14, 15))
```

Elasticsearch:
```
sce = SearchableCipherElasticsearch(b'key goes here!!!')

doc = {
    "text": "The quick brown fox jumps over the lazy dog!",
    "value": 1234,
}

sce.index_doc(doc, [], ["text"], [], [], ["value"])
sce.es.indices.refresh(sce.index_name)

# Matches nothing
self.sce.search({"match": {"text": self.sce.encrypt_token("foxie")}})

# Matches "Quick"
self.sce.search({"match": {"text": self.sce.encrypt_token("qui")}})

# Matches nothing
self.sce.search({"range": {"value": {"gt": self.sce.encrypt_int(1235)}}})

# Matches 1234
self.sce.search({"range": {"value": {"gt": self.sce.encrypt_int(1233)}}})
```

## Testing

To run generic tests:

```
python test_basic.py
```

To run Elasticsearch tests you need to install and run Elasticsearch. Then:

```
python test_es.py
```

## Advantages

 * Elasticsearch is optional, we're only using it for great tokenization. It's easy to add any necessary backend.
 * All encryption is done in the application, so you can use insecure channels and Elasticsearch servers.
 * It's possible to use separate encryption key for each index.

## Downsides

 * NGram based indexes should consume more space
 * Each tokenized field adds a roundtrip to ES, so indexing should be slower
 * It imposes additional restrictions and may lose some precision in OPE

## TODO

 * Remove redundant ES assumptions (hosts, ports, creds, index/analyzer names etc)
 * Better module layout
 * setup.py
