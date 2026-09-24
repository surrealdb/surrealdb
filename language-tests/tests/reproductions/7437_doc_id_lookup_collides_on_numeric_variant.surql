/**
[env]

[test]
reason = """
Two record ids that differ only in the numeric variant of a nested value are
two records, and every index must keep them apart.

Storage addresses a record by the lossless codec, which carries a number's
`NumberKind`, so `c:[1]` and `c:[1dec]` are distinct records with distinct
keys. The forward doc-ID key `!di` encodes the id under `IndexFormat`, which
omits that kind, so both land on one key: the second record to be resolved
adopts the first's doc-ID, and the reverse mapping names only one of them.
Every doc-ID-consuming index — full-text and both ANN families — then returns
that one and silently drops the other.

The forward mapping therefore has a second, injective key, `!dj`, spelled with
`RecordIdentity`. `!di` is still written beside it so a node on the previous
release keeps resolving records this one maps, and a `!di` hit is adopted only
when `!dd` agrees the doc-ID names the record that was asked for — which is
what stops a record inheriting a collision it did not own.

That makes the doc-ID mapping whole, which is all a full-text index reads, so
the full-text index is what this pins. A plain index and the HNSW and DiskANN
pending queues key the id under `IndexFormat` themselves and still treat the
pair as one record; their routes in the keyspace say so.
"""
issue = 7437

[[test.results]]
value = "[{ body: 'hello world', id: c:[1], v: [1f, 0f] }]"

[[test.results]]
value = "[{ body: 'hello world', id: c:[1dec], v: [0f, 1f] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: c:[1] }, { id: c:[1dec] }]"

[[test.results]]
value = "[{ id: c:[1] }, { id: c:[1dec] }]"

[[test.results]]
value = "[{ id: c:[1] }, { id: c:[1dec] }]"
*/

-- `1` and `1dec` compare equal in SurrealQL but are distinct record keys.
CREATE c:[1]    SET body = 'hello world', v = [1.0f, 0.0f];
CREATE c:[1dec] SET body = 'hello world', v = [0.0f, 1.0f];

DEFINE ANALYZER simple TOKENIZERS blank,class;
DEFINE INDEX c_ft ON c FIELDS body FULLTEXT ANALYZER simple BM25;

-- Both records are stored and both are reachable without an index.
SELECT id FROM c ORDER BY id;
SELECT id FROM c WHERE body = 'hello world' ORDER BY id;
-- An index that allocates doc-IDs must return both as well.
SELECT id FROM c WHERE body @@ 'hello' ORDER BY id;
