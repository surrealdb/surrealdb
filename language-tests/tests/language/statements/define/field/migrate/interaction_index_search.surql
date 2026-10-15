/**
[test]
reason = "A full-text index and a vector index defined over rows behind a migration step are built from the migrated values, and a write to a row behind the step removes the entries of its migrated value, not of the stored one."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[doc:1, doc:2]"

[[test.results]]
match = '''string::contains(<string>$result, "operator: 'FullTextScan'") && string::contains(<string>$result, "index: 'ft'") && !string::contains(<string>$result, 'TableScan')'''

[[test.results]]
value = "[doc:4]"

[[test.results]]
value = "[doc:1]"

[[test.results]]
value = "['hello earth', 'goodbye earth', 'plain text', 'brave world']"

[[test.results]]
value = "[]"

[[test.results]]
value = "[doc:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[doc:1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[1f, 0f, 0f], [0f, 1f, 0f], [0f, 0f, 1f], [5f, 5f, 5f]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[vec:2]"

[[test.results]]
match = '''string::contains(<string>$result, "operator: 'KnnScan'") && string::contains(<string>$result, "index: 'hn'") && !string::contains(<string>$result, 'TableScan')'''

[[test.results]]
value = "[vec:4]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[vec:1]"

[[test.results]]
value = "[vec:2]"
*/
DEFINE ANALYZER an TOKENIZERS blank FILTERS lowercase;
DEFINE TABLE doc SCHEMAFULL;
DEFINE FIELD body ON doc TYPE string;
CREATE doc:1 SET body = 'hello world' RETURN NONE;
CREATE doc:2 SET body = 'goodbye world' RETURN NONE;
CREATE doc:3 SET body = 'plain text' RETURN NONE;
ALTER FIELD body ON doc MIGRATE EXISTING ROWS AS earth USING string::replace($value, 'world', 'earth') FALLBACK '';
CREATE doc:4 SET body = 'brave world' RETURN NONE;
DEFINE INDEX ft ON doc FIELDS body FULLTEXT ANALYZER an BM25;
SELECT VALUE id FROM doc WHERE body @@ 'earth' ORDER BY id;
SELECT VALUE id FROM doc WHERE body @@ 'earth' ORDER BY id EXPLAIN;
SELECT VALUE id FROM doc WHERE body @@ 'world' ORDER BY id;
SELECT VALUE id FROM doc WHERE body @@ 'hello' ORDER BY id;
SELECT VALUE body FROM doc ORDER BY id;
UPDATE doc:1 SET body = 'bright sun' RETURN NONE;
SELECT VALUE id FROM doc WHERE body @@ 'earth' ORDER BY id;
SELECT VALUE id FROM doc WHERE body @@ 'hello' ORDER BY id;
SELECT VALUE id FROM doc WHERE body @@ 'bright' ORDER BY id;
DELETE doc:2;
SELECT VALUE id FROM doc WHERE body @@ 'earth' ORDER BY id;
SELECT VALUE id FROM doc WHERE body @@ 'goodbye' ORDER BY id;
-- A vector index.
DEFINE TABLE vec SCHEMAFULL;
DEFINE FIELD v ON vec TYPE array<string>;
CREATE vec:1 SET v = ['1', '0', '0'] RETURN NONE;
CREATE vec:2 SET v = ['0', '1', '0'] RETURN NONE;
CREATE vec:3 SET v = ['0', '0', '1'] RETURN NONE;
DEFINE FIELD OVERWRITE v ON vec TYPE array<float, 3> MIGRATE EXISTING ROWS AS v_float USING array::map($value, |$x| <float>$x) FALLBACK [0f, 0f, 0f];
CREATE vec:4 SET v = [5f, 5f, 5f] RETURN NONE;
SELECT VALUE v FROM vec ORDER BY id;
DEFINE INDEX hn ON vec FIELDS v HNSW DIMENSION 3 DIST EUCLIDEAN TYPE F32;
SELECT VALUE id FROM vec WHERE v <|1,40|> [0.1, 0.9, 0.0];
SELECT VALUE id FROM vec WHERE v <|1,40|> [0.1, 0.9, 0.0] EXPLAIN;
SELECT VALUE id FROM vec WHERE v <|1,40|> [4.0, 4.0, 4.0];
UPDATE vec:2 SET v = [9f, 9f, 9f] RETURN NONE;
SELECT VALUE id FROM vec WHERE v <|1,40|> [0.1, 0.9, 0.0];
SELECT VALUE id FROM vec WHERE v <|1,40|> [8.0, 8.0, 8.0];
