/**
[env]
backend = ["surrealkv"]
versioned = true
timeout = 15000

[test]
reason = """
	`MATCHES` has no evaluation but the full-text index, and an index entry
	holds the state the last write left rather than the state at a stamp. A
	read at a `VERSION` stamp answers a `MATCHES` from that current index and
	reads each record it names at the stamp: `red` matches only `doc:4` now,
	which did not exist at the stamp, so the read finds nothing, while `apple`
	matches `doc:2` now and at the stamp alike. The same statement with no
	stamp, or with `VERSION NONE`, reads the current records.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ body: 'red apple', id: doc:1 }]"

[[test.results]]
value = "[{ body: 'green apple', id: doc:2 }]"

[[test.results]]
value = "[{ body: 'red pear', id: doc:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ body: 'red plum', id: doc:4 }]"

[[test.results]]
value = "[{ body: 'blue grape', id: doc:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[doc:4]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[doc:2]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[doc:4]"
*/

DEFINE TABLE doc SCHEMALESS;
DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
DEFINE INDEX doc_body ON doc FIELDS body FULLTEXT ANALYZER simple BM25;

CREATE doc:1 SET body = "red apple";
CREATE doc:2 SET body = "green apple";
CREATE doc:3 SET body = "red pear";
SLEEP 50ms;
LET $ts = time::now();
SLEEP 50ms;

CREATE doc:4 SET body = "red plum";
UPDATE doc:1 SET body = "blue grape";
DELETE doc:3;

SELECT VALUE id FROM doc WHERE body @@ "red" ORDER BY id;
SELECT VALUE id FROM doc WHERE body @@ "red" ORDER BY id VERSION $ts;
SELECT VALUE id FROM doc WHERE body @@ "apple" ORDER BY id VERSION $ts;
SELECT count() FROM doc WHERE body @@ "red" GROUP ALL VERSION $ts;
SELECT VALUE id FROM doc WHERE id IN (SELECT VALUE id FROM doc WHERE body @@ "red") ORDER BY id VERSION $ts;
SELECT VALUE id FROM doc WHERE body @@ "red" ORDER BY id VERSION NONE;
