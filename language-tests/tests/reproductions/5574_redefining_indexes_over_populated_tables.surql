/**
[env]
timeout = 15000
context-timeout = 15000

[test]
reason = "Public issue surrealdb/surrealdb#5574: redefining full-text indexes with OVERWRITE over very large RocksDB tables made the 2.x server exit silently. The report needs tens of millions of rows on RocksDB, which 4.0 does not ship, and has no reproduction without them. This pins what a small instance can: repeated OVERWRITE of several full-text indexes leaves every index ready and answering queries."
issue = 5574

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'ready'"

[[test.results]]
value = "'ready'"

[[test.results]]
value = "[{ count: 200 }]"

[[test.results]]
value = "[{ count: 200 }]"

*/

DEFINE ANALYZER ascii TOKENIZERS class FILTERS ascii;
DEFINE TABLE a SCHEMALESS;
DEFINE TABLE b SCHEMALESS;
FOR $i IN 1..=200 { CREATE type::record("a", $i) SET d = 'alpha ' + <string> $i RETURN NONE; CREATE type::record("b", $i) SET d = 'beta ' + <string> $i RETURN NONE; };
DEFINE INDEX OVERWRITE p ON TABLE a FIELDS d FULLTEXT ANALYZER ascii BM25 HIGHLIGHTS;
DEFINE INDEX OVERWRITE p ON TABLE b FIELDS d FULLTEXT ANALYZER ascii BM25 HIGHLIGHTS;
DEFINE INDEX OVERWRITE p ON TABLE a FIELDS d FULLTEXT ANALYZER ascii BM25 HIGHLIGHTS;
DEFINE INDEX OVERWRITE p ON TABLE b FIELDS d FULLTEXT ANALYZER ascii BM25 HIGHLIGHTS;
RETURN (INFO FOR INDEX p ON a).building.status;
RETURN (INFO FOR INDEX p ON b).building.status;
SELECT count() FROM a WHERE d @@ 'alpha' GROUP ALL;
SELECT count() FROM b WHERE d @@ 'beta' GROUP ALL;
