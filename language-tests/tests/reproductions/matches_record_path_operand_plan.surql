/**
[env]

[test]
reason = """A MATCHES whose query is a record path, a row-independent expression evaluated once, is served by the full-text index rather than falling back to a table scan, so the plan reads the index."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: user:alice, principals: ['g1', 'g2'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

*/

DEFINE TABLE user SCHEMAFULL;
DEFINE FIELD principals ON user TYPE array<string>;
CREATE user:alice SET principals = ['g1', 'g2'];

DEFINE TABLE doc SCHEMAFULL;
DEFINE FIELD acl ON doc TYPE string;
DEFINE ANALYZER simple TOKENIZERS blank;
DEFINE INDEX doc_acl ON doc FIELDS acl FULLTEXT ANALYZER simple;

-- The plan reads the full-text index, and does not scan the table.
RETURN string::contains(<string> (EXPLAIN SELECT VALUE id FROM doc WHERE acl @2@ user:alice.principals[1]), 'FullTextScan [ctx: Db] [table: doc, index: doc_acl');
RETURN string::contains(<string> (EXPLAIN SELECT VALUE id FROM doc WHERE acl @2@ user:alice.principals[1]), 'TableScan');
