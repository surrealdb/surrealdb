/**
[env]
timeout = 15000
context-timeout = 15000

[test]
reason = "Public issue surrealdb/surrealdb#7410: DEFINE INDEX OVERWRITE that restates the definition of a ready index (the same table, columns and kind) keeps the index and its data rather than rebuilding it, so an idempotent schema script costs no rebuild. The build state's `initial` count shows it: it still counts the two records the first build scanned, not the third created after it. A changed definition, an index that is not ready, or one stored in an older format is rebuilt."
issue = 7410

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

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
value = "NONE"

[[test.results]]
value = "{ initial: 2, status: 'ready' }"

[[test.results]]
value = "[obs:1, obs:2, obs:3]"

*/

DEFINE ANALYZER an TOKENIZERS blank,class FILTERS lowercase;
DEFINE TABLE obs SCHEMALESS;
CREATE obs:1 SET content = 'alpha beta' RETURN NONE;
CREATE obs:2 SET content = 'beta gamma' RETURN NONE;
DEFINE INDEX ft ON obs FIELDS content FULLTEXT ANALYZER an BM25;
CREATE obs:3 SET content = 'beta delta' RETURN NONE;
DEFINE INDEX OVERWRITE ft ON obs FIELDS content FULLTEXT ANALYZER an BM25;
DEFINE INDEX OVERWRITE ft ON obs FIELDS content FULLTEXT ANALYZER an BM25;
RETURN (INFO FOR INDEX ft ON obs).building.{ initial, status };
SELECT VALUE id FROM obs WITH INDEX ft WHERE content @@ 'beta' ORDER BY id;
