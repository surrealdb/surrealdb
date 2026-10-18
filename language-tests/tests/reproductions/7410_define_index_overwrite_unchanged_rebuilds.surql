/**
[env]
timeout = 15000
context-timeout = 15000

[test]
reason = "Public issue surrealdb/surrealdb#7410: DEFINE INDEX OVERWRITE with an unchanged definition rebuilds the index, and only a plain DEFINE replayed by an import keeps it. The build is not observable from a language test, so this pins only that repeated identical overwrites leave a ready, complete index that answers queries; it does not distinguish a rebuild from a skipped one. Skipping the rebuild is a feature decision."
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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'ready'"

[[test.results]]
value = "[obs:1, obs:2]"
*/

DEFINE ANALYZER an TOKENIZERS blank,class FILTERS lowercase;
DEFINE TABLE obs SCHEMALESS;
CREATE obs:1 SET content = 'alpha beta' RETURN NONE;
CREATE obs:2 SET content = 'beta gamma' RETURN NONE;
DEFINE INDEX ft ON obs FIELDS content FULLTEXT ANALYZER an BM25;
DEFINE INDEX OVERWRITE ft ON obs FIELDS content FULLTEXT ANALYZER an BM25;
DEFINE INDEX OVERWRITE ft ON obs FIELDS content FULLTEXT ANALYZER an BM25;
RETURN (INFO FOR INDEX ft ON obs).building.status;
SELECT VALUE id FROM obs WITH INDEX ft WHERE content @@ 'beta' ORDER BY id;
