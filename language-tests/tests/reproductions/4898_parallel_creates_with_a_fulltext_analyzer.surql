/**
[env]
timeout = 15000
context-timeout = 15000

[test]
reason = "Public issue surrealdb/surrealdb#4898: creating documents on a table with a full-text index and analyzer raised a transaction read conflict. A language test runs its statements in order, so this pins the end state only: every create is indexed and queryable. The parallel shape (16 writers x 100 creates over HTTP) was checked against a release server and raised no error."
issue = 4898

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 50 }]"
*/

DEFINE ANALYZER an TOKENIZERS blank,class FILTERS lowercase,snowball(english);
DEFINE TABLE doc SCHEMALESS;
DEFINE INDEX ft ON doc FIELDS body FULLTEXT ANALYZER an BM25;
FOR $i IN 1..=50 { CREATE doc SET body = 'hello world ' + <string> $i RETURN NONE; };
SELECT count() FROM doc WHERE body @@ 'hello' GROUP ALL;
