/**
[env]
# Extra timeout because this test sleeps.
timeout = 10000
context-timeout = 10000

[test]
reason = "Public issue surrealdb/surrealdb#7234: an analyzer and a full-text index that uses it, defined CONCURRENTLY in one transaction, left the index in `error` with \"The analyzer ... does not exist\", because the build read the catalog before the transaction committed. The build now waits for the commit, so the index reaches `ready` and answers queries."
issue = 7234

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'ready'"

[[test.results]]
value = "[{ id: accounts:1 }]"
*/

DEFINE TABLE accounts SCHEMALESS;
CREATE accounts:1 SET firstName = 'Alice', lastName = 'Smith' RETURN NONE;
CREATE accounts:2 SET firstName = 'Bob', lastName = 'Jones' RETURN NONE;

BEGIN;
DEFINE ANALYZER accounts_admin_search TOKENIZERS blank FILTERS lowercase, ascii, edgengram(1, 50);
DEFINE INDEX _admin_search ON accounts FIELDS firstName FULLTEXT ANALYZER accounts_admin_search BM25(1.2, 0.75) CONCURRENTLY;
SLEEP 500ms;
COMMIT;
SLEEP 1s;
RETURN (INFO FOR INDEX _admin_search ON accounts).building.status;
SELECT id FROM accounts WITH INDEX _admin_search WHERE firstName @@ 'ali';
