/**
[test]
reason = """
	`REMOVE TABLE` kills the table's live queries, each with a `Killed`
	notification queued for after the removal commits. A `LIVE` on the table
	rewrites the table's live-query stamp, which the removal also writes, so a
	`LIVE` and a `REMOVE TABLE` whose transactions overlap conflict and only one
	of them commits: no live query is registered on a table whose removal has
	already read its live queries. Live queries need a realtime session, which
	this runner does not open, so this is pinned by the tests in
	`surrealdb-core`'s `it::live`. This file holds the removal and redefinition
	the report's subscribers watch.
"""
issue = 4534

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, extensions: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: {  }, types: {  }, users: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[env]
namespace = true
database = true
auth = { level = "owner" }
*/
DEFINE TABLE person;
CREATE person:1;
REMOVE TABLE person;
INFO FOR DB;
DEFINE TABLE person;
SELECT * FROM person;
