/**
[test]
reason = """
	A `LIVE SELECT` is owned by the datastore node that registered it. With the
	default inline engine a write's notifications are made on the node that
	wrote, and the community broker delivers only those for subscriptions its
	own node owns; reaching a subscription on another node needs a broker that
	routes to that node, which a clustered composer supplies through
	`TransactionBuilderFactory::live_query_broker`. Live queries need a realtime
	session, which this runner does not open, so delivery is pinned by the Rust
	tests in `surrealdb-core`'s `it::live`. This file holds the writes the
	report's subscriber is owed notifications for.
"""
issue = 5070

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: liveQueryTestMurder:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: liveQueryTestMurder:2 }]"

[[test.results]]
value = "[{ id: liveQueryTestMurder:2 }]"

[env]
namespace = true
database = true
auth = { level = "owner" }
*/
DEFINE TABLE liveQueryTestMurder SCHEMAFULL PERMISSIONS FULL;
CREATE liveQueryTestMurder:1 CONTENT {};
DELETE liveQueryTestMurder;
CREATE liveQueryTestMurder:2 CONTENT {};
SELECT * FROM liveQueryTestMurder WHERE id.parent == NONE;
