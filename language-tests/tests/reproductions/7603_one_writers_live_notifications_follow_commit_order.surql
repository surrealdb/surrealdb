/**
[test]
reason = """
	The live-query notifications of one client's commits are delivered in the
	order of those commits. Each commit queues its notifications on the
	datastore's one delivery queue before its statement answers, and one task
	hands them to the broker in that order; the embedded engine passes each to
	its subscriber in the order it receives them. Live queries need a realtime
	session, which this runner does not open, so the order is pinned by
	`it::live::one_writers_notifications_reach_the_broker_in_commit_order` in
	`surrealdb-core` and by
	`session::tests::a_subscriber_receives_its_notifications_in_delivery_order`
	in `surrealdb-engine-local`. This file holds the writes, one commit each,
	that the subscriber is owed notifications for.
"""
issue = 7603

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: counter:1, value: 1 }]"

[[test.results]]
value = "[{ id: counter:1, value: 2 }]"

[[test.results]]
value = "[{ id: counter:1, value: 3 }]"

[[test.results]]
value = "[{ id: counter:1, value: 3 }]"

[env]
namespace = true
database = true
auth = { level = "owner" }
*/
DEFINE TABLE counter SCHEMALESS;
UPSERT counter:1 CONTENT { value: 1 };
UPSERT counter:1 CONTENT { value: 2 };
UPSERT counter:1 CONTENT { value: 3 };
SELECT * FROM counter;
