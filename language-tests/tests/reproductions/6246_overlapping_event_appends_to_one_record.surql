/**
[test]
reason = """
	A synchronous event's body runs in the transaction of the write that fired
	it. Two writes whose event bodies append to the same array on one record
	both write that record, so when their transactions overlap the second to
	commit loses with a retryable `TransactionConflict`, and retried it appends
	to what the winner committed. Run one after the other, as here, both
	appends land. The overlap needs two concurrent requests, which the runner
	does not issue, so it is pinned by the tests in `surrealdb-core`'s
	`it::asyncevent`, along with an `ASYNC` event, whose body runs in a
	transaction of its own that is re-run on conflict.
"""
issue = 6246

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
value = "[{ events: [], id: User:u1 }]"

[[test.results]]
value = "[{ author: User:u1, id: SpaceEvent:a }]"

[[test.results]]
value = "[{ author: User:u1, id: SpaceEvent:b }]"

[[test.results]]
value = "[SpaceEvent:a, SpaceEvent:b]"

[[test.results]]
value = "[{ id: SpaceEvent:a }]"

[[test.results]]
value = "[SpaceEvent:b]"

*/
DEFINE TABLE SpaceEvent SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD author ON TABLE SpaceEvent TYPE option<record<User>>;
DEFINE EVENT update_author ON TABLE SpaceEvent WHEN $before.author != $after.author THEN {
	IF $before.author THEN { UPDATE $before.author SET events -= $before.id } END;
	IF $after.author THEN { UPDATE $after.author SET events += $after.id } END;
};
DEFINE TABLE User SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD events ON TABLE User TYPE option<array<record<SpaceEvent>>>;
CREATE User:u1 SET events = [];
CREATE SpaceEvent:a SET author = User:u1;
CREATE SpaceEvent:b SET author = User:u1;
SELECT VALUE events FROM ONLY User:u1;
UPDATE SpaceEvent:a SET author = NONE;
SELECT VALUE events FROM ONLY User:u1;
