/**
[test]
reason = """
	A killed live query ends: `KILL` removes it and queues a `Killed`
	notification for its subscriber, which the Rust SDK reads as the end of the
	stream, so `stream.next()` answers `None`. Live queries need a realtime
	session, which this runner does not open, so this is pinned by
	`method::live::a_killed_notification_leaves_nothing_to_tear_down` in
	`surrealdb` and by the WebSocket tests in `surrealdb-server`. This file
	holds the refusal a session without realtime meets.
"""
issue = 5793

[[test.results]]
value = "NONE"

[[test.results]]
error = "Unable to perform the realtime query"

[[test.results]]
error = "Unable to perform the realtime query"

[env]
remote-skip = "asserts the refusal of LIVE by a session without realtime, which the embedded harness's session is; a server endpoint opens the live query"
namespace = true
database = true
auth = { level = "owner" }
*/
DEFINE TABLE person;
LIVE SELECT * FROM person;
KILL u'0190a3c4-0000-7000-8000-000000000000';
