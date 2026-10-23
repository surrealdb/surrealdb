/**
[test]
reason = """
	`KILL` answers `NONE`, so a transport cannot learn from the statement's
	result which live query it ended. The server untracks a killed live query
	from the `Killed` notification the kill queues instead: the notification
	carries the id, and `dispatch_live_notification` removes the WebSocket
	registration and decrements the active live-query gauge on it, for `KILL`
	as a statement and the `kill` RPC method alike. Live queries need a realtime
	session, which this runner does not open, so this is pinned by
	`rpc::websocket::kill_statement_unregisters_live_query_over_websocket` in
	`surrealdb-server`. This file holds the refusal a session without realtime
	meets.
"""
issue = 7528

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
