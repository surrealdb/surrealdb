/**
[test]
reason = "Setup for return_none_pruned_gate_observers.surql: twin tables under each select clause the stages can prove inert (FULL, NONE, and a predicate over the session alone), each with a change feed that keeps the earlier image, an event that records the images of every write in an audit table, and a view that shows the stored rows."
run = false
*/

DEFINE TABLE user SCHEMALESS;
CREATE user:alice;
DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('user', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE ob_full_a SCHEMALESS CHANGEFEED 1h INCLUDE ORIGINAL PERMISSIONS FOR select FULL FOR create, update, delete FULL;
DEFINE TABLE ob_full_a_audit SCHEMALESS PERMISSIONS FULL;
DEFINE EVENT ob_full_a_event ON ob_full_a WHEN true THEN (CREATE ob_full_a_audit SET action = $event, before = $before.n, after = $after.n);
DEFINE TABLE ob_full_a_state AS SELECT id, n FROM ob_full_a PERMISSIONS FULL;

DEFINE TABLE ob_full_b SCHEMALESS CHANGEFEED 1h INCLUDE ORIGINAL PERMISSIONS FOR select FULL FOR create, update, delete FULL;
DEFINE TABLE ob_full_b_audit SCHEMALESS PERMISSIONS FULL;
DEFINE EVENT ob_full_b_event ON ob_full_b WHEN true THEN (CREATE ob_full_b_audit SET action = $event, before = $before.n, after = $after.n);
DEFINE TABLE ob_full_b_state AS SELECT id, n FROM ob_full_b PERMISSIONS FULL;

DEFINE TABLE ob_none_a SCHEMALESS CHANGEFEED 1h INCLUDE ORIGINAL PERMISSIONS FOR select NONE FOR create, update, delete FULL;
DEFINE TABLE ob_none_a_audit SCHEMALESS PERMISSIONS FULL;
DEFINE EVENT ob_none_a_event ON ob_none_a WHEN true THEN (CREATE ob_none_a_audit SET action = $event, before = $before.n, after = $after.n);
DEFINE TABLE ob_none_a_state AS SELECT id, n FROM ob_none_a PERMISSIONS FULL;

DEFINE TABLE ob_none_b SCHEMALESS CHANGEFEED 1h INCLUDE ORIGINAL PERMISSIONS FOR select NONE FOR create, update, delete FULL;
DEFINE TABLE ob_none_b_audit SCHEMALESS PERMISSIONS FULL;
DEFINE EVENT ob_none_b_event ON ob_none_b WHEN true THEN (CREATE ob_none_b_audit SET action = $event, before = $before.n, after = $after.n);
DEFINE TABLE ob_none_b_state AS SELECT id, n FROM ob_none_b PERMISSIONS FULL;

DEFINE TABLE ob_auth_a SCHEMALESS CHANGEFEED 1h INCLUDE ORIGINAL PERMISSIONS FOR select WHERE $auth != NONE FOR create, update, delete FULL;
DEFINE TABLE ob_auth_a_audit SCHEMALESS PERMISSIONS FULL;
DEFINE EVENT ob_auth_a_event ON ob_auth_a WHEN true THEN (CREATE ob_auth_a_audit SET action = $event, before = $before.n, after = $after.n);
DEFINE TABLE ob_auth_a_state AS SELECT id, n FROM ob_auth_a PERMISSIONS FULL;

DEFINE TABLE ob_auth_b SCHEMALESS CHANGEFEED 1h INCLUDE ORIGINAL PERMISSIONS FOR select WHERE $auth != NONE FOR create, update, delete FULL;
DEFINE TABLE ob_auth_b_audit SCHEMALESS PERMISSIONS FULL;
DEFINE EVENT ob_auth_b_event ON ob_auth_b WHEN true THEN (CREATE ob_auth_b_audit SET action = $event, before = $before.n, after = $after.n);
DEFINE TABLE ob_auth_b_state AS SELECT id, n FROM ob_auth_b PERMISSIONS FULL;
