/**
[test]
reason = "Setup for the tests of calls of user-defined functions and reads of parameters by a record user: functions whose permission clauses are predicates, one that reads data a record user sees only in part, a table whose permission calls a function, and parameters whose permission clauses admit one reader or none."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE item SCHEMALESS PERMISSIONS FOR select WHERE hidden != true, FOR create FULL;
DEFINE FUNCTION fn::is_open() { RETURN count(SELECT * FROM item) > 0 } PERMISSIONS FULL;
DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select WHERE fn::is_open();

DEFINE FUNCTION fn::bob_only() { RETURN 'bob' } PERMISSIONS WHERE $auth.id = person:bob;
DEFINE FUNCTION fn::alice_only() { RETURN 'alice' } PERMISSIONS WHERE $auth.id = person:alice;
DEFINE FUNCTION fn::nobody() { RETURN 'nobody' } PERMISSIONS NONE;
DEFINE FUNCTION fn::counts() { LET $n = count(SELECT * FROM item); RETURN $n } PERMISSIONS FULL;
DEFINE FUNCTION fn::counted_by_bob() { LET $n = count(SELECT * FROM item); RETURN $n }
	PERMISSIONS WHERE $auth.id = person:bob;
DEFINE FUNCTION fn::double($x: int) { RETURN $x * 2 } PERMISSIONS FULL;
DEFINE FUNCTION fn::chain($x: int) { LET $d = fn::double($x); RETURN fn::counts() + $d } PERMISSIONS FULL;

DEFINE PARAM $bob_param VALUE 'for bob' PERMISSIONS WHERE $auth.id = person:bob;
DEFINE PARAM $alice_param VALUE 'for alice' PERMISSIONS WHERE $auth.id = person:alice;
DEFINE PARAM $hidden_param VALUE 'for nobody' PERMISSIONS NONE;
DEFINE PARAM $open_param VALUE 'for all' PERMISSIONS FULL;

CREATE person:bob;
CREATE person:alice;
CREATE item:1 SET n = 1;
CREATE item:2 SET n = 2, hidden = true;
CREATE item:3 SET n = 3;
CREATE doc:1;
