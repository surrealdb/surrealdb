/**
[test]
reason = "Setup for capture_values_record_user.surql: a record access, a user, a parameter its users may not view and a table whose field clause makes and invokes a closure."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE w SCHEMALESS PERMISSIONS FULL;
DEFINE PARAM $r VALUE 1 PERMISSIONS NONE;
DEFINE FIELD f ON w VALUE { LET $g = |$e| { LET $r = 2; $r }; $g(1) };

CREATE person:bob;
