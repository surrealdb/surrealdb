/**
[test]
reason = "Setup for the tests that a function body reads its arguments, the row names and the ordinary chain (the bind parameters, the session values and the database's parameters) and never a LET or FOR binding of its caller, from a table permission and from an ordinary call alike: a table whose permission asks a function that reads a parameter, and functions that read a parameter the caller may also bind."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE PARAM $allowed VALUE false PERMISSIONS FULL;
DEFINE PARAM $limit VALUE 10 PERMISSIONS FULL;

DEFINE FUNCTION fn::allowed() { RETURN $allowed } PERMISSIONS FULL;
DEFINE FUNCTION fn::lim() { RETURN $limit } PERMISSIONS FULL;
DEFINE FUNCTION fn::lim_block() { LET $one = 1; RETURN $limit * $one } PERMISSIONS FULL;
DEFINE FUNCTION fn::sees($v: int) { RETURN [$v, $limit, $unbound] } PERMISSIONS FULL;
DEFINE FUNCTION fn::shadow($limit: int) { RETURN $limit } PERMISSIONS FULL;
DEFINE FUNCTION fn::who() { RETURN [$auth, $access] } PERMISSIONS FULL;
DEFINE FUNCTION fn::row() { RETURN $this.v } PERMISSIONS FULL;
DEFINE FUNCTION fn::nested() { RETURN fn::lim() } PERMISSIONS FULL;

DEFINE TABLE user SCHEMALESS PERMISSIONS NONE;
DEFINE TABLE viafn SCHEMALESS PERMISSIONS FOR select, create, update WHERE fn::allowed();
DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;

CREATE user:a, user:b;
CREATE viafn:1 SET v = 1;
