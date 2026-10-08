/**
[test]
reason = "Setup for eval/surql_record_user.surql: a record access, a table whose SELECT permission calls eval::surql, and a function whose body calls it."
run = false
*/
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:bob;
DEFINE TABLE item SCHEMALESS PERMISSIONS FOR select WHERE eval::surql("RETURN $n > 1", { n: n }) FOR create, update, delete NONE;
CREATE item:1 SET n = 1;
CREATE item:2 SET n = 2;
CREATE item:3 SET n = 3;
DEFINE FUNCTION fn::double($x: int) { RETURN eval::surql("RETURN $v * 2", { v: $x }) } PERMISSIONS FULL;
