/**
[test]
reason = "Setup for eval/surql_record_user.surql: a record access, a table whose SELECT permission reaches eval::surql through a stored function, and a function whose body calls it. A clause calling eval::surql is refused where it is defined, since what it runs cannot be known there, so the permission calls `fn::gate` while its body only reads and the body is redefined to call eval::surql afterwards."
run = false
*/
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:bob;
DEFINE FUNCTION fn::gate($n: int) { RETURN $n > 1 } PERMISSIONS FULL;
DEFINE TABLE item SCHEMALESS PERMISSIONS FOR select WHERE fn::gate(n) FOR create, update, delete NONE;
DEFINE FUNCTION OVERWRITE fn::gate($n: int) { RETURN eval::surql("RETURN $n > 1", { n: $n }) } PERMISSIONS FULL;
CREATE item:1 SET n = 1;
CREATE item:2 SET n = 2;
CREATE item:3 SET n = 3;
DEFINE FUNCTION fn::double($x: int) { RETURN eval::surql("RETURN $v * 2", { v: $x }) } PERMISSIONS FULL;
