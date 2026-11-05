/**
[test]
reason = "Setup for the test that imports it: a record access, the database parameter and function it reads, and the table or clause under test."
run = false
*/
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid ) DURATION FOR TOKEN 1h, FOR SESSION 1h;
DEFINE PARAM $pm VALUE 8;
DEFINE FUNCTION fn::ok($x: number, $t: number) { RETURN array::any([$x], |$e| $e >= $t); };
DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
CREATE person:bob SET lim = 8;
DEFINE TABLE pt_upd SCHEMALESS PERMISSIONS FOR update WHERE array::any([$before.n], |$e| $e >= $pm) FOR select FULL;
CREATE pt_upd:1 SET n = 1;
CREATE pt_upd:2 SET n = 10;
