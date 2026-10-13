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
DEFINE TABLE pt_cl_auth_id SCHEMALESS PERMISSIONS FOR select WHERE array::any([$auth.id], |$e| $e = person:bob);
CREATE pt_cl_auth_id:1 SET n = 1, lim = 8;
CREATE pt_cl_auth_id:2 SET n = 10, lim = 8;
