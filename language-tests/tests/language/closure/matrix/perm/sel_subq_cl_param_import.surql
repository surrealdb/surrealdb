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
DEFINE TABLE pt_perm_cl_param_k SCHEMALESS PERMISSIONS FULL;
CREATE pt_perm_cl_param_k:1 SET k = 100;
DEFINE TABLE pt_subq_cl_param SCHEMALESS PERMISSIONS FOR select WHERE array::any((SELECT VALUE k FROM pt_perm_cl_param_k WHERE k >= $pm), |$e| $e <= n);
CREATE pt_subq_cl_param:1 SET n = 1, lim = 8;
CREATE pt_subq_cl_param:2 SET n = 10, lim = 8;
