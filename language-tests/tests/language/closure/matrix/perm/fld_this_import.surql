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
DEFINE TABLE pt_fld2 SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD f ON pt_fld2 PERMISSIONS FOR select WHERE array::any([1], |$e| $e <= $this.lim);
CREATE pt_fld2:1 SET f = 1, lim = 0;
CREATE pt_fld2:2 SET f = 10, lim = 8;
