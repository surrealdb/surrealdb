/**
[test]
reason = """
	Setup for a_call_in_an_element_body_is_judged_on_the_element: a row whose
	field and whose elements' field sit on either side of the bound two
	functions' `PERMISSIONS` clauses test, one admitting the elements and
	refusing the row, one the other way round, each once with a body that is
	inlined and once with a body that runs as a call; and the record user the
	test runs as. Runs as owner.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:probe;

DEFINE TABLE s SCHEMALESS PERMISSIONS FULL;
CREATE s:1 SET x = 9, items = [{ x: 1 }, { x: 2 }];

DEFINE FUNCTION fn::small() { RETURN x; } PERMISSIONS WHERE x < 5;
DEFINE FUNCTION fn::small_call() { LET $v = x; RETURN $v; } PERMISSIONS WHERE x < 5;
DEFINE FUNCTION fn::big() { RETURN x; } PERMISSIONS WHERE x > 5;
DEFINE FUNCTION fn::big_call() { LET $v = x; RETURN $v; } PERMISSIONS WHERE x > 5;
