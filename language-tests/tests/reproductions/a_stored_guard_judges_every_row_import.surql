/**
[test]
reason = """
	Setup for a_stored_guard_judges_every_row: a parameter and a
	function whose `PERMISSIONS` clauses read the row they are judged on —
	one of each admitting every row of `t`, one of each admitting only some —
	and the record user the test runs as. Runs as owner.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:probe;

DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
CREATE t:1 SET n = 1;
CREATE t:2 SET n = 2;
CREATE t:3 SET n = 3;

DEFINE PARAM $always VALUE 'yes' PERMISSIONS WHERE n < 100;
DEFINE PARAM $some VALUE 'yes' PERMISSIONS WHERE n < 3;

DEFINE FUNCTION fn::always() { RETURN 'yes'; } PERMISSIONS WHERE n < 100;
DEFINE FUNCTION fn::some() { RETURN 'yes'; } PERMISSIONS WHERE n < 3;
