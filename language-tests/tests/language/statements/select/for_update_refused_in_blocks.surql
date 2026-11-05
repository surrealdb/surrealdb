/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A SELECT … FOR UPDATE whose clauses or targets the planner rejects is refused with the same error wherever it stands — in a block, a loop body, an IF branch, a LET value and a nested block — and the statement holding it writes nothing"

[[test.results]]
value = "[{ id: person:1 }, { id: person:2 }]"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE cannot be combined with GROUP BY or SPLIT"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE cannot be combined with LIMIT over multiple targets"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE cannot be combined with GROUP BY or SPLIT"

[[test.results]]
error = "The table 'marker' does not exist"

*/

CREATE person:1, person:2;

{
	CREATE marker:one;
	SELECT * FROM person FOR UPDATE;
};

FOR $i IN [1] {
	CREATE marker:two;
	SELECT * FROM [$i] FOR UPDATE;
};

IF true {
	CREATE marker:three;
	SELECT count() FROM person:1 GROUP ALL FOR UPDATE;
} ELSE {
	NONE
};

{
	LET $rows = SELECT * FROM person:1, person:2 LIMIT 1 FOR UPDATE;
	RETURN $rows;
};

{
	CREATE marker:four;
	{ SELECT * FROM person:1..3 FOR UPDATE; };
};

{
	SELECT * FROM person:1 SPLIT tags FOR UPDATE;
};

SELECT * FROM marker;
