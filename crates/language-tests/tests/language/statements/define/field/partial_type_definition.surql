/**
[test]
wip = true
reason = """
Setting fields which have partially defined types didn't seem to work when the test was written

This seems to be a problem in the cleanup_table_fields function being over-eager with deleting fields.
"""

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ fully_specified: [0, 0], id: test:u1628npei8z4webnmwng, partial: [0,0], partial_with_any: [0,0], partial_with_any_and_size: [0,0] }]"

[[test.results]]
value = "[{ fully_specified: [1, 1], id: test:u1628npei8z4webnmwng, partial: [2,2], partial_with_any: [3,3], partial_with_any_and_size: [4,4] }]"

*/

BEGIN;
	DEFINE TABLE test schemafull;
	DEFINE FIELD fully_specified ON test TYPE array<number,2> DEFAULT [0,0];
	DEFINE FIELD partial ON test TYPE array DEFAULT [0,0];
	DEFINE FIELD partial_with_any ON test TYPE array<any> DEFAULT [0,0];
	DEFINE FIELD partial_with_any_and_size ON test TYPE array<any,2> DEFAULT [0,0];

	RETURN "OK";
COMMIT;

CREATE test;
UPDATE test SET fully_specified=[1,1],partial=[2,2],partial_with_any=[3,3],partial_with_any_and_size=[4,4];

