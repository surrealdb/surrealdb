/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ fully_specified: [0, 0], id: test:l8ofv4c0x0g2phjaks5m, partial: [0,0], partial_with_any: [0,0], partial_with_any_and_size: [0,0] }]"
skip-record-id-key = true

[[test.results]]
value = "[{ fully_specified: [1, 1], id: test:l8ofv4c0x0g2phjaks5m, partial: [2,2], partial_with_any: [3,3], partial_with_any_and_size: [4,4] }]"
skip-record-id-key = true

*/

BEGIN;
	DEFINE TABLE test SCHEMAFULL;
	DEFINE FIELD fully_specified ON test TYPE array<number,2> DEFAULT [0,0];
	DEFINE FIELD partial ON test TYPE array DEFAULT [0,0];
	DEFINE FIELD partial_with_any ON test TYPE array<any> DEFAULT [0,0];
	DEFINE FIELD partial_with_any_and_size ON test TYPE array<any,2> DEFAULT [0,0];

	RETURN "OK";
COMMIT;

CREATE test;
UPDATE test SET fully_specified=[1,1],partial=[2,2],partial_with_any=[3,3],partial_with_any_and_size=[4,4];

