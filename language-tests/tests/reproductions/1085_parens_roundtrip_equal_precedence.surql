/**
[test]
reason = "A parenthesised right operand of a left-associative binary operator must survive the catalog text round-trip used by DEFINE FUNCTION and DEFINE FIELD"
issue = 1085

[[test.results]]
value = "1"

[[test.results]]
value = "25"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "3"

[[test.results]]
value = "NONE"

[[test.results]]
value = "50"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: q:1, v: 10 }]"

*/

-- Parsing itself is correct: the parenthesised divisor binds as one operand.
RETURN 10 / (2 * 5);
RETURN 10 / 2 * 5;

-- Definitions are persisted as canonical SurrealQL text and reparsed on load,
-- so the rendered body must re-parse to the same tree it was defined with.
DEFINE FUNCTION fn::div_by_product($x: number, $a: number, $b: number) {
	RETURN $x / ($a * $b);
};
RETURN fn::div_by_product(10, 2, 5);

DEFINE FUNCTION fn::sub_of_sum($a: number, $b: number, $c: number) {
	RETURN $a - ($b + $c);
};
RETURN fn::sub_of_sum(10, 2, 5);

-- Chosen so that misparsing as `($a / $b) / $c` (10) is distinguishable from
-- the correct `$a / ($b / $c)` (50) even under SurrealQL's integer division.
DEFINE FUNCTION fn::div_by_quotient($a: number, $b: number, $c: number) {
	RETURN $a / ($b / $c);
};
RETURN fn::div_by_quotient(100, 5, 2);

-- Field computations take the same text round-trip.
DEFINE TABLE q SCHEMALESS;
DEFINE FIELD v ON q VALUE 100 / (2 * 5) TYPE number;
CREATE q:1;
