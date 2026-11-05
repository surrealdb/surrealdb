/**
[test]
reason = """
	A parameter holding a list of record ids, read as a whole FROM source, reads
	the records it names as the same list written in FROM does: in list order,
	a repeated id once per position, a missing record as no row, a computed
	field derived, the error of an undefined table raised before any row runs
	whatever the limit, and LIMIT, START, WHERE, GROUP and ORDER over the rows.
	A list longer than one read window reads the same way. The same parameter
	read elsewhere in the statement is its value there. A list holding a range
	is read as before. UPDATE and DELETE of such a parameter write the records
	it names, in order, each position after the writes of the positions before
	it.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 0, double: 6, id: person:3, n: 3 }, { c: 0, double: 2, id: person:1, n: 1 }, { c: 0, double: 4, id: person:2, n: 2 }, { c: 0, double: 2, id: person:1, n: 1 }]"

[[test.results]]
value = "[{ c: 0, double: 6, id: person:3, n: 3 }, { c: 0, double: 2, id: person:1, n: 1 }, { c: 0, double: 4, id: person:2, n: 2 }, { c: 0, double: 2, id: person:1, n: 1 }]"

[[test.results]]
value = "[{ double: 6, n: 3 }, { double: 2, n: 1 }, { double: 4, n: 2 }, { double: 2, n: 1 }]"

[[test.results]]
value = "[3, 2]"

[[test.results]]
value = "[3, 1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[3, 2, 1, 1]"

[[test.results]]
value = "3"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1299, 1298, 1297]"

[[test.results]]
value = "1299"

[[test.results]]
value = "1688700"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
error = "The table 'missing' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1]"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
value = "[3, 1, 2, 1]"

[[test.results]]
value = "[1, 1, 1, 2]"

[[test.results]]
value = "[[1, 2], [2, 1], [3, 1]]"

[[test.results]]
value = "[{ c: 1, double: 6, id: person:3, n: 3, touched: true }, { c: 2, double: 2, id: person:1, n: 1, touched: true }, { c: 1, double: 4, id: person:2, n: 2, touched: true }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[4]"
*/

DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD n ON person TYPE int;
DEFINE FIELD c ON person TYPE int DEFAULT 0;
DEFINE FIELD touched ON person TYPE option<bool>;
DEFINE FIELD double ON person COMPUTED n * 2;
FOR $i IN 1..1300 {
	CREATE type::record('person', $i) SET n = $i RETURN NONE;
};
LET $ids = [person:3, person:1, person:9999, person:2, person:1];
SELECT * FROM $ids;
SELECT * FROM [person:3, person:1, person:9999, person:2, person:1];
SELECT n, double FROM $ids;
SELECT VALUE n FROM $ids WHERE n > 1;
SELECT VALUE n FROM $ids LIMIT 2;
SELECT VALUE n FROM $ids LIMIT 0;
SELECT VALUE n FROM $ids START 1 LIMIT 2;
SELECT count() FROM $ids GROUP ALL;
SELECT VALUE n FROM $ids WHERE id IN $ids ORDER BY n DESC;
SELECT VALUE n FROM ONLY $ids LIMIT 1;
LET $big = (SELECT VALUE id FROM person ORDER BY n DESC);
SELECT VALUE n FROM $big LIMIT 3;
RETURN array::len(SELECT VALUE n FROM $big);
RETURN math::sum(SELECT VALUE double FROM $big);
LET $bad = [person:1, missing:1, person:2];
SELECT * FROM $bad;
SELECT * FROM $bad LIMIT 1;
SELECT * FROM $bad LIMIT 0;
LET $ranged = [person:1, person:2..4];
SELECT VALUE n FROM $ranged;
SELECT * FROM $ids FOR UPDATE;
UPDATE $ids SET touched = true RETURN VALUE n;
UPDATE $ids SET c += 1 RETURN VALUE c;
SELECT VALUE [n, c] FROM person:1..4;
DELETE $ids RETURN BEFORE;
SELECT * FROM $ids;
SELECT VALUE n FROM person:1..5;
