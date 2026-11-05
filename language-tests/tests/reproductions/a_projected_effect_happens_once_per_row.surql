/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	An expression a projection evaluates per row that does something besides
	compute its value does it once per row, in the order the statement is
	written: for each row in turn, each projected field from left to right.

	Advancing a sequence is that something, and the sequence counts it: each
	query is followed by the next value, which says how many draws the
	statement made, and the values it reports say which draw reached which
	field. Evaluating a whole field before the next would draw the same values
	in a different order, and evaluating a field again after a row of it
	signalled RETURN would draw more of them than the statement has rows. Both
	are shapes an engine may take where the expressions have nothing to repeat
	or reorder; neither may be visible in what a statement consumes.

	The three operators a projection can take are covered, because each has its
	own batch shape: a named field is computed before the projection reads it,
	a field whose output path nests is built by the projection itself, and
	SELECT VALUE projects one expression per row. A field's name is chosen to
	disagree with its position once, since the order the fields are computed in
	is the order they were written and not the order they sort in.
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
value = "[{ id: r:1 }]"

[[test.results]]
value = "[{ id: r:2 }]"

[[test.results]]
value = "[{ id: r:3 }]"

[[test.results]]
value = "[{ a: 0, b: 1 }, { a: 2, b: 3 }, { a: 4, b: 5 }]"

[[test.results]]
value = "6"

[[test.results]]
value = "[{ a: 1, b: 0 }, { a: 3, b: 2 }, { a: 5, b: 4 }]"

[[test.results]]
value = "6"

[[test.results]]
value = "[{ x: { a: 0, b: 1 } }, { x: { a: 2, b: 3 } }, { x: { a: 4, b: 5 } }]"

[[test.results]]
value = "6"

[[test.results]]
value = "[0, 1, 2]"

[[test.results]]
value = "3"

[[test.results]]
value = "[{ a: 0 }, { a: 1 }, { a: 2 }]"

[[test.results]]
value = "3"
*/

-- A batch wide enough that no draw here reserves another one; the values a
-- sequence hands out are contiguous whatever the batch is.
DEFINE SEQUENCE s BATCH 1000 START 0;
DEFINE SEQUENCE t BATCH 1000 START 0;
DEFINE SEQUENCE u BATCH 1000 START 0;
DEFINE SEQUENCE v BATCH 1000 START 0;
DEFINE SEQUENCE w BATCH 1000 START 0;
CREATE r:1;
CREATE r:2;
CREATE r:3;

-- Two fields over the same sequence: each row draws for the first field and
-- then for the second.
SELECT sequence::nextval('s') AS a, sequence::nextval('s') AS b FROM r;
RETURN sequence::nextval('s');

-- The field written first draws first, whichever way the two names sort.
SELECT sequence::nextval('t') AS b, sequence::nextval('t') AS a FROM r;
RETURN sequence::nextval('t');

-- A projection whose output paths nest, which the projection builds itself.
SELECT sequence::nextval('u') AS x.a, sequence::nextval('u') AS x.b FROM r;
RETURN sequence::nextval('u');

-- A row of SELECT VALUE that signals RETURN ends the batch, and no row draws
-- twice: three rows, three draws.
SELECT VALUE { RETURN sequence::nextval('v') } FROM r;
RETURN sequence::nextval('v');

-- The same, inside a named field.
SELECT { RETURN sequence::nextval('w') } AS a FROM r;
RETURN sequence::nextval('w');
